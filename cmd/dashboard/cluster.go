package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"raft-go/kv"
	"raft-go/raft"
)

// basePort is the first RPC port handed to node id 1; node id N listens
// on basePort+N. This is purely internal plumbing between simulated
// nodes on localhost, never exposed to the browser.
const basePort = 21000

// node wraps one simulated cluster member. Only manager touches these
// fields, always under manager.mu, so a node can be torn down and
// rebuilt (simulating a crash + restart) without racing the HTTP
// handlers that read its state.
type node struct {
	id      uint64
	address string

	server *raft.Server
	sm     *kv.StateMachine
	alive  bool
}

// manager owns every node in the simulated cluster and is the only
// thing allowed to start, kill, or restart one. All of Raft's actual
// peer-to-peer traffic still goes over real TCP connections between
// these in-process servers; manager just plays the role an init system
// or orchestrator would in a real deployment.
type manager struct {
	mu      sync.Mutex
	nodes   []*node
	cluster []raft.ClusterMember
	dataDir string
	debug   bool
}

func newManager(count int, dataDir string, debug bool) *manager {
	m := &manager{dataDir: dataDir, debug: debug}

	for i := 1; i <= count; i++ {
		id := uint64(i)
		m.cluster = append(m.cluster, raft.ClusterMember{
			Id:      id,
			Address: fmt.Sprintf("127.0.0.1:%d", basePort+i),
		})
	}

	for _, member := range m.cluster {
		m.nodes = append(m.nodes, &node{id: member.Id, address: member.Address})
	}

	return m
}

// StartAll (re)creates a clean metadata directory and boots every node
// fresh. Called once at process startup.
func (m *manager) StartAll() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := os.RemoveAll(m.dataDir); err != nil {
		return err
	}
	if err := os.MkdirAll(m.dataDir, 0o755); err != nil {
		return err
	}

	for i := range m.nodes {
		if err := m.startLocked(i); err != nil {
			return err
		}
	}
	return nil
}

// startLocked boots node index i with a fresh raft.Server and a fresh
// (empty) state machine. Raft replays every committed log entry from
// disk into the new state machine on the way up, exactly as it would
// for cmd/kvapi restarting as a separate OS process — the metadata
// directory, not the in-memory struct, is what survives a "crash".
func (m *manager) startLocked(i int) error {
	n := m.nodes[i]
	metaDir := filepath.Join(m.dataDir, fmt.Sprintf("node%d", n.id))
	if err := os.MkdirAll(metaDir, 0o755); err != nil {
		return err
	}

	sm := kv.NewStateMachine()
	rs := raft.NewServer(m.cluster, sm, metaDir, i)
	rs.Debug = m.debug
	rs.Start()

	n.server = rs
	n.sm = sm
	n.alive = true
	return nil
}

// Reset shuts down every node, wipes the metadata directory, and boots
// the whole cluster fresh — as if the process had just started.
func (m *manager) Reset() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, n := range m.nodes {
		if n.alive {
			n.server.Shutdown()
			n.alive = false
		}
	}

	if err := os.RemoveAll(m.dataDir); err != nil {
		return err
	}
	if err := os.MkdirAll(m.dataDir, 0o755); err != nil {
		return err
	}

	for i := range m.nodes {
		if err := m.startLocked(i); err != nil {
			return err
		}
	}
	return nil
}

var errUnknownNode = fmt.Errorf("unknown node id")
var errAlreadyAlive = fmt.Errorf("node is already running")
var errAlreadyDead = fmt.Errorf("node is already down")

func (m *manager) indexOf(id uint64) int {
	for i, n := range m.nodes {
		if n.id == id {
			return i
		}
	}
	return -1
}

// Kill simulates a process crash: the RPC listener closes and the
// driver loop stops, so peers start seeing this node as unreachable
// exactly like a killed process. Its metadata directory is left intact
// on disk.
func (m *manager) Kill(id uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	i := m.indexOf(id)
	if i < 0 {
		return errUnknownNode
	}
	n := m.nodes[i]
	if !n.alive {
		return errAlreadyDead
	}

	n.server.Shutdown()
	n.alive = false
	return nil
}

// Restart simulates the node process being brought back up: a brand
// new Server/StateMachine pair boots against the same metadata
// directory and replays whatever was persisted before the crash.
func (m *manager) Restart(id uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	i := m.indexOf(id)
	if i < 0 {
		return errUnknownNode
	}
	if m.nodes[i].alive {
		return errAlreadyAlive
	}

	return m.startLocked(i)
}

// nodeStatus mirrors raft.Status but adds the `alive` flag a dead node
// can't report about itself.
type nodeStatus struct {
	raft.Status
	Alive bool `json:"alive"`
}

// Statuses snapshots every node. Dead nodes report only id/address so
// the UI can still render them (greyed out) at their fixed position.
func (m *manager) Statuses() []nodeStatus {
	m.mu.Lock()
	defer m.mu.Unlock()

	out := make([]nodeStatus, len(m.nodes))
	for i, n := range m.nodes {
		if n.alive {
			out[i] = nodeStatus{Status: n.server.Status(), Alive: true}
		} else {
			out[i] = nodeStatus{Status: raft.Status{Id: n.id, Address: n.address}, Alive: false}
		}
	}
	return out
}

var errNoLeader = fmt.Errorf("no leader currently elected; try again shortly")

// leaderLocked returns the node currently believed to be leader, if
// any. Must be called with m.mu held.
func (m *manager) leaderLocked() *node {
	for _, n := range m.nodes {
		if n.alive && n.server.IsLeader() {
			return n
		}
	}
	return nil
}

// applyTimeout bounds how long an HTTP request waits for consensus
// before giving up on the client. raft.Server.Apply itself has no
// cancellation, so on timeout the underlying call is left running in
// the background (it'll complete or block forever if quorum is lost)
// while the HTTP handler returns early — an acceptable tradeoff for a
// demo tool, not something you'd want in production.
const applyTimeout = 2 * time.Second

// Set applies a write through consensus on whichever node is currently
// leader.
func (m *manager) Set(key, value string) error {
	m.mu.Lock()
	leader := m.leaderLocked()
	m.mu.Unlock()

	if leader == nil {
		return errNoLeader
	}

	done := make(chan error, 1)
	go func() {
		_, err := leader.server.Apply([][]byte{kv.Encode(kv.Command{Kind: kv.Set, Key: key, Value: value})})
		done <- err
	}()

	select {
	case err := <-done:
		return err
	case <-time.After(applyTimeout):
		return fmt.Errorf("timed out waiting for commit (lost quorum?)")
	}
}

// Get reads a key. If relaxed is true it reads the leader's local copy
// directly (fast, technically not linearizable); otherwise it goes
// through consensus like a Set.
func (m *manager) Get(key string, relaxed bool) (string, error) {
	m.mu.Lock()
	leader := m.leaderLocked()
	m.mu.Unlock()

	if leader == nil {
		return "", errNoLeader
	}

	if relaxed {
		v, ok := leader.sm.Peek(key)
		if !ok {
			return "", fmt.Errorf("key not found: %s", key)
		}
		return v, nil
	}

	type result struct {
		value string
		err   error
	}
	done := make(chan result, 1)
	go func() {
		results, err := leader.server.Apply([][]byte{kv.Encode(kv.Command{Kind: kv.Get, Key: key})})
		if err != nil {
			done <- result{err: err}
			return
		}
		if results[0].Error != nil {
			done <- result{err: results[0].Error}
			return
		}
		done <- result{value: string(results[0].Result)}
	}()

	select {
	case r := <-done:
		return r.value, r.err
	case <-time.After(applyTimeout):
		return "", fmt.Errorf("timed out waiting for commit (lost quorum?)")
	}
}
