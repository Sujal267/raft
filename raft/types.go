package raft

import (
	"fmt"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// Assert panics if a != b. Used liberally throughout the implementation
// to catch protocol invariant violations as early as possible instead of
// silently corrupting state.
func Assert[T comparable](msg string, a, b T) {
	if a != b {
		panic(fmt.Sprintf("%s. Got a = %#v, b = %#v", msg, a, b))
	}
}

func min[T ~int | ~uint64](a, b T) T {
	if a < b {
		return a
	}
	return b
}

func max[T ~int | ~uint64](a, b T) T {
	if a > b {
		return a
	}
	return b
}

// StateMachine is whatever application the caller wants replicated. Raft
// itself does not know or care what `cmd` contains: it just guarantees
// that every node in the cluster calls Apply() with the same commands in
// the same order.
type StateMachine interface {
	Apply(cmd []byte) ([]byte, error)
}

// ApplyResult is handed back to the caller of Server.Apply() once a
// command has made it through consensus and been applied locally.
type ApplyResult struct {
	Result []byte
	Error  error
}

// Entry is a single slot in the replicated log.
type Entry struct {
	Term    uint64
	Command []byte

	// result is only set on the leader that originally accepted the
	// command from a client. It's how Apply() blocks until the entry
	// has actually been committed and applied.
	result chan ApplyResult
}

// RPCMessage is embedded in every RPC request/response so that any
// message can be used to discover a higher term and step down.
type RPCMessage struct {
	Term uint64
}

type RequestVoteRequest struct {
	RPCMessage

	CandidateId  uint64
	LastLogIndex uint64
	LastLogTerm  uint64
}

type RequestVoteResponse struct {
	RPCMessage

	VoteGranted bool
}

type AppendEntriesRequest struct {
	RPCMessage

	LeaderId uint64

	// Used by the follower to find where in its own log the new
	// entries should be spliced in, and to detect a log mismatch.
	PrevLogIndex uint64
	PrevLogTerm  uint64

	// Empty for a pure heartbeat.
	Entries []Entry

	LeaderCommit uint64
}

type AppendEntriesResponse struct {
	RPCMessage

	Success bool
}

// ClusterMember describes one node's static identity plus the leader-only
// bookkeeping (nextIndex/matchIndex) and per-node persistent vote record.
type ClusterMember struct {
	Id      uint64
	Address string

	// Next log index to send this peer. Leader-only, reset on election.
	nextIndex uint64
	// Highest log index known to be replicated to this peer. Leader-only.
	matchIndex uint64

	// votedFor is stored per cluster-slot because it's really "who did
	// *this* node vote for", and we keep every node's own slot up to
	// date via updateTerm/setVotedFor so persistence stays simple.
	votedFor uint64

	rpcClient *rpc.Client
}

type serverState string

const (
	leader    serverState = "leader"
	follower  serverState = "follower"
	candidate serverState = "candidate"
)

// Server is a single Raft node: the state machine of the consensus
// protocol itself, not to be confused with the user's StateMachine.
type Server struct {
	done       bool
	httpServer *http.Server

	Debug bool

	mu sync.Mutex

	// ---------------- Persistent state ----------------
	// (survives restarts; must be fsynced before responding to RPCs)

	currentTerm uint64
	log         []Entry
	// votedFor lives in cluster[clusterIndex].votedFor

	// ---------------- Volatile state -------------------

	commitIndex  uint64
	lastApplied  uint64
	state        serverState
	cluster      []ClusterMember
	clusterIndex int

	// ---------------- Read-only / config ----------------

	id      uint64
	address string

	electionTimeout  time.Time
	heartbeatMs      int
	heartbeatTimeout time.Time

	statemachine StateMachine

	metadataDir string
	fd          *os.File
}

// NewServer builds a Server for cluster[clusterIndex]. `cluster` should
// list every node in the cluster, this one included.
func NewServer(cluster []ClusterMember, statemachine StateMachine, metadataDir string, clusterIndex int) *Server {
	// Copy the cluster slice: we mutate per-member fields (nextIndex,
	// votedFor, rpcClient) and don't want to alias the caller's slice.
	members := make([]ClusterMember, 0, len(cluster))
	for _, m := range cluster {
		if m.Id == 0 {
			panic("ClusterMember.Id must not be 0")
		}
		members = append(members, m)
	}

	return &Server{
		id:           members[clusterIndex].Id,
		address:      members[clusterIndex].Address,
		cluster:      members,
		statemachine: statemachine,
		metadataDir:  metadataDir,
		clusterIndex: clusterIndex,
		heartbeatMs:  300,
	}
}

func (s *Server) debugmsg(msg string) string {
	return fmt.Sprintf("%s [id=%d term=%d] %s", time.Now().Format(time.RFC3339Nano), s.id, s.currentTerm, msg)
}

func (s *Server) debug(msg string) {
	if s.Debug {
		fmt.Println(s.debugmsg(msg))
	}
}

func (s *Server) debugf(format string, args ...any) {
	if s.Debug {
		s.debug(fmt.Sprintf(format, args...))
	}
}

func (s *Server) warnf(format string, args ...any) {
	fmt.Println("[WARN] " + s.debugmsg(fmt.Sprintf(format, args...)))
}

func serverAssert[T comparable](s *Server, msg string, a, b T) {
	Assert(s.debugmsg(msg), a, b)
}
