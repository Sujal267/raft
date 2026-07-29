package raft

import (
	"errors"
	"net/rpc"
	"sync"
	"time"
)

// MaxAppendEntriesBatch caps how many entries go out in a single
// AppendEntries RPC, so one very large Apply() call doesn't stall
// replication latency for everyone else.
const MaxAppendEntriesBatch = 8_000

var ErrApplyToLeader = errors.New("cannot apply message on a non-leader, retry against the leader")

// Apply appends commands to the leader's log and blocks until each one
// has been replicated to a quorum and applied to the local state
// machine. It returns ErrApplyToLeader if called on a non-leader.
func (s *Server) Apply(commands [][]byte) ([]ApplyResult, error) {
	s.mu.Lock()
	if s.state != leader {
		s.mu.Unlock()
		return nil, ErrApplyToLeader
	}

	s.debugf("applying %d new command(s)", len(commands))

	resultChans := make([]chan ApplyResult, len(commands))
	for i, cmd := range commands {
		resultChans[i] = make(chan ApplyResult, 1)
		s.log = append(s.log, Entry{
			Term:    s.currentTerm,
			Command: cmd,
			result:  resultChans[i],
		})
	}
	s.persist(true, len(commands))
	s.mu.Unlock()

	s.appendEntries()

	results := make([]ApplyResult, len(commands))
	var wg sync.WaitGroup
	wg.Add(len(commands))
	for i, ch := range resultChans {
		go func(i int, ch chan ApplyResult) {
			defer wg.Done()
			results[i] = <-ch
		}(i, ch)
	}
	wg.Wait()

	return results, nil
}

// rpcCall invokes a Server RPC method on peer i, lazily dialing a
// connection on first use. Returns false (and logs a warning) on any
// transport error; the caller is expected to retry on the next tick.
func (s *Server) rpcCall(i int, method string, req, rsp any) bool {
	s.mu.Lock()
	member := s.cluster[i]
	client := member.rpcClient
	var dialErr error
	if client == nil {
		client, dialErr = rpc.DialHTTP("tcp", member.Address)
		s.cluster[i].rpcClient = client
	}
	s.mu.Unlock()

	err := dialErr
	if err == nil {
		err = client.Call(method, req, rsp)
	}
	if err != nil {
		// Drop the cached client so the next call redials instead of
		// reusing a connection that may be permanently broken (e.g.
		// the peer crashed and came back up on the same address).
		s.mu.Lock()
		if s.cluster[i].rpcClient == client {
			s.cluster[i].rpcClient = nil
		}
		s.mu.Unlock()

		s.warnf("rpc %s to %d failed: %s", method, member.Id, err)
		return false
	}
	return true
}

// appendEntries sends an AppendEntries RPC (heartbeat or with new
// entries) to every peer in parallel.
func (s *Server) appendEntries() {
	for i := range s.cluster {
		if i == s.clusterIndex {
			continue
		}

		go func(i int) {
			s.mu.Lock()

			next := s.cluster[i].nextIndex
			prevLogIndex := next - 1
			prevLogTerm := s.log[prevLogIndex].Term

			var entries []Entry
			if uint64(len(s.log)-1) >= next {
				entries = s.log[next:]
			}
			if len(entries) > MaxAppendEntriesBatch {
				entries = entries[:MaxAppendEntriesBatch]
			}
			nEntries := uint64(len(entries))

			req := AppendEntriesRequest{
				RPCMessage:   RPCMessage{Term: s.currentTerm},
				LeaderId:     s.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: s.commitIndex,
			}
			s.mu.Unlock()

			var rsp AppendEntriesResponse
			if !s.rpcCall(i, "Server.HandleAppendEntriesRequest", req, &rsp) {
				return
			}

			s.mu.Lock()
			defer s.mu.Unlock()

			if s.updateTerm(rsp.RPCMessage) {
				return
			}

			if rsp.Term != req.Term && s.state == leader {
				// Stale response; ignore.
				return
			}

			if rsp.Success {
				s.cluster[i].nextIndex = max(req.PrevLogIndex+nEntries+1, 1)
				s.cluster[i].matchIndex = s.cluster[i].nextIndex - 1
			} else {
				// Log mismatch: back off by one and retry on
				// the next heartbeat.
				s.cluster[i].nextIndex = max(s.cluster[i].nextIndex-1, 1)
			}
		}(i)
	}
}

// HandleAppendEntriesRequest implements the AppendEntries RPC (Raft
// paper Figure 2, receiver implementation).
func (s *Server) HandleAppendEntriesRequest(req AppendEntriesRequest, rsp *AppendEntriesResponse) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.updateTerm(req.RPCMessage)

	// A candidate that hears from a legitimate leader in the same
	// term steps down without waiting for a higher term (§5.2).
	if req.Term == s.currentTerm && s.state == candidate {
		s.state = follower
	}

	rsp.Term = s.currentTerm
	rsp.Success = false

	if s.state != follower {
		return nil
	}
	if req.Term < s.currentTerm {
		// Message from a stale leader.
		return nil
	}

	s.resetElectionTimeout()
	s.leaderId = req.LeaderId

	logLen := uint64(len(s.log))
	validPrevLog := req.PrevLogIndex == 0 ||
		(req.PrevLogIndex < logLen && s.log[req.PrevLogIndex].Term == req.PrevLogTerm)
	if !validPrevLog {
		return nil
	}

	next := req.PrevLogIndex + 1
	nNewEntries := 0

	for i := next; i < next+uint64(len(req.Entries)); i++ {
		e := req.Entries[i-next]

		if i < uint64(len(s.log)) && s.log[i].Term != e.Term {
			// Conflicting entry at the same index: drop it and
			// everything after (§5.3).
			s.log = s.log[:i]
		}

		if i < uint64(len(s.log)) {
			serverAssert(s, "existing entry matches incoming entry", s.log[i].Term, e.Term)
		} else {
			s.log = append(s.log, e)
			nNewEntries++
		}
	}

	if req.LeaderCommit > s.commitIndex {
		s.commitIndex = min(req.LeaderCommit, uint64(len(s.log)-1))
	}

	s.persist(nNewEntries != 0, nNewEntries)
	rsp.Success = true
	return nil
}

// advanceCommitIndex lets a leader pull commitIndex forward once a
// quorum of peers have replicated a given index, then applies any
// newly-committed entries to the local state machine.
func (s *Server) advanceCommitIndex() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.state == leader {
		lastLogIndex := uint64(len(s.log) - 1)

		for i := lastLogIndex; i > s.commitIndex; i-- {
			quorum := len(s.cluster)/2 + 1
			for j := range s.cluster {
				if quorum == 0 {
					break
				}
				if j == s.clusterIndex || s.cluster[j].matchIndex >= i {
					quorum--
				}
			}

			if quorum == 0 {
				s.commitIndex = i
				s.debugf("advanced commit index to %d", i)
				break
			}
		}
	}

	if s.lastApplied <= s.commitIndex {
		e := s.log[s.lastApplied]

		// Empty Command marks a leader's no-op or the log's index-0
		// sentinel; nothing to hand to the state machine.
		if len(e.Command) > 0 {
			res, err := s.statemachine.Apply(e.Command)
			if e.result != nil {
				e.result <- ApplyResult{Result: res, Error: err}
			}
		}

		s.lastApplied++
	}
}

// heartbeat sends an empty (or not) AppendEntries to all peers on a
// fixed interval to keep them from starting an election.
func (s *Server) heartbeat() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if time.Now().Before(s.heartbeatTimeout) {
		return
	}
	s.heartbeatTimeout = time.Now().Add(time.Duration(s.heartbeatMs) * time.Millisecond)
	s.appendEntries()
}
