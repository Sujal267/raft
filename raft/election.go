package raft

import "time"

// updateTerm steps down to follower whenever it observes a strictly
// higher term, per Raft's "Rules for Servers": if RPC request or response
// contains a higher term, update currentTerm and convert to follower.
// Must be called with s.mu held. Returns whether it transitioned.
func (s *Server) updateTerm(msg RPCMessage) bool {
	if msg.Term <= s.currentTerm {
		return false
	}

	s.currentTerm = msg.Term
	s.state = follower
	s.setVotedFor(0)
	s.resetElectionTimeout()
	s.persist(false, 0)
	s.debug("stepping down to follower: saw higher term")
	return true
}

// requestVote fires off RequestVote RPCs to every peer in parallel. It
// does not wait for the outcome; becomeLeader() polls the accumulated
// votes on the next tick.
func (s *Server) requestVote() {
	for i := range s.cluster {
		if i == s.clusterIndex {
			continue
		}

		go func(i int) {
			s.mu.Lock()
			s.debugf("requesting vote from %d", s.cluster[i].Id)

			lastLogIndex := uint64(len(s.log) - 1)
			lastLogTerm := s.log[lastLogIndex].Term
			req := RequestVoteRequest{
				RPCMessage:   RPCMessage{Term: s.currentTerm},
				CandidateId:  s.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			s.mu.Unlock()

			var rsp RequestVoteResponse
			if !s.rpcCall(i, "Server.HandleRequestVoteRequest", req, &rsp) {
				// Peer unreachable; we'll retry on the next
				// election timeout if we're still a candidate.
				return
			}

			s.mu.Lock()
			defer s.mu.Unlock()

			if s.updateTerm(rsp.RPCMessage) {
				return
			}

			if rsp.Term != req.Term {
				// Stale response from a previous term's request.
				return
			}

			if rsp.VoteGranted {
				s.debugf("vote granted by %d", s.cluster[i].Id)
				s.cluster[i].votedFor = s.id
			}
		}(i)
	}
}

// HandleRequestVoteRequest implements the RequestVote RPC (Raft paper
// Figure 2, receiver implementation).
func (s *Server) HandleRequestVoteRequest(req RequestVoteRequest, rsp *RequestVoteResponse) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.updateTerm(req.RPCMessage)
	s.debugf("received vote request from %d", req.CandidateId)

	rsp.Term = s.currentTerm
	rsp.VoteGranted = false

	if req.Term < s.currentTerm {
		return nil
	}

	lastLogTerm := s.log[len(s.log)-1].Term
	lastLogIndex := uint64(len(s.log) - 1)

	// §5.4.1: grant the vote only if the candidate's log is at least
	// as up to date as ours (higher last term, or same term and at
	// least as long).
	logOk := req.LastLogTerm > lastLogTerm ||
		(req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastLogIndex)

	grant := req.Term == s.currentTerm &&
		logOk &&
		(s.getVotedFor() == 0 || s.getVotedFor() == req.CandidateId)

	if grant {
		s.debugf("voted for %d", req.CandidateId)
		s.setVotedFor(req.CandidateId)
		rsp.VoteGranted = true
		s.resetElectionTimeout()
		s.persist(false, 0)
	} else {
		s.debugf("not granting vote request from %d", req.CandidateId)
	}

	return nil
}

// resetElectionTimeout picks a new randomized deadline for starting the
// next election. Must be called with s.mu held.
func (s *Server) resetElectionTimeout() {
	interval := randomDuration(2*s.heartbeatMs, 4*s.heartbeatMs)
	s.electionTimeout = time.Now().Add(interval)
	s.debugf("next election timeout in %s", interval)
}

// timeout starts a new election if we haven't heard from a leader (or
// cast a vote) recently enough.
func (s *Server) timeout() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if time.Now().Before(s.electionTimeout) {
		return
	}

	s.debug("election timeout: starting new election")
	s.state = candidate
	s.currentTerm++
	for i := range s.cluster {
		if i == s.clusterIndex {
			s.cluster[i].votedFor = s.id
		} else {
			s.cluster[i].votedFor = 0
		}
	}

	s.resetElectionTimeout()
	s.persist(false, 0)
	s.requestVote()
}

// becomeLeader checks whether we've collected votes from a quorum and,
// if so, transitions from candidate to leader.
func (s *Server) becomeLeader() {
	s.mu.Lock()
	defer s.mu.Unlock()

	quorum := len(s.cluster)/2 + 1
	for i := range s.cluster {
		if s.cluster[i].votedFor == s.id && quorum > 0 {
			quorum--
		}
	}

	if quorum != 0 {
		return
	}

	for i := range s.cluster {
		s.cluster[i].nextIndex = uint64(len(s.log) + 1)
		// Both nextIndex and matchIndex reset on every election,
		// even for peers we thought were caught up before.
		s.cluster[i].matchIndex = 0
	}

	s.debug("elected leader")
	s.state = leader

	// A new leader can't know which entries from prior terms are
	// committed until it commits something in its own term (§8), so
	// it appends a no-op immediately. AllCommitted/UserEntries skip
	// entries with an empty Command.
	s.log = append(s.log, Entry{Term: s.currentTerm})
	s.persist(true, 1)

	// Trigger an immediate appendEntries broadcast on the next tick
	// instead of waiting a full heartbeat interval.
	s.heartbeatTimeout = time.Now()
}
