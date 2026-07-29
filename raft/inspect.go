package raft

// This file collects small read-only accessors used by test harnesses
// (see cmd/stress) to observe cluster state without reaching into
// unexported fields.

func (s *Server) Id() uint64 {
	return s.id
}

func (s *Server) IsLeader() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state == leader
}

// AllCommitted reports whether every user-submitted entry (i.e.
// excluding the blank no-ops leaders commit on election) has been
// applied locally, plus a completion percentage for progress logging.
func (s *Server) AllCommitted() (bool, float64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i := max(s.commitIndex, 1) - 1; i > 0; i-- {
		if len(s.log[i].Command) > 0 {
			return s.lastApplied >= uint64(i), float64(s.lastApplied) / float64(len(s.log)) * 100
		}
	}

	// No user-submitted entries found at all.
	return true, 100
}

// EntriesIterator walks the log skipping blank (no-op) entries.
type EntriesIterator struct {
	s     *Server
	index int
	Entry Entry
}

// Next advances the iterator and returns the log index it stopped at
// plus whether this was the last user entry.
func (it *EntriesIterator) Next() (int, bool) {
	it.s.mu.Lock()
	defer it.s.mu.Unlock()

	for it.index < len(it.s.log) {
		it.Entry = it.s.log[it.index]
		it.index++
		if len(it.Entry.Command) > 0 {
			break
		}
	}

	hasMore := false
	for i := it.index; i < len(it.s.log); i++ {
		if len(it.s.log[i].Command) > 0 {
			hasMore = true
			break
		}
	}

	return it.index, !hasMore
}

// UserEntries returns an iterator over log entries excluding blank
// no-ops.
func (s *Server) UserEntries() *EntriesIterator {
	return &EntriesIterator{s: s}
}

// AllEntries returns the full log, including blank no-ops.
func (s *Server) AllEntries() []Entry {
	return s.log
}

// LogEntrySummary is a JSON-friendly view of one log slot, deliberately
// omitting the raw Command bytes (which may not be printable) and the
// internal result channel.
type LogEntrySummary struct {
	Term       uint64 `json:"term"`
	HasCommand bool   `json:"hasCommand"`
	Committed  bool   `json:"committed"`
	Applied    bool   `json:"applied"`
}

// PeerStatus is the leader-only replication progress for one other node,
// included in Status() so a dashboard can show how far behind each peer
// is without granting it access to unexported ClusterMember fields.
type PeerStatus struct {
	Id         uint64 `json:"id"`
	Address    string `json:"address"`
	NextIndex  uint64 `json:"nextIndex"`
	MatchIndex uint64 `json:"matchIndex"`
}

// Status is a point-in-time, JSON-friendly snapshot of everything a
// dashboard or test harness would want to display about this node. It's
// the one blessed way to observe a Server from outside the package.
type Status struct {
	Id          uint64            `json:"id"`
	Address     string            `json:"address"`
	State       string            `json:"state"`
	Term        uint64            `json:"term"`
	LeaderId    uint64            `json:"leaderId"`
	CommitIndex uint64            `json:"commitIndex"`
	LastApplied uint64            `json:"lastApplied"`
	Log         []LogEntrySummary `json:"log"`
	Peers       []PeerStatus      `json:"peers"`
}

// Status returns a snapshot of this server's state suitable for
// serializing straight to JSON.
func (s *Server) Status() Status {
	s.mu.Lock()
	defer s.mu.Unlock()

	log := make([]LogEntrySummary, len(s.log))
	for i, e := range s.log {
		log[i] = LogEntrySummary{
			Term:       e.Term,
			HasCommand: len(e.Command) > 0,
			Committed:  uint64(i) <= s.commitIndex,
			Applied:    uint64(i) < s.lastApplied,
		}
	}

	var peers []PeerStatus
	for i, m := range s.cluster {
		if i == s.clusterIndex {
			continue
		}
		peers = append(peers, PeerStatus{
			Id:         m.Id,
			Address:    m.Address,
			NextIndex:  m.nextIndex,
			MatchIndex: m.matchIndex,
		})
	}

	return Status{
		Id:          s.id,
		Address:     s.address,
		State:       string(s.state),
		Term:        s.currentTerm,
		LeaderId:    s.leaderId,
		CommitIndex: s.commitIndex,
		LastApplied: s.lastApplied,
		Log:         log,
		Peers:       peers,
	}
}
