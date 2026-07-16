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
