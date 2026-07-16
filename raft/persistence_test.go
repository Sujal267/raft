package raft

import (
	"bytes"
	"os"
	"strings"
	"testing"
)

func TestPersistRestore(t *testing.T) {
	dir := t.TempDir()

	s := NewServer(
		[]ClusterMember{{Id: 1, Address: ":0"}},
		nil,
		dir,
		0,
	)

	// Creates the metadata file and seeds the sentinel log entry.
	s.restore()

	entries := []Entry{
		{Term: 23, Command: []byte("hey there")},
		{Term: 56, Command: []byte("what's cooking")},
		{Term: 809, Command: []byte(strings.Repeat("a", 100))},
	}
	s.log = entries

	s.persist(true, len(entries))
	s.restore()

	if len(s.log) != len(entries) {
		t.Fatalf("expected log length %d, got %d", len(entries), len(s.log))
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range entries {
		if entries[i].Term != s.log[i].Term {
			t.Errorf("entry %d: expected term %d, got %d", i, entries[i].Term, s.log[i].Term)
		}
		if !bytes.Equal(entries[i].Command, s.log[i].Command) {
			t.Errorf("entry %d: expected command %q, got %q", i, entries[i].Command, s.log[i].Command)
		}
	}
}

func TestRestoreFreshDirectorySeedsSentinelEntry(t *testing.T) {
	dir := t.TempDir()

	s := NewServer(
		[]ClusterMember{{Id: 1, Address: ":0"}},
		nil,
		dir,
		0,
	)
	s.restore()

	if len(s.log) != 1 {
		t.Fatalf("expected a single sentinel entry, got %d", len(s.log))
	}

	if _, err := os.Stat(dir + "/" + s.Metadata()); err != nil {
		t.Fatalf("expected metadata file to be created: %s", err)
	}
}
