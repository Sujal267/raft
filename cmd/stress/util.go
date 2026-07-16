package main

import (
	"bytes"
	"fmt"
	"math/rand"
	"time"

	"raft-go/raft"
)

var letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randomString() string {
	b := make([]byte, 16)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func waitForLeader(servers []*raft.Server) uint64 {
	for {
		for _, s := range servers {
			if s.IsLeader() {
				return s.Id()
			}
		}
		fmt.Println("waiting for a leader...")
		time.Sleep(time.Second)
	}
}

// validateAllCommitted blocks until every server has applied all entries
// it knows to be committed. There's no shortcut here: a server can only
// discover the latest commitIndex via a heartbeat, so we just poll.
func validateAllCommitted(servers []*raft.Server) {
	time.Sleep(time.Second)

	for _, s := range servers {
		for {
			done, pct := s.AllCommitted()
			if done {
				fmt.Printf("server %d: all commits applied\n", s.Id())
				break
			}
			fmt.Printf("server %d: waiting for commits to apply (%.1f%%)\n", s.Id(), pct)
			time.Sleep(time.Second)
		}
	}
}

// validateUserEntries checks that every server's log contains exactly
// `want` (excluding blank no-ops), in the same order, regardless of
// which server originally accepted each command.
func validateUserEntries(servers []*raft.Server, want [][]byte, describe func([]byte) string) {
	fmt.Println("validating log entries across all servers...")

	for _, s := range servers {
		i := 0
		it := s.UserEntries()
		for {
			logIndex, done := it.Next()

			if i >= len(want) {
				panic(fmt.Sprintf("server %d: unexpected extra entry at position %d (log index %d)", s.Id(), i, logIndex))
			}
			if !bytes.Equal(it.Entry.Command, want[i]) {
				panic(fmt.Sprintf("server %d: entry mismatch at position %d (log index %d): got %q want %q",
					s.Id(), i, logIndex, describe(it.Entry.Command), describe(want[i])))
			}

			i++
			if done {
				break
			}
		}

		if i != len(want) {
			panic(fmt.Sprintf("server %d: expected %d entries, found %d", s.Id(), len(want), i))
		}
	}
}
