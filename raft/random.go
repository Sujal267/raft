package raft

import (
	"math/rand"
	"time"
)

// randomDuration returns a random duration in [minMs, maxMs) milliseconds.
// Randomizing election timeouts is what keeps split votes rare in Raft.
func randomDuration(minMs, maxMs int) time.Duration {
	return time.Duration(minMs+rand.Intn(maxMs-minMs)) * time.Millisecond
}
