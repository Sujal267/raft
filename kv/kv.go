// Package kv is a tiny replicated key-value store built as a
// raft.StateMachine. It's shared by cmd/kvapi (a real multi-process
// deployment) and cmd/dashboard (an in-process visualizer), so both
// exercise identical Apply semantics.
package kv

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
)

type Kind uint8

const (
	Set Kind = iota
	Get
)

type Command struct {
	Kind  Kind
	Key   string
	Value string
}

func Encode(c Command) []byte {
	var buf bytes.Buffer
	buf.WriteByte(uint8(c.Kind))
	binary.Write(&buf, binary.LittleEndian, uint64(len(c.Key)))
	buf.WriteString(c.Key)
	binary.Write(&buf, binary.LittleEndian, uint64(len(c.Value)))
	buf.WriteString(c.Value)
	return buf.Bytes()
}

func Decode(msg []byte) Command {
	var c Command
	c.Kind = Kind(msg[0])

	keyLen := binary.LittleEndian.Uint64(msg[1:9])
	c.Key = string(msg[9 : 9+keyLen])

	if c.Kind == Set {
		valOffset := 9 + keyLen
		valLen := binary.LittleEndian.Uint64(msg[valOffset : valOffset+8])
		c.Value = string(msg[valOffset+8 : valOffset+8+valLen])
	}

	return c
}

// StateMachine is the raft.StateMachine every node applies committed
// commands to. Each node owns its own instance; correctness relies
// entirely on Raft feeding every replica the same commands in the same
// order, never on sharing this struct across nodes.
type StateMachine struct {
	db sync.Map
}

func NewStateMachine() *StateMachine {
	return &StateMachine{}
}

func (sm *StateMachine) Apply(cmd []byte) ([]byte, error) {
	c := Decode(cmd)

	switch c.Kind {
	case Set:
		sm.db.Store(c.Key, c.Value)
		return nil, nil
	case Get:
		value, ok := sm.db.Load(c.Key)
		if !ok {
			return nil, fmt.Errorf("key not found: %s", c.Key)
		}
		return []byte(value.(string)), nil
	default:
		return nil, fmt.Errorf("unknown command kind: %d", c.Kind)
	}
}

// Peek reads the local copy directly, bypassing consensus. Used for
// "relaxed" (possibly stale) reads.
func (sm *StateMachine) Peek(key string) (string, bool) {
	v, ok := sm.db.Load(key)
	if !ok {
		return "", false
	}
	return v.(string), true
}
