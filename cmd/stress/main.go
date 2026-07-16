// stress runs a 3-node cluster in a single process (still communicating
// over real TCP and writing real files) and drives it through a basic
// correctness/stress workout: leader election, batched writes from
// multiple clients, a full restart, and recovery after losing one
// node's on-disk log entirely.
package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	"raft-go/raft"
)

type kvStateMachine struct {
	mu sync.Mutex
	kv map[string]string
}

func newKvStateMachine() *kvStateMachine {
	return &kvStateMachine{kv: make(map[string]string)}
}

func encodeSet(key, value string) []byte {
	msg := make([]byte, 3+8+8+len(key)+len(value))
	copy(msg[:3], "set")
	binary.LittleEndian.PutUint64(msg[3:11], uint64(len(key)))
	binary.LittleEndian.PutUint64(msg[11:19], uint64(len(value)))
	copy(msg[19:19+len(key)], key)
	copy(msg[19+len(key):], value)
	return msg
}

func decodeSet(msg []byte) (ok bool, key, value string) {
	if len(msg) < 3 || !bytes.Equal(msg[:3], []byte("set")) {
		return false, "", ""
	}
	keyLen := binary.LittleEndian.Uint64(msg[3:11])
	valLen := binary.LittleEndian.Uint64(msg[11:19])
	key = string(msg[19 : 19+keyLen])
	value = string(msg[19+keyLen : 19+keyLen+valLen])
	return true, key, value
}

func encodeGet(key string) []byte {
	msg := make([]byte, 3+8+8+len(key))
	copy(msg[:3], "get")
	binary.LittleEndian.PutUint64(msg[3:11], uint64(len(key)))
	copy(msg[19:19+len(key)], key)
	return msg
}

func decodeGet(msg []byte) (ok bool, key string) {
	if len(msg) < 3 || !bytes.Equal(msg[:3], []byte("get")) {
		return false, ""
	}
	keyLen := binary.LittleEndian.Uint64(msg[3:11])
	return true, string(msg[19 : 19+keyLen])
}

func describe(msg []byte) string {
	if ok, key, value := decodeSet(msg); ok {
		return fmt.Sprintf("set %s=%s", key, value)
	}
	if ok, key := decodeGet(msg); ok {
		return fmt.Sprintf("get %s", key)
	}
	return fmt.Sprintf("unknown(%x)", msg)
}

func (sm *kvStateMachine) Apply(msg []byte) ([]byte, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if ok, key, value := decodeSet(msg); ok {
		sm.kv[key] = value
		return nil, nil
	}
	if ok, key := decodeGet(msg); ok {
		return []byte(sm.kv[key]), nil
	}
	return nil, fmt.Errorf("unknown state machine message: %x", msg)
}

func removeMetadataFiles() {
	entries, err := os.ReadDir(".")
	if err != nil {
		panic(err)
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".dat") {
			os.Remove(e.Name())
		}
	}
}

func newCluster(cluster []raft.ClusterMember, sms []*kvStateMachine) []*raft.Server {
	servers := make([]*raft.Server, len(cluster))
	for i := range cluster {
		servers[i] = raft.NewServer(cluster, sms[i], ".", i)
	}
	return servers
}

func startAll(servers []*raft.Server, debug bool) {
	for _, s := range servers {
		s.Debug = debug
		s.Start()
	}
}

func shutdownAll(servers []*raft.Server) {
	for _, s := range servers {
		s.Shutdown()
	}
}

func main() {
	rand.Seed(0)
	removeMetadataFiles()

	cluster := []raft.ClusterMember{
		{Id: 1, Address: "localhost:2020"},
		{Id: 2, Address: "localhost:2021"},
		{Id: 3, Address: "localhost:2022"},
	}
	sms := []*kvStateMachine{newKvStateMachine(), newKvStateMachine(), newKvStateMachine()}
	servers := newCluster(cluster, sms)
	startAll(servers, false)

	leader := waitForLeader(servers)

	const nClients = 4
	const nEntries = 2_000
	const batchSize = 50
	fmt.Printf("clients=%d entries=%d batch=%d\n", nClients, nEntries, batchSize)

	var allEntries [][]byte
	for i := 0; i < nEntries; i++ {
		allEntries = append(allEntries, encodeSet(randomString(), randomString()))
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	var total time.Duration
	perClient := nEntries / nClients

	wg.Add(nClients)
	for c := 0; c < nClients; c++ {
		go func(c int) {
			defer wg.Done()

			start := c * perClient
			end := start + perClient
			if c == nClients-1 {
				end = nEntries
			}

			for i := start; i < end; i += batchSize {
				batchEnd := i + batchSize
				if batchEnd > end {
					batchEnd = end
				}
				batch := allEntries[i:batchEnd]

			retry:
				for {
					for _, s := range servers {
						t := time.Now()
						_, err := s.Apply(batch)
						if err == raft.ErrApplyToLeader {
							continue
						} else if err != nil {
							panic(err)
						}

						raft.Assert("leader stayed the same", s.Id(), leader)
						diff := time.Since(t)
						mu.Lock()
						total += diff
						mu.Unlock()
						break retry
					}
					time.Sleep(100 * time.Millisecond)
				}
			}
		}(c)
	}
	wg.Wait()

	fmt.Printf("total=%s avg_per_entry=%s throughput=%.1f entries/s\n",
		total, total/time.Duration(nEntries), float64(nEntries)/(float64(total)/float64(time.Second)))

	validateAllCommitted(servers)
	validateUserEntries(servers, allEntries, describe)

	for _, entry := range allEntries {
		_, key, value := decodeSet(entry)
		for i, sm := range sms {
			sm.mu.Lock()
			got := sm.kv[key]
			sm.mu.Unlock()
			raft.Assert(fmt.Sprintf("server %d state machine has correct value for %s", cluster[i].Id, key), value, got)
		}
	}

	fmt.Println("validating get...")
	_, testKey, testValue := decodeSet(allEntries[0])
	var gotValue []byte
	for _, s := range servers {
		res, err := s.Apply([][]byte{encodeGet(testKey)})
		if err == raft.ErrApplyToLeader {
			continue
		} else if err != nil {
			panic(err)
		}
		raft.Assert("leader stayed the same", s.Id(), leader)
		gotValue = res[0].Result
		break
	}
	raft.Assert("get returns the value that was set", testValue, string(gotValue))

	fmt.Println("testing shutdown and restart preserves all values...")
	shutdownAll(servers)
	servers = newCluster(cluster, sms)
	startAll(servers, false)
	waitForLeader(servers)

	allEntriesPlusGet := append(allEntries, encodeGet(testKey))
	validateAllCommitted(servers)
	validateUserEntries(servers, allEntriesPlusGet, describe)

	fmt.Println("testing recovery after deleting one server's log file...")
	shutdownAll(servers)
	servers = newCluster(cluster, sms)
	os.Remove(servers[2].Metadata())
	startAll(servers, false)
	waitForLeader(servers)

	// Give the new leader time to notice the lagging follower and
	// replicate its entire log back to it.
	time.Sleep(5 * time.Second)

	validateAllCommitted(servers)
	validateUserEntries(servers, allEntriesPlusGet, describe)

	fmt.Println("ok")
}
