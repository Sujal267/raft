// kvapi is a distributed key-value store built on top of the raft
// package. Every node runs an HTTP API; writes and (by default) reads
// are routed through Raft consensus so all nodes agree on the same
// sequence of operations.
package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

	"raft-go/kv"
	"raft-go/raft"
)

type httpServer struct {
	raft *raft.Server
	sm   *kv.StateMachine
}

// setHandler applies a Set command through consensus.
//
//	curl 'http://localhost:2020/set?key=x&value=1'
func (hs *httpServer) setHandler(w http.ResponseWriter, r *http.Request) {
	c := kv.Command{
		Kind:  kv.Set,
		Key:   r.URL.Query().Get("key"),
		Value: r.URL.Query().Get("value"),
	}

	if _, err := hs.raft.Apply([][]byte{kv.Encode(c)}); err != nil {
		log.Printf("could not set key: %s", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
	}
}

// getHandler reads a key. By default the read goes through consensus so
// it reflects the latest committed value even on a follower that hasn't
// heard the newest heartbeat; pass relaxed=true to read the local copy
// directly instead (faster, but possibly stale).
//
//	curl 'http://localhost:2020/get?key=x'
//	curl 'http://localhost:2020/get?key=x&relaxed=true'
func (hs *httpServer) getHandler(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")

	var value []byte
	var err error

	if r.URL.Query().Get("relaxed") == "true" {
		v, ok := hs.sm.Peek(key)
		if !ok {
			err = fmt.Errorf("key not found: %s", key)
		} else {
			value = []byte(v)
		}
	} else {
		var results []raft.ApplyResult
		results, err = hs.raft.Apply([][]byte{kv.Encode(kv.Command{Kind: kv.Get, Key: key})})
		if err == nil {
			if len(results) != 1 {
				err = fmt.Errorf("expected exactly one result, got %d", len(results))
			} else if results[0].Error != nil {
				err = results[0].Error
			} else {
				value = results[0].Result
			}
		}
	}

	if err != nil {
		log.Printf("could not get key: %s", err)
		http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}

	w.Write(value)
}

type config struct {
	cluster []raft.ClusterMember
	index   int
	http    string
}

func parseArgs() config {
	var cfg config
	var nodeSet, httpSet, clusterSet bool

	args := os.Args[1:]
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--node":
			i++
			idx, err := strconv.Atoi(args[i])
			if err != nil {
				log.Fatalf("expected an integer in `--node %s`", args[i])
			}
			cfg.index = idx
			nodeSet = true

		case "--http":
			i++
			cfg.http = args[i]
			httpSet = true

		case "--cluster":
			i++
			for _, part := range strings.Split(args[i], ";") {
				idAddress := strings.SplitN(part, ",", 2)
				id, err := strconv.ParseUint(idAddress[0], 10, 64)
				if err != nil {
					log.Fatalf("expected an integer id in `--cluster %s`", part)
				}
				cfg.cluster = append(cfg.cluster, raft.ClusterMember{
					Id:      id,
					Address: idAddress[1],
				})
			}
			clusterSet = true
		}
	}

	if !nodeSet {
		log.Fatal("missing required flag: --node $index")
	}
	if !httpSet {
		log.Fatal("missing required flag: --http $address")
	}
	if !clusterSet {
		log.Fatal("missing required flag: --cluster $id1,$addr1;...;$idN,$addrN")
	}

	return cfg
}

func main() {
	cfg := parseArgs()

	sm := kv.NewStateMachine()

	rs := raft.NewServer(cfg.cluster, sm, ".", cfg.index)
	rs.Debug = os.Getenv("DEBUG") == "true"
	go rs.Start()

	hs := &httpServer{raft: rs, sm: sm}
	http.HandleFunc("/set", hs.setHandler)
	http.HandleFunc("/get", hs.getHandler)

	log.Printf("listening on %s", cfg.http)
	if err := http.ListenAndServe(cfg.http, nil); err != nil {
		log.Fatal(err)
	}
}
