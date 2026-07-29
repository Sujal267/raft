// dashboard runs a small Raft cluster entirely in one process (each
// node still talks to its peers over real TCP RPC, exactly like
// cmd/kvapi) and serves a web UI for watching — and poking at —
// leader election, log replication, and crash recovery live.
package main

import (
	"embed"
	"encoding/json"
	"errors"
	"flag"
	"io/fs"
	"log"
	"net/http"
	"os"
	"strconv"
)

//go:embed static
var staticFS embed.FS

var errEmptyKey = errors.New("key must not be empty")

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
}

func main() {
	defaultAddr := ":8080"
	if port := os.Getenv("PORT"); port != "" {
		defaultAddr = ":" + port
	}

	nodes := flag.Int("nodes", 5, "number of simulated cluster nodes")
	httpAddr := flag.String("http", defaultAddr, "address for the dashboard's own HTTP server")
	dataDir := flag.String("data", "./dashboard-data", "directory for node metadata files")
	debug := flag.Bool("debug", false, "print raft debug logs to stdout")
	flag.Parse()

	if *nodes < 3 {
		log.Fatal("--nodes must be at least 3 (Raft needs a real quorum to be interesting)")
	}

	m := newManager(*nodes, *dataDir, *debug)
	if err := m.StartAll(); err != nil {
		log.Fatalf("could not start cluster: %s", err)
	}
	log.Printf("started %d-node raft cluster (metadata in %s)", *nodes, *dataDir)

	mux := http.NewServeMux()

	mux.HandleFunc("GET /api/status", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, m.Statuses())
	})

	mux.HandleFunc("POST /api/nodes/{id}/kill", func(w http.ResponseWriter, r *http.Request) {
		id, err := strconv.ParseUint(r.PathValue("id"), 10, 64)
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		if err := m.Kill(id); err != nil {
			writeError(w, http.StatusConflict, err)
			return
		}
		writeJSON(w, map[string]bool{"ok": true})
	})

	mux.HandleFunc("POST /api/nodes/{id}/restart", func(w http.ResponseWriter, r *http.Request) {
		id, err := strconv.ParseUint(r.PathValue("id"), 10, 64)
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		if err := m.Restart(id); err != nil {
			writeError(w, http.StatusConflict, err)
			return
		}
		writeJSON(w, map[string]bool{"ok": true})
	})

	mux.HandleFunc("POST /api/set", func(w http.ResponseWriter, r *http.Request) {
		var body struct{ Key, Value string }
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		if body.Key == "" {
			writeError(w, http.StatusBadRequest, errEmptyKey)
			return
		}
		if err := m.Set(body.Key, body.Value); err != nil {
			writeError(w, http.StatusServiceUnavailable, err)
			return
		}
		writeJSON(w, map[string]bool{"ok": true})
	})

	mux.HandleFunc("GET /api/get", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		if key == "" {
			writeError(w, http.StatusBadRequest, errEmptyKey)
			return
		}
		relaxed := r.URL.Query().Get("relaxed") == "true"

		value, err := m.Get(key, relaxed)
		if err != nil {
			writeError(w, http.StatusNotFound, err)
			return
		}
		writeJSON(w, map[string]string{"value": value})
	})

	staticContent, err := fs.Sub(staticFS, "static")
	if err != nil {
		log.Fatal(err)
	}
	mux.Handle("/", http.FileServer(http.FS(staticContent)))

	log.Printf("dashboard listening on %s", *httpAddr)
	log.Fatal(http.ListenAndServe(*httpAddr, mux))
}
