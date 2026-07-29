# raft-go

A from-scratch implementation of the [Raft consensus algorithm](https://raft.github.io/)
in Go: leader election, log replication, and crash-recovery persistence, built directly
against the [extended Raft paper](https://raft.github.io/raft.pdf) (Figure 2). See
`raft/` for the protocol implementation itself.

Two things are built on top of it:

- **`cmd/kvapi`** — a real distributed key-value store. Each node is its own OS
  process with an HTTP API; writes and (by default) reads are routed through Raft so
  every node agrees on the same sequence of operations.
- **`cmd/dashboard`** — a live, interactive visualization of the cluster: leader
  election, log replication, and crash recovery, watchable and pokeable in a browser.

## Dashboard

```sh
cd web && npm install && npm run build   # builds the UI into cmd/dashboard/static
cd ../raft && go run ./cmd/dashboard     # starts a 5-node cluster + UI on :8080
```

Then open http://localhost:8080. Every node in the cluster runs in this one process,
but they still talk to each other over real TCP RPC connections — nothing about the
consensus protocol is simulated or faked. You can:

- Watch the ring of nodes: gold = leader, blue = follower, pulsing orange =
  candidate (mid-election), hatched grey = crashed.
- **Kill** the leader and watch a real election play out among the survivors.
- **Restart** a killed node and watch it replay its persisted log from disk and
  catch back up — the same crash-recovery path a real deployment relies on.
- Issue `Set`/`Get` commands through the client panel and watch the log entry
  propagate, commit (once a majority has it), and get applied.
- Read the live event log for a plain-English narration of what's happening, and the
  sidebar for a short explanation of the underlying Raft concepts.

Flags: `go run ./cmd/dashboard --nodes 7 --http :9000`.

## Key-value store (multi-process)

```sh
go run ./cmd/kvapi --node 0 --http :2020 --cluster "1,:3020;2,:3021;3,:3022"
go run ./cmd/kvapi --node 1 --http :2021 --cluster "1,:3020;2,:3021;3,:3022"
go run ./cmd/kvapi --node 2 --http :2022 --cluster "1,:3020;2,:3021;3,:3022"

curl 'localhost:2020/set?key=x&value=1'
curl 'localhost:2021/get?key=x'                # goes through consensus
curl 'localhost:2021/get?key=x&relaxed=true'   # local read, possibly stale
```

## Tests

```sh
go test ./...
```
