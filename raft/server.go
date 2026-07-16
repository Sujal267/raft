package raft

import (
	"context"
	"net"
	"net/http"
	"net/rpc"
)

// Start restores persisted state, opens the RPC listener, and launches
// the background loop that drives elections, heartbeats, and commit
// advancement. It returns once the server is listening; the driver loop
// keeps running in a goroutine until Shutdown() is called.
func (s *Server) Start() {
	s.mu.Lock()
	s.state = follower
	s.done = false
	s.mu.Unlock()

	s.restore()

	rpcServer := rpc.NewServer()
	rpcServer.Register(s)

	listener, err := net.Listen("tcp", s.address)
	if err != nil {
		panic(err)
	}

	mux := http.NewServeMux()
	mux.Handle(rpc.DefaultRPCPath, rpcServer)
	s.httpServer = &http.Server{Handler: mux}
	go s.httpServer.Serve(listener)

	go s.driveLoop()
}

// driveLoop is the single-threaded heart of the protocol: on every
// iteration it checks the current state and does exactly what a server
// in that state should do (Raft paper Figure 2, "Rules for Servers").
func (s *Server) driveLoop() {
	s.mu.Lock()
	s.resetElectionTimeout()
	s.mu.Unlock()

	for {
		s.mu.Lock()
		if s.done {
			s.mu.Unlock()
			return
		}
		state := s.state
		s.mu.Unlock()

		switch state {
		case leader:
			s.heartbeat()
			s.advanceCommitIndex()
		case follower:
			s.timeout()
			s.advanceCommitIndex()
		case candidate:
			s.timeout()
			s.becomeLeader()
		}
	}
}

// Shutdown stops the RPC listener and background loop and closes the
// metadata file. The Server cannot be reused after this; construct a new
// one with NewServer to restart.
func (s *Server) Shutdown() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.debug("shutting down")
	if s.fd != nil {
		s.fd.Close()
		s.fd = nil
	}
	if s.httpServer != nil {
		s.httpServer.Shutdown(context.Background())
	}
	s.done = true
}
