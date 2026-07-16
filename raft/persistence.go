package raft

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path"
)

// On-disk layout of a node's metadata file:
//
//	Bytes 0    - 8:    current term
//	Bytes 8    - 16:   voted for
//	Bytes 16   - 24:   log length
//	Bytes 4096 - N:    log entries, one fixed-size record each
//
// The header lives in its own page so appending log entries never has to
// rewrite them; writing to a deleted file is silently a no-op on Linux, so
// callers must not assume persist() failing loudly means the file exists.
const (
	pageSize    = 4096
	entryHeader = 16
	entrySize   = 128
)

func (s *Server) metadataFilename() string {
	return fmt.Sprintf("md_%d.dat", s.id)
}

// Metadata returns the filename this server persists to, relative to its
// metadataDir.
func (s *Server) Metadata() string {
	return s.metadataFilename()
}

func (s *Server) ensureLog() {
	if len(s.log) == 0 {
		// Index 0 is never applied; keeping a sentinel entry there
		// means every real index can use PrevLogIndex/PrevLogTerm
		// without a special case for "no previous entry".
		s.log = append(s.log, Entry{})
	}
}

// setVotedFor and getVotedFor must be called with s.mu held.
func (s *Server) setVotedFor(id uint64) {
	s.cluster[s.clusterIndex].votedFor = id
}

func (s *Server) getVotedFor() uint64 {
	return s.cluster[s.clusterIndex].votedFor
}

// persist flushes currentTerm, votedFor, log length, and (if writeLog)
// the last nNewEntries log entries to disk, then fsyncs. Must be called
// with s.mu held.
func (s *Server) persist(writeLog bool, nNewEntries int) {
	if writeLog && nNewEntries == 0 {
		nNewEntries = len(s.log)
	}

	s.fd.Seek(0, io.SeekStart)

	var header [pageSize]byte
	binary.LittleEndian.PutUint64(header[0:8], s.currentTerm)
	binary.LittleEndian.PutUint64(header[8:16], s.getVotedFor())
	binary.LittleEndian.PutUint64(header[16:24], uint64(len(s.log)))

	n, err := s.fd.Write(header[:])
	if err != nil {
		panic(err)
	}
	serverAssert(s, "wrote full header page", n, pageSize)

	if writeLog && nNewEntries > 0 {
		firstNewIndex := max(len(s.log)-nNewEntries, 0)

		s.fd.Seek(int64(pageSize+entrySize*firstNewIndex), io.SeekStart)
		w := bufio.NewWriter(s.fd)

		var rec [entrySize]byte
		for i := firstNewIndex; i < len(s.log); i++ {
			e := s.log[i]
			if len(e.Command) > entrySize-entryHeader {
				panic(fmt.Sprintf("command too large (%d bytes); max is %d", len(e.Command), entrySize-entryHeader))
			}

			binary.LittleEndian.PutUint64(rec[0:8], e.Term)
			binary.LittleEndian.PutUint64(rec[8:16], uint64(len(e.Command)))
			copy(rec[16:], e.Command)

			n, err := w.Write(rec[:])
			if err != nil {
				panic(err)
			}
			serverAssert(s, "wrote full entry record", n, entrySize)
		}

		if err := w.Flush(); err != nil {
			panic(err)
		}
	}

	if err := s.fd.Sync(); err != nil {
		panic(err)
	}
	s.debugf("persisted: term=%d log_len=%d new_entries=%d voted_for=%d", s.currentTerm, len(s.log), nNewEntries, s.getVotedFor())
}

// restore loads persistent state from disk, or initializes a fresh log if
// no metadata file exists yet.
func (s *Server) restore() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.fd == nil {
		fd, err := os.OpenFile(
			path.Join(s.metadataDir, s.metadataFilename()),
			os.O_SYNC|os.O_CREATE|os.O_RDWR,
			0755,
		)
		if err != nil {
			panic(err)
		}
		s.fd = fd
	}

	s.fd.Seek(0, io.SeekStart)

	var header [pageSize]byte
	n, err := s.fd.Read(header[:])
	if err == io.EOF {
		s.ensureLog()
		return
	} else if err != nil {
		panic(err)
	}
	serverAssert(s, "read full header page", n, pageSize)

	s.currentTerm = binary.LittleEndian.Uint64(header[0:8])
	s.setVotedFor(binary.LittleEndian.Uint64(header[8:16]))
	logLen := binary.LittleEndian.Uint64(header[16:24])

	s.log = nil
	if logLen > 0 {
		s.fd.Seek(int64(pageSize), io.SeekStart)

		var rec [entrySize]byte
		for i := uint64(0); i < logLen; i++ {
			n, err := s.fd.Read(rec[:])
			if err != nil {
				panic(err)
			}
			serverAssert(s, "read full entry record", n, entrySize)

			var e Entry
			e.Term = binary.LittleEndian.Uint64(rec[0:8])
			cmdLen := binary.LittleEndian.Uint64(rec[8:16])
			e.Command = append([]byte(nil), rec[16:16+cmdLen]...)
			s.log = append(s.log, e)
		}
	}

	s.ensureLog()
}
