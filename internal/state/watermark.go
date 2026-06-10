package state

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Entry struct {
	Offset int64  `json:"offset"`
	Inode  uint64 `json:"inode"`
}

// Options controls how often watermark updates are persisted to disk.
// Zero values persist on every Set (synchronous mode).
type Options struct {
	FlushInterval time.Duration
	FlushEvery    int
}

type fileState struct {
	Files map[string]Entry `json:"files"`
}

type Store struct {
	path              string
	opts              Options
	mu                sync.Mutex
	files             map[string]Entry
	dirty             bool
	updatesSinceFlush int
}

func NewStore(path string, opts ...Options) (*Store, error) {
	var o Options
	if len(opts) > 0 {
		o = opts[0]
	}
	s := &Store{
		path:  path,
		opts:  o,
		files: make(map[string]Entry),
	}
	if err := s.load(); err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	return s, nil
}

func (s *Store) load() error {
	data, err := os.ReadFile(s.path)
	if err != nil {
		return err
	}
	var state fileState
	if err := json.Unmarshal(data, &state); err != nil {
		return fmt.Errorf("parse watermark file: %w", err)
	}
	if state.Files != nil {
		s.files = state.Files
	}
	return nil
}

func (s *Store) Get(path string) (Entry, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.files[path]
	return entry, ok
}

func (s *Store) Set(path string, offset int64, inode uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.files[path] = Entry{Offset: offset, Inode: inode}
	s.dirty = true
	s.updatesSinceFlush++
	if s.shouldPersistLocked() {
		return s.persistLocked()
	}
	return nil
}

func (s *Store) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.dirty {
		return nil
	}
	return s.persistLocked()
}

func (s *Store) RunPeriodicFlush(ctx context.Context) {
	if s.opts.FlushInterval <= 0 {
		return
	}

	ticker := time.NewTicker(s.opts.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.Flush(); err != nil {
				// Best-effort background flush; pipeline Set errors still surface on count-based flush.
				continue
			}
		}
	}
}

func (s *Store) shouldPersistLocked() bool {
	if s.opts.FlushInterval == 0 && s.opts.FlushEvery == 0 {
		return true
	}
	if s.opts.FlushEvery > 0 && s.updatesSinceFlush >= s.opts.FlushEvery {
		return true
	}
	return false
}

func (s *Store) persistLocked() error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return fmt.Errorf("create watermark directory: %w", err)
	}

	payload, err := json.MarshalIndent(fileState{Files: s.files}, "", "  ")
	if err != nil {
		return fmt.Errorf("encode watermarks: %w", err)
	}

	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, payload, 0o644); err != nil {
		return fmt.Errorf("write watermark file: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("replace watermark file: %w", err)
	}

	s.dirty = false
	s.updatesSinceFlush = 0
	return nil
}
