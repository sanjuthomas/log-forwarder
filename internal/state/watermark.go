// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

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

// Entry is the persisted read position for one tailed log file.
type Entry struct {
	Offset int64  `json:"offset"`
	Inode  uint64 `json:"inode"`
}

// Options controls how often watermark updates are persisted to disk.
// Zero values persist on every Set (synchronous mode).
type Options struct {
	FlushInterval time.Duration
	FlushEvery    int
	// ResetOnCorrupt archives a corrupt watermark file and starts with a fresh store.
	ResetOnCorrupt bool
	// OnPeriodicFlushError is called when RunPeriodicFlush fails to persist.
	OnPeriodicFlushError func(error)
}

// SetOnPeriodicFlushError registers a callback for interval flush failures.
func (s *Store) SetOnPeriodicFlushError(fn func(error)) {
	s.mu.Lock()
	s.onPeriodicFlushError = fn
	s.mu.Unlock()
}

type fileState struct {
	Files map[string]Entry `json:"files"`
}

// Store persists per-file byte offsets and inodes for resume-after-restart.
type Store struct {
	path              string
	opts              Options
	onPeriodicFlushError func(error)
	corruptBackupPath string
	mu                sync.Mutex
	files             map[string]Entry
	dirty             bool
	updatesSinceFlush int
}

// CorruptBackupPath returns the archived path when ResetOnCorrupt recovered from a corrupt file.
func (s *Store) CorruptBackupPath() string {
	return s.corruptBackupPath
}

// NewStore loads or creates the watermark file at path.
func NewStore(path string, opts ...Options) (*Store, error) {
	var o Options
	if len(opts) > 0 {
		o = opts[0]
	}
	s := &Store{
		path:                 path,
		opts:                 o,
		onPeriodicFlushError: o.OnPeriodicFlushError,
		files:                make(map[string]Entry),
	}
	if err := s.load(); err != nil {
		if os.IsNotExist(err) {
			return s, nil
		}
		if isCorruptWatermarkError(err) && o.ResetOnCorrupt {
			backup, archiveErr := archiveCorruptWatermark(s.path)
			if archiveErr != nil {
				return nil, fmt.Errorf("reset corrupt watermark %q: %w", s.path, archiveErr)
			}
			s.corruptBackupPath = backup
			return s, nil
		}
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
		return &CorruptWatermarkError{Path: s.path, Cause: err}
	}
	if state.Files != nil {
		s.files = state.Files
	}
	return nil
}

// Get returns the stored watermark for path, if any.
func (s *Store) Get(path string) (Entry, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.files[path]
	return entry, ok
}

// Set records the latest offset and inode for path and persists when policy requires.
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

// Flush writes pending watermark updates to disk immediately.
func (s *Store) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.dirty {
		return nil
	}
	return s.persistLocked()
}

// RunPeriodicFlush persists dirty watermarks on a fixed interval until ctx is canceled.
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
				s.mu.Lock()
				onError := s.onPeriodicFlushError
				s.mu.Unlock()
				if onError != nil {
					onError(err)
				}
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
