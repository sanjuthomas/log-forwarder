package state

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type Entry struct {
	Offset int64  `json:"offset"`
	Inode  uint64 `json:"inode"`
}

type fileState struct {
	Files map[string]Entry `json:"files"`
}

type Store struct {
	path  string
	mu    sync.Mutex
	files map[string]Entry
}

func NewStore(path string) (*Store, error) {
	s := &Store{
		path:  path,
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
	return s.persistLocked()
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
	return nil
}

func (s *Store) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.persistLocked()
}
