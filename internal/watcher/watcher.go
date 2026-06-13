// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package watcher

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/sanjuthomas/log-forwarder/internal/config"
	"github.com/sanjuthomas/log-forwarder/internal/metrics"
	"github.com/sanjuthomas/log-forwarder/internal/state"
)

// LineEvent carries a single tailed log line and its source file metadata.
type LineEvent struct {
	// Path is the absolute path of the source log file.
	Path string
	// Line is the trimmed line bytes without trailing newline characters.
	Line []byte
	// Offset is the byte offset in Path after reading this line.
	Offset int64
	// Inode is the source file inode at read time (rotation detection).
	Inode uint64
}

// Watcher tails log files matching configured paths and patterns.
type Watcher struct {
	cfg        config.WatchConfig
	onFull     string
	poll       time.Duration
	lines      chan<- LineEvent
	watermarks *state.Store
	metrics    *metrics.Collector
	logger     *slog.Logger

	runCtx context.Context
	mu     sync.Mutex
	files  map[string]*fileState
}

type fileState struct {
	path           string
	file           *os.File
	reader         *bufio.Reader
	offset         int64
	inode          uint64
	resumed        bool
	resumeOffset   int64
	fileSizeAtOpen int64
}

// New constructs a watcher for the configured watch paths and pipeline buffer policy.
func New(cfg *config.Config, lines chan<- LineEvent, watermarks *state.Store, collector *metrics.Collector, logger *slog.Logger) *Watcher {
	onFull := cfg.Pipeline.OnFull
	if onFull == "" {
		onFull = "block"
	}
	return &Watcher{
		cfg:        cfg.Watch,
		onFull:     onFull,
		poll:       cfg.PollInterval(),
		lines:      lines,
		watermarks: watermarks,
		metrics:    collector,
		logger:     logger,
		files:      make(map[string]*fileState),
	}
}

// FileCount returns the number of log files currently being tailed.
func (w *Watcher) FileCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.files)
}

// Run watches configured directories until ctx is canceled.
func (w *Watcher) Run(ctx context.Context) error {
	w.runCtx = ctx
	defer func() { w.runCtx = nil }()

	fsWatcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("create fs watcher: %w", err)
	}
	defer fsWatcher.Close()

	watchPaths, err := w.watchPaths()
	if err != nil {
		return err
	}
	for _, dir := range watchPaths {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("ensure watch path %q: %w", dir, err)
		}
		if err := fsWatcher.Add(dir); err != nil {
			return fmt.Errorf("watch %q: %w", dir, err)
		}
	}

	w.logger.Info("watching log directories", "paths", watchPaths)

	if err := w.scan(); err != nil {
		return err
	}

	pollTicker := time.NewTicker(w.poll)
	defer pollTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			w.closeAll()
			return ctx.Err()
		case <-pollTicker.C:
			if err := w.scan(); err != nil {
				w.logger.Error("scan failed", "error", err)
			}
		case event, ok := <-fsWatcher.Events:
			if !ok {
				return nil
			}
			if event.Has(fsnotify.Create) || event.Has(fsnotify.Write) || event.Has(fsnotify.Rename) {
				if err := w.scan(); err != nil {
					w.logger.Error("scan after event failed", "error", err, "path", event.Name)
				}
			}
		case err, ok := <-fsWatcher.Errors:
			if !ok {
				return nil
			}
			w.logger.Error("fsnotify error", "error", err)
		}
	}
}

func (w *Watcher) watchPaths() ([]string, error) {
	seen := make(map[string]struct{})
	paths := make([]string, 0, len(w.cfg.Entries()))

	for _, source := range w.cfg.Entries() {
		abs, err := filepath.Abs(source.Path)
		if err != nil {
			return nil, fmt.Errorf("resolve watch path %q: %w", source.Path, err)
		}
		if _, ok := seen[abs]; ok {
			continue
		}
		seen[abs] = struct{}{}
		paths = append(paths, abs)
	}
	return paths, nil
}

func (w *Watcher) scan() error {
	seen := make(map[string]struct{})

	for _, source := range w.cfg.Entries() {
		abs, err := filepath.Abs(source.Path)
		if err != nil {
			return err
		}

		for _, pattern := range source.Patterns {
			matches, err := filepath.Glob(filepath.Join(abs, pattern))
			if err != nil {
				return fmt.Errorf("glob %q in %q: %w", pattern, abs, err)
			}
			for _, match := range matches {
				info, err := os.Stat(match)
				if err != nil || info.IsDir() {
					continue
				}
				seen[match] = struct{}{}
				if err := w.tailFile(match); err != nil {
					w.logger.Error("tail file failed", "path", match, "error", err)
				}
			}
		}
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	for path, state := range w.files {
		if _, ok := seen[path]; !ok {
			state.close()
			delete(w.files, path)
		}
	}
	return nil
}

func (w *Watcher) tailFile(path string) error {
	inode, err := fileInode(path)
	if err != nil {
		return err
	}

	w.mu.Lock()
	state, exists := w.files[path]
	if exists && state.inode == inode {
		w.mu.Unlock()
		return w.readNewLines(state)
	}
	if exists {
		state.close()
		delete(w.files, path)
	}
	w.mu.Unlock()

	f, err := os.Open(path)
	if err != nil {
		return err
	}

	offset, resumed := w.initialOffset(path, inode)
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		_ = f.Close()
		return err
	}

	if resumed {
		w.logger.Info("resuming file from watermark", "path", path, "offset", offset)
	} else {
		w.logger.Info("tailing file from beginning", "path", path)
	}

	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return err
	}

	state = &fileState{
		path:           path,
		file:           f,
		reader:         bufio.NewReader(f),
		offset:         offset,
		inode:          inode,
		resumed:        resumed,
		resumeOffset:   offset,
		fileSizeAtOpen: info.Size(),
	}

	w.mu.Lock()
	w.files[path] = state
	w.mu.Unlock()

	return w.readNewLines(state)
}

func (w *Watcher) initialOffset(path string, inode uint64) (int64, bool) {
	if w.watermarks == nil {
		return 0, false
	}
	entry, ok := w.watermarks.Get(path)
	if !ok || entry.Inode != inode {
		return 0, false
	}
	return entry.Offset, true
}

func (w *Watcher) readNewLines(state *fileState) error {
	for {
		line, err := state.reader.ReadBytes('\n')
		if len(line) > 0 {
			trimmed := strings.TrimRight(string(line), "\r\n")
			state.offset += int64(len(line))
			if trimmed != "" {
				w.recordLineIngested(context.Background(), state)
				if !w.sendLineEvent(LineEvent{
					Path:   state.path,
					Line:   []byte(trimmed),
					Offset: state.offset,
					Inode:  state.inode,
				}) {
					return nil
				}
			}
		}
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}

// recordLineIngested updates read/replayed counters for a non-empty physical line.
// Replayed lines were already present in the file when resuming from a stale watermark
// (at-least-once restart); newly appended lines increment lines_read.
func (w *Watcher) recordLineIngested(ctx context.Context, state *fileState) {
	if state.isReplayed() {
		w.metrics.RecordLineReplayed(ctx, 1)
		return
	}
	w.metrics.RecordLineRead(ctx, 1)
}

func (s *fileState) isReplayed() bool {
	if !s.resumed || s.resumeOffset <= 0 {
		return false
	}
	return s.offset > s.resumeOffset && s.offset <= s.fileSizeAtOpen
}

// sendLineEvent enqueues a line for the pipeline. It returns false when shutdown
// was signaled while waiting on a full buffer in block mode.
func (w *Watcher) sendLineEvent(event LineEvent) bool {
	if w.onFull == "drop" {
		select {
		case w.lines <- event:
		default:
			w.metrics.RecordLineBufferDropped(context.Background())
			w.logger.Debug("dropping line, pipeline buffer full", "path", event.Path)
		}
		return true
	}
	if w.runCtx == nil {
		w.lines <- event
		return true
	}
	select {
	case w.lines <- event:
		return true
	case <-w.runCtx.Done():
		return false
	}
}

func (w *Watcher) closeAll() {
	w.mu.Lock()
	defer w.mu.Unlock()
	for path, state := range w.files {
		state.close()
		delete(w.files, path)
	}
}

func (s *fileState) close() {
	if s.file != nil {
		_ = s.file.Close()
	}
}

func fileInode(path string) (uint64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, fmt.Errorf("unsupported file stat for %q", path)
	}
	return stat.Ino, nil
}
