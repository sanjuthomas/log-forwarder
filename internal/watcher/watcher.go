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

// LineEvent carries a single log line and its source file path.
type LineEvent struct {
	Path   string
	Line   []byte
	Offset int64
	Inode  uint64
}

// Watcher tails log files matching configured paths and patterns.
type Watcher struct {
	cfg        config.WatchConfig
	poll       time.Duration
	lines      chan<- LineEvent
	watermarks *state.Store
	metrics    *metrics.Collector
	logger     *slog.Logger

	mu    sync.Mutex
	files map[string]*fileState
}

type fileState struct {
	path   string
	file   *os.File
	reader *bufio.Reader
	offset int64
	inode  uint64
}

func New(cfg *config.Config, lines chan<- LineEvent, watermarks *state.Store, collector *metrics.Collector, logger *slog.Logger) *Watcher {
	return &Watcher{
		cfg:        cfg.Watch,
		poll:       cfg.PollInterval(),
		lines:      lines,
		watermarks: watermarks,
		metrics:    collector,
		logger:     logger,
		files:      make(map[string]*fileState),
	}
}

func (w *Watcher) FileCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.files)
}

func (w *Watcher) Run(ctx context.Context) error {
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

	state = &fileState{
		path:   path,
		file:   f,
		reader: bufio.NewReader(f),
		offset: offset,
		inode:  inode,
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
				w.metrics.RecordLineRead(context.Background(), 1)
				w.lines <- LineEvent{
					Path:   state.path,
					Line:   []byte(trimmed),
					Offset: state.offset,
					Inode:  state.inode,
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
