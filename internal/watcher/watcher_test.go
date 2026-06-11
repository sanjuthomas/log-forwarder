package watcher

import (
	"bufio"
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sanjuthomas/log-forwarder/internal/config"
	"github.com/sanjuthomas/log-forwarder/internal/metrics"
	"github.com/sanjuthomas/log-forwarder/internal/state"
)

func newTestWatcher(t *testing.T, lines chan LineEvent, watermarks *state.Store, watch config.WatchConfig) *Watcher {
	t.Helper()

	cfg := config.Default()
	cfg.Watch = watch
	return New(cfg, lines, watermarks, &metrics.Collector{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func openFileState(t *testing.T, path string) *fileState {
	t.Helper()

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	inode, err := fileInode(path)
	if err != nil {
		t.Fatalf("fileInode() error = %v", err)
	}
	return &fileState{
		path:   path,
		file:   f,
		reader: bufio.NewReader(f),
		inode:  inode,
	}
}

func drainLineEvents(lines <-chan LineEvent) []LineEvent {
	var events []LineEvent
	for {
		select {
		case event := <-lines:
			events = append(events, event)
		default:
			return events
		}
	}
}

func TestInitialOffset_ResumeFromMatchingWatermark(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")
	if err := os.WriteFile(logPath, []byte("data\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	inode, err := fileInode(logPath)
	if err != nil {
		t.Fatal(err)
	}

	store, err := state.NewStore(filepath.Join(dir, "watermarks.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Set(logPath, 5, inode); err != nil {
		t.Fatal(err)
	}

	w := newTestWatcher(t, make(chan LineEvent), store, config.WatchConfig{})
	offset, resumed := w.initialOffset(logPath, inode)
	if !resumed {
		t.Fatal("expected resume from watermark")
	}
	if offset != 5 {
		t.Fatalf("offset = %d, want 5", offset)
	}
}

func TestInitialOffset_ResetOnInodeChange(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")
	if err := os.WriteFile(logPath, []byte("data\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	inode, err := fileInode(logPath)
	if err != nil {
		t.Fatal(err)
	}

	store, err := state.NewStore(filepath.Join(dir, "watermarks.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Set(logPath, 5, inode+999); err != nil {
		t.Fatal(err)
	}

	w := newTestWatcher(t, make(chan LineEvent), store, config.WatchConfig{})
	offset, resumed := w.initialOffset(logPath, inode)
	if resumed {
		t.Fatal("expected no resume when inode changed")
	}
	if offset != 0 {
		t.Fatalf("offset = %d, want 0", offset)
	}
}

func TestInitialOffset_NoWatermarkStore(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")
	if err := os.WriteFile(logPath, []byte("data\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	inode, err := fileInode(logPath)
	if err != nil {
		t.Fatal(err)
	}

	w := newTestWatcher(t, make(chan LineEvent), nil, config.WatchConfig{})
	offset, resumed := w.initialOffset(logPath, inode)
	if resumed || offset != 0 {
		t.Fatalf("offset = %d resumed = %v, want 0 false", offset, resumed)
	}
}

func TestInitialOffset_MissingEntry(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")
	if err := os.WriteFile(logPath, []byte("data\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	inode, err := fileInode(logPath)
	if err != nil {
		t.Fatal(err)
	}

	store, err := state.NewStore(filepath.Join(dir, "watermarks.json"))
	if err != nil {
		t.Fatal(err)
	}

	w := newTestWatcher(t, make(chan LineEvent), store, config.WatchConfig{})
	offset, resumed := w.initialOffset(logPath, inode)
	if resumed || offset != 0 {
		t.Fatalf("offset = %d resumed = %v, want 0 false", offset, resumed)
	}
}

func TestReadNewLines_EmptyLineAdvancesOffsetNoEvent(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")
	content := "line-one\n\nline-two\n"
	if err := os.WriteFile(logPath, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	lines := make(chan LineEvent, 4)
	w := newTestWatcher(t, lines, nil, config.WatchConfig{})
	state := openFileState(t, logPath)
	if err := w.readNewLines(state); err != nil {
		t.Fatalf("readNewLines() error = %v", err)
	}

	events := drainLineEvents(lines)
	if len(events) != 2 {
		t.Fatalf("len(events) = %d, want 2", len(events))
	}
	if string(events[0].Line) != "line-one" || string(events[1].Line) != "line-two" {
		t.Fatalf("events = %q and %q", events[0].Line, events[1].Line)
	}
	if state.offset != int64(len(content)) {
		t.Fatalf("state.offset = %d, want %d", state.offset, len(content))
	}
}

func TestReadNewLines_EventMetadata(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")
	line := "hello\n"
	if err := os.WriteFile(logPath, []byte(line), 0o644); err != nil {
		t.Fatal(err)
	}

	lines := make(chan LineEvent, 1)
	w := newTestWatcher(t, lines, nil, config.WatchConfig{})
	state := openFileState(t, logPath)
	if err := w.readNewLines(state); err != nil {
		t.Fatalf("readNewLines() error = %v", err)
	}

	event := <-lines
	if event.Path != logPath {
		t.Fatalf("Path = %q, want %q", event.Path, logPath)
	}
	if string(event.Line) != "hello" {
		t.Fatalf("Line = %q, want hello", event.Line)
	}
	if event.Offset != int64(len(line)) {
		t.Fatalf("Offset = %d, want %d", event.Offset, len(line))
	}
	if event.Inode != state.inode {
		t.Fatalf("Inode = %d, want %d", event.Inode, state.inode)
	}
}

func TestScan_PrunesFileRemovedFromGlob(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")
	if err := os.WriteFile(logPath, []byte("line\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	lines := make(chan LineEvent, 4)
	w := newTestWatcher(t, lines, nil, config.WatchConfig{
		Paths:    []string{dir},
		Patterns: []string{"*.log"},
		Poll:     "10ms",
	})

	if err := w.scan(); err != nil {
		t.Fatalf("scan() error = %v", err)
	}
	if w.FileCount() != 1 {
		t.Fatalf("FileCount() = %d, want 1", w.FileCount())
	}
	drainLineEvents(lines)

	if err := os.Remove(logPath); err != nil {
		t.Fatal(err)
	}
	if err := w.scan(); err != nil {
		t.Fatalf("scan() after remove error = %v", err)
	}
	if w.FileCount() != 0 {
		t.Fatalf("FileCount() = %d, want 0 after file removed from glob", w.FileCount())
	}
}

func TestTailFile_ResumesFromWatermark(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")
	content := "line-one\nline-two\n"
	if err := os.WriteFile(logPath, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	inode, err := fileInode(logPath)
	if err != nil {
		t.Fatal(err)
	}

	store, err := state.NewStore(filepath.Join(dir, "watermarks.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Set(logPath, int64(len("line-one\n")), inode); err != nil {
		t.Fatal(err)
	}

	lines := make(chan LineEvent, 2)
	w := newTestWatcher(t, lines, store, config.WatchConfig{})
	if err := w.tailFile(logPath); err != nil {
		t.Fatalf("tailFile() error = %v", err)
	}

	events := drainLineEvents(lines)
	if len(events) != 1 {
		t.Fatalf("len(events) = %d, want 1", len(events))
	}
	if string(events[0].Line) != "line-two" {
		t.Fatalf("Line = %q, want line-two", events[0].Line)
	}
}

func TestTailFile_InodeChangeRestartsFromBeginning(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")
	if err := os.WriteFile(logPath, []byte("before-rotate\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	lines := make(chan LineEvent, 4)
	w := newTestWatcher(t, lines, nil, config.WatchConfig{
		Paths:    []string{dir},
		Patterns: []string{"*.log"},
		Poll:     "10ms",
	})

	if err := w.scan(); err != nil {
		t.Fatalf("scan() error = %v", err)
	}
	events := drainLineEvents(lines)
	if len(events) != 1 || string(events[0].Line) != "before-rotate" {
		t.Fatalf("first events = %#v, want before-rotate", events)
	}

	if err := os.Rename(logPath, logPath+".1"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(logPath, []byte("after-rotate\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Allow filesystem metadata to settle before re-scanning.
	time.Sleep(10 * time.Millisecond)
	if err := w.scan(); err != nil {
		t.Fatalf("scan() after rotate error = %v", err)
	}

	events = drainLineEvents(lines)
	if len(events) != 1 {
		t.Fatalf("len(events) after rotate = %d, want 1", len(events))
	}
	if string(events[0].Line) != "after-rotate" {
		t.Fatalf("Line = %q, want after-rotate", events[0].Line)
	}
}

func TestSendLineEventBlock(t *testing.T) {
	t.Parallel()

	lines := make(chan LineEvent, 1)
	lines <- LineEvent{Path: "/tmp/app.log", Line: []byte("filled")}

	w := &Watcher{
		onFull:  "block",
		lines:   lines,
		metrics: &metrics.Collector{},
		logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	done := make(chan struct{})
	go func() {
		w.sendLineEvent(LineEvent{Path: "/tmp/app.log", Line: []byte("blocked")})
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("expected block mode to wait on full buffer")
	case event := <-lines:
		if string(event.Line) != "filled" {
			t.Fatalf("first event = %q", event.Line)
		}
	}

	<-done
	if got := len(lines); got != 1 {
		t.Fatalf("buffer len = %d, want 1", got)
	}
}

func TestSendLineEventBlockUnblocksOnShutdown(t *testing.T) {
	t.Parallel()

	lines := make(chan LineEvent, 1)
	lines <- LineEvent{Path: "/tmp/app.log", Line: []byte("filled")}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w := &Watcher{
		onFull:  "block",
		lines:   lines,
		runCtx:  ctx,
		metrics: &metrics.Collector{},
		logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	done := make(chan bool, 1)
	go func() {
		done <- w.sendLineEvent(LineEvent{Path: "/tmp/app.log", Line: []byte("blocked")})
	}()

	select {
	case <-done:
		t.Fatal("expected block mode to wait on full buffer")
	case <-time.After(100 * time.Millisecond):
	}

	cancel()

	select {
	case ok := <-done:
		if ok {
			t.Fatal("expected sendLineEvent to return false on shutdown")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for sendLineEvent to unblock on shutdown")
	}
}

func TestSendLineEventDrop(t *testing.T) {
	t.Parallel()

	lines := make(chan LineEvent, 1)
	lines <- LineEvent{Path: "/tmp/app.log", Line: []byte("filled")}

	w := &Watcher{
		onFull:  "drop",
		lines:   lines,
		metrics: &metrics.Collector{},
		logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	w.sendLineEvent(LineEvent{Path: "/tmp/app.log", Line: []byte("dropped")})

	if got := len(lines); got != 1 {
		t.Fatalf("buffer len = %d, want 1", got)
	}
}

func TestWatchPathsDeduplicatesSources(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	w := newTestWatcher(t, make(chan LineEvent), nil, config.WatchConfig{
		Sources: []config.WatchSource{
			{Path: dir, Patterns: []string{"*.log"}},
			{Path: dir, Patterns: []string{"*.out"}},
		},
	})

	paths, err := w.watchPaths()
	if err != nil {
		t.Fatalf("watchPaths() error = %v", err)
	}
	if len(paths) != 1 {
		t.Fatalf("len(paths) = %d, want 1", len(paths))
	}
}

func TestRunCancelClosesTailedFiles(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")
	if err := os.WriteFile(logPath, []byte("line\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	lines := make(chan LineEvent, 4)
	w := newTestWatcher(t, lines, nil, config.WatchConfig{
		Paths:    []string{dir},
		Patterns: []string{"*.log"},
		Poll:     "50ms",
	})

	done := make(chan error, 1)
	go func() {
		done <- w.Run(ctx)
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err == nil || err != context.Canceled {
			t.Fatalf("Run() error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for Run to stop")
	}

	if w.FileCount() != 0 {
		t.Fatalf("FileCount() = %d, want 0 after shutdown", w.FileCount())
	}
}

func TestReadNewLinesStopsOnShutdownWhileBlocked(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")
	content := "line-one\nline-two\n"
	if err := os.WriteFile(logPath, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	lines := make(chan LineEvent, 1)
	lines <- LineEvent{Path: logPath, Line: []byte("filled")}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w := &Watcher{
		onFull:  "block",
		lines:   lines,
		runCtx:  ctx,
		metrics: &metrics.Collector{},
		logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	state := openFileState(t, logPath)

	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	if err := w.readNewLines(state); err != nil {
		t.Fatalf("readNewLines() error = %v", err)
	}
	if len(drainLineEvents(lines)) != 1 {
		t.Fatalf("expected only pre-filled event while blocked")
	}
}

func TestCloseAll(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")
	if err := os.WriteFile(logPath, []byte("line\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	lines := make(chan LineEvent, 4)
	w := newTestWatcher(t, lines, nil, config.WatchConfig{})
	if err := w.tailFile(logPath); err != nil {
		t.Fatalf("tailFile() error = %v", err)
	}
	if w.FileCount() != 1 {
		t.Fatalf("FileCount() = %d, want 1", w.FileCount())
	}

	w.closeAll()
	if w.FileCount() != 0 {
		t.Fatalf("FileCount() = %d, want 0 after closeAll", w.FileCount())
	}
}

func TestNewDefaultsOnFullToBlock(t *testing.T) {
	t.Parallel()

	cfg := config.Default()
	cfg.Pipeline.OnFull = ""
	w := New(cfg, make(chan LineEvent), nil, &metrics.Collector{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if w.onFull != "block" {
		t.Fatalf("onFull = %q, want block", w.onFull)
	}
}
