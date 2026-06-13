// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package pipeline

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sanjuthomas/log-forwarder/internal/config"
	"github.com/sanjuthomas/log-forwarder/internal/state"
	"github.com/sanjuthomas/log-forwarder/internal/watcher"
)

type slowBatchSink struct {
	delay       time.Duration
	mu          sync.Mutex
	inFlight    int32
	maxInFlight int32
	batches     [][][]byte
}

func (s *slowBatchSink) Publish(ctx context.Context, payload []byte) error {
	return s.PublishBatch(ctx, [][]byte{payload})
}

func (s *slowBatchSink) PublishBatch(ctx context.Context, payloads [][]byte) error {
	current := atomic.AddInt32(&s.inFlight, 1)
	for {
		prev := atomic.LoadInt32(&s.maxInFlight)
		if current <= prev || atomic.CompareAndSwapInt32(&s.maxInFlight, prev, current) {
			break
		}
	}
	defer atomic.AddInt32(&s.inFlight, -1)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(s.delay):
	}

	s.mu.Lock()
	copied := make([][]byte, len(payloads))
	for i, payload := range payloads {
		copied[i] = append([]byte(nil), payload...)
	}
	s.batches = append(s.batches, copied)
	s.mu.Unlock()
	return nil
}

func (s *slowBatchSink) Close() error { return nil }

func (s *slowBatchSink) recordCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	total := 0
	for _, batch := range s.batches {
		total += len(batch)
	}
	return total
}

func TestPublishFlusherAsyncOverlapUnderBurst(t *testing.T) {
	cfg := testPipelineConfig()
	cfg.Pipeline.PublishBatch = config.PublishBatchConfig{
		MaxBytes:      180,
		FlushInterval: "0",
	}

	sink := &slowBatchSink{delay: 150 * time.Millisecond}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	pipe, err := New(cfg, sink, logger, Options{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	lines := make(chan watcher.LineEvent, 8)
	for i := 0; i < 8; i++ {
		lines <- watcher.LineEvent{
			Path:   "/tmp/test.log",
			Line:   []byte(fmt.Sprintf("2024-01-01T00:00:00Z\tINFO\tburst-%d", i)),
			Offset: int64(10 * (i + 1)),
			Inode:  1,
		}
	}
	close(lines)

	if err := pipe.Run(context.Background(), lines); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if sink.recordCount() != 8 {
		t.Fatalf("record count = %d, want 8", sink.recordCount())
	}
	if sink.maxInFlight != 1 {
		t.Fatalf("max in-flight flushes = %d, want 1", sink.maxInFlight)
	}
}

func TestPublishFlusherBlocksWhenActiveFullDuringFlush(t *testing.T) {
	cfg := testPipelineConfig()
	cfg.Pipeline.PublishBatch = config.PublishBatchConfig{
		MaxBytes:      120,
		FlushInterval: "0",
	}

	sink := &slowBatchSink{delay: 200 * time.Millisecond}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	pipe, err := New(cfg, sink, logger, Options{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	lines := make(chan watcher.LineEvent, 6)
	for i := 0; i < 6; i++ {
		lines <- watcher.LineEvent{
			Path:   "/tmp/test.log",
			Line:   []byte(fmt.Sprintf("2024-01-01T00:00:00Z\tINFO\tblock-%d", i)),
			Offset: int64(10 * (i + 1)),
			Inode:  1,
		}
	}

	done := make(chan error, 1)
	go func() {
		done <- pipe.Run(context.Background(), lines)
	}()

	time.Sleep(50 * time.Millisecond)
	close(lines)

	if err := <-done; err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if sink.recordCount() != 6 {
		t.Fatalf("record count = %d, want 6", sink.recordCount())
	}
}

func TestPublishFlusherRecoversAfterAsyncFlushError(t *testing.T) {
	stateDir := t.TempDir()
	statePath := filepath.Join(stateDir, "watermarks.json")

	watermarks, err := state.NewStore(statePath)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}

	cfg := testPipelineConfig()
	cfg.Pipeline.PublishBatch = config.PublishBatchConfig{
		MaxBytes:      1 << 20,
		FlushInterval: "20ms",
	}

	const path = "/tmp/test.log"
	sink := &batchCapturingSink{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	pipe, err := New(cfg, sink, logger, Options{Watermarks: watermarks})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	lines := make(chan watcher.LineEvent, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		_ = pipe.Run(ctx, lines)
		close(done)
	}()

	lines <- watcher.LineEvent{
		Path:   path,
		Line:   []byte("2024-01-01T00:00:00Z\tINFO\tline-one"),
		Offset: 10,
		Inode:  1,
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if sink.recordCount() >= 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if sink.recordCount() < 1 {
		t.Fatal("timed out waiting for first publish")
	}

	if err := os.Chmod(stateDir, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(stateDir, 0o755) })

	lines <- watcher.LineEvent{
		Path:   path,
		Line:   []byte("2024-01-01T00:00:01Z\tINFO\tline-two"),
		Offset: 20,
		Inode:  1,
	}

	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if sink.recordCount() >= 2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if sink.recordCount() < 2 {
		t.Fatal("timed out waiting for second publish before watermark failure")
	}

	lines <- watcher.LineEvent{
		Path:   path,
		Line:   []byte("2024-01-01T00:00:02Z\tINFO\tline-three"),
		Offset: 30,
		Inode:  1,
	}

	select {
	case <-done:
		t.Fatal("pipeline exited after async flush error; expected recovery")
	case <-time.After(100 * time.Millisecond):
	}

	if err := os.Chmod(stateDir, 0o755); err != nil {
		t.Fatal(err)
	}

	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if sink.recordCount() >= 3 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if sink.recordCount() != 3 {
		t.Fatalf("record count = %d, want 3 after flusher recovery", sink.recordCount())
	}

	cancel()
	<-done
}
