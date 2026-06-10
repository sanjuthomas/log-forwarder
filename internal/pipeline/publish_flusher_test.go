package pipeline

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sanjuthomas/log-forwarder/internal/config"
	"github.com/sanjuthomas/log-forwarder/internal/watcher"
)

type slowBatchSink struct {
	delay      time.Duration
	mu         sync.Mutex
	inFlight   int32
	maxInFlight int32
	batches    [][][]byte
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
