package pipeline

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/sanjuthomas/log-forwarder/internal/config"
	"github.com/sanjuthomas/log-forwarder/internal/state"
	"github.com/sanjuthomas/log-forwarder/internal/watcher"
)

type batchCapturingSink struct {
	mu       sync.Mutex
	batches  [][][]byte
	publishs int
}

func (b *batchCapturingSink) Publish(ctx context.Context, payload []byte) error {
	return b.PublishBatch(ctx, [][]byte{payload})
}

func (b *batchCapturingSink) PublishBatch(_ context.Context, payloads [][]byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	copied := make([][]byte, len(payloads))
	for i, payload := range payloads {
		copied[i] = append([]byte(nil), payload...)
	}
	b.batches = append(b.batches, copied)
	b.publishs++
	return nil
}

func (b *batchCapturingSink) Close() error { return nil }

func (b *batchCapturingSink) batchCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.batches)
}

func (b *batchCapturingSink) recordCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	total := 0
	for _, batch := range b.batches {
		total += len(batch)
	}
	return total
}

func disablePublishBatch(cfg *config.Config) {
	cfg.Pipeline.PublishBatch = config.PublishBatchConfig{
		MaxBytes:      0,
		FlushInterval: "0",
	}
}

func testPipelineConfig() *config.Config {
	cfg := config.Default()
	cfg.Transform = config.TransformConfig{
		Type:    "delimiter",
		Columns: []string{"timestamp", "level", "message"},
		OnError: "wrap",
	}
	return cfg
}

func TestPipelinePublishBatchSizeFlush(t *testing.T) {
	cfg := testPipelineConfig()
	cfg.Pipeline.PublishBatch = config.PublishBatchConfig{
		MaxBytes:      200,
		FlushInterval: "0",
	}

	sink := &batchCapturingSink{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	pipe, err := New(cfg, sink, logger, Options{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	lines := make(chan watcher.LineEvent, 3)
	for i := 0; i < 3; i++ {
		lines <- watcher.LineEvent{
			Path:   "/tmp/test.log",
			Line:   []byte(fmt.Sprintf("2024-01-01T00:00:00Z\tINFO\tmessage-%d", i)),
			Offset: int64(10 * (i + 1)),
			Inode:  1,
		}
	}
	close(lines)

	if err := pipe.Run(context.Background(), lines); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if sink.batchCount() < 2 {
		t.Fatalf("batch count = %d, want at least 2 size-triggered flushes", sink.batchCount())
	}
	if sink.recordCount() != 3 {
		t.Fatalf("record count = %d, want 3", sink.recordCount())
	}
}

func TestPipelinePublishBatchTimerFlush(t *testing.T) {
	cfg := testPipelineConfig()
	cfg.Pipeline.PublishBatch = config.PublishBatchConfig{
		MaxBytes:      1 << 20,
		FlushInterval: "50ms",
	}

	sink := &batchCapturingSink{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	pipe, err := New(cfg, sink, logger, Options{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	lines := make(chan watcher.LineEvent, 1)
	lines <- watcher.LineEvent{
		Path:   "/tmp/test.log",
		Line:   []byte("2024-01-01T00:00:00Z\tINFO\thello"),
		Offset: 42,
		Inode:  1,
	}

	done := make(chan error, 1)
	go func() {
		done <- pipe.Run(context.Background(), lines)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if sink.batchCount() > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if sink.batchCount() == 0 {
		t.Fatal("expected timer-based flush before shutdown")
	}

	close(lines)
	if err := <-done; err != nil {
		t.Fatalf("Run() error = %v", err)
	}
}

func TestPipelinePublishBatchWatermarkAfterFlush(t *testing.T) {
	watermarks, err := state.NewStore(filepath.Join(t.TempDir(), "watermarks.json"))
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}

	cfg := testPipelineConfig()
	cfg.Pipeline.PublishBatch = config.PublishBatchConfig{
		MaxBytes:      1 << 20,
		FlushInterval: "0",
	}

	const path = "/tmp/test.log"
	sink := &batchCapturingSink{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	pipe, err := New(cfg, sink, logger, Options{Watermarks: watermarks})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	lines := make(chan watcher.LineEvent, 1)
	lines <- watcher.LineEvent{
		Path:   path,
		Line:   []byte("2024-01-01T00:00:00Z\tINFO\thello"),
		Offset: 42,
		Inode:  9,
	}
	close(lines)

	if err := pipe.Run(context.Background(), lines); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if _, ok := watermarks.Get(path); !ok {
		t.Fatal("expected watermark after shutdown batch flush")
	}
}

func TestPipelinePublishBatchWatermarkStalledOnFlushFailure(t *testing.T) {
	watermarks, err := state.NewStore(filepath.Join(t.TempDir(), "watermarks.json"))
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}

	cfg := testPipelineConfig()
	disablePublishBatch(cfg)
	cfg.Pipeline.PublishRetry = config.PublishRetryConfig{
		InitialBackoff: "1ms",
		MaxBackoff:     "5ms",
		MaxAttempts:    2,
	}

	const path = "/tmp/test.log"
	sink := &flakySink{failures: 10}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	pipe, err := New(cfg, sink, logger, Options{Watermarks: watermarks})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	lines := make(chan watcher.LineEvent, 1)
	lines <- watcher.LineEvent{
		Path:   path,
		Line:   []byte("2024-01-01T00:00:00Z\tINFO\thello"),
		Offset: 42,
		Inode:  9,
	}
	close(lines)

	if err := pipe.Run(context.Background(), lines); err == nil {
		t.Fatal("expected publish failure")
	}
	if _, ok := watermarks.Get(path); ok {
		t.Fatal("watermark must not advance when batch publish fails")
	}
}
