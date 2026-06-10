package pipeline

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sanjuthomas/log-forwarder/internal/config"
	"github.com/sanjuthomas/log-forwarder/internal/watcher"
)

type flakySink struct {
	failures int32
	calls    int32
}

func (f *flakySink) Publish(ctx context.Context, payload []byte) error {
	return f.PublishBatch(ctx, [][]byte{payload})
}

func (f *flakySink) PublishBatch(_ context.Context, _ [][]byte) error {
	call := atomic.AddInt32(&f.calls, 1)
	if int(call) <= int(f.failures) {
		return errors.New("publish failed")
	}
	return nil
}

func (f *flakySink) Close() error { return nil }

func TestPipelinePublishRetrySucceedsAfterFailures(t *testing.T) {
	cfg := config.Default()
	cfg.Transform = config.TransformConfig{
		Type:    "delimiter",
		Columns: []string{"timestamp", "level", "message"},
		OnError: "wrap",
	}
	cfg.Pipeline.PublishRetry = config.PublishRetryConfig{
		InitialBackoff: "10ms",
		MaxBackoff:     "50ms",
		MaxAttempts:    0,
	}

	sink := &flakySink{failures: 2}
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
	}
	close(lines)

	if err := pipe.Run(context.Background(), lines); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if got := atomic.LoadInt32(&sink.calls); got != 3 {
		t.Fatalf("publish calls = %d, want 3", got)
	}
}

func TestPipelinePublishRetryExhaustsMaxAttempts(t *testing.T) {
	cfg := config.Default()
	cfg.Transform = config.TransformConfig{
		Type:    "delimiter",
		Columns: []string{"timestamp", "level", "message"},
		OnError: "wrap",
	}
	cfg.Pipeline.PublishRetry = config.PublishRetryConfig{
		InitialBackoff: "1ms",
		MaxBackoff:     "5ms",
		MaxAttempts:    2,
	}
	disablePublishBatch(cfg)

	sink := &flakySink{failures: 10}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	pipe, err := New(cfg, sink, logger, Options{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	lines := make(chan watcher.LineEvent, 1)
	lines <- watcher.LineEvent{
		Path:   "/tmp/test.log",
		Line:   []byte("2024-01-01T00:00:00Z\tINFO\thello"),
	}
	close(lines)

	err = pipe.Run(context.Background(), lines)
	if err == nil {
		t.Fatal("expected error when max attempts exhausted")
	}
	if got := atomic.LoadInt32(&sink.calls); got != 2 {
		t.Fatalf("publish calls = %d, want 2", got)
	}
}

func TestPipelinePublishTimeout(t *testing.T) {
	cfg := config.Default()
	cfg.Transform = config.TransformConfig{
		Type:    "delimiter",
		Columns: []string{"timestamp", "level", "message"},
		OnError: "wrap",
	}
	cfg.Pipeline.PublishTimeout = "1ms"
	cfg.Pipeline.PublishRetry = config.PublishRetryConfig{
		InitialBackoff: "1ms",
		MaxBackoff:     "5ms",
		MaxAttempts:    1,
	}
	disablePublishBatch(cfg)

	sink := &slowSink{delay: 50 * time.Millisecond}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	pipe, err := New(cfg, sink, logger, Options{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	lines := make(chan watcher.LineEvent, 1)
	lines <- watcher.LineEvent{
		Path:   "/tmp/test.log",
		Line:   []byte("2024-01-01T00:00:00Z\tINFO\thello"),
	}
	close(lines)

	if err := pipe.Run(context.Background(), lines); err == nil {
		t.Fatal("expected error when publish exceeds timeout")
	}
}

type slowSink struct {
	delay time.Duration
}

func (s *slowSink) Publish(ctx context.Context, _ []byte) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(s.delay):
		return nil
	}
}

func (s *slowSink) Close() error { return nil }
