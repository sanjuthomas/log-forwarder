package pipeline

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sanjuthomas/log-forwarder/internal/config"
	"github.com/sanjuthomas/log-forwarder/internal/state"
	"github.com/sanjuthomas/log-forwarder/internal/watcher"
)

type thresholdSink struct {
	mu        sync.Mutex
	failUntil int
	calls     int
}

func (s *thresholdSink) Publish(ctx context.Context, payload []byte) error {
	return s.PublishBatch(ctx, [][]byte{payload})
}

func (s *thresholdSink) PublishBatch(_ context.Context, _ [][]byte) error {
	s.mu.Lock()
	s.calls++
	fail := s.calls <= s.failUntil
	s.mu.Unlock()
	if fail {
		return errors.New("publish failed")
	}
	return nil
}

func (s *thresholdSink) Close() error { return nil }

func (s *thresholdSink) setFailUntil(n int) {
	s.mu.Lock()
	s.failUntil = n
	s.mu.Unlock()
}

func TestHibernateWakeRetriesAndExitsHibernate(t *testing.T) {
	watermarks, err := state.NewStore(filepath.Join(t.TempDir(), "watermarks.json"))
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}

	cfg := testPipelineConfig()
	cfg.Pipeline.PublishBatch = config.PublishBatchConfig{
		MaxBytes:       1 << 20,
		FlushInterval:  "10ms",
		OnFlushFailure: config.OnFlushFailureHibernate,
		MaxAttempts:    2,
		Hibernate: config.HibernateConfig{
			WakeEnabled:  true,
			WakeInterval: "50ms",
		},
	}
	cfg.Pipeline.PublishRetry = config.PublishRetryConfig{
		InitialBackoff: "1ms",
		MaxBackoff:     "5ms",
		MaxAttempts:    2,
	}

	const path = "/tmp/test.log"
	sink := &thresholdSink{failUntil: 100}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	pipe, err := New(cfg, sink, logger, Options{Watermarks: watermarks})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	wakeSignal := make(chan struct{})
	pipe.hibernateWakeAfter = func(_ time.Duration) <-chan time.Time {
		ch := make(chan time.Time, 1)
		go func() {
			<-wakeSignal
			ch <- time.Now()
		}()
		return ch
	}

	lines := make(chan watcher.LineEvent, 1)
	lines <- watcher.LineEvent{
		Path:   path,
		Line:   []byte("2024-01-01T00:00:00Z\tINFO\twake-line"),
		Offset: 42,
		Inode:  1,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		_ = pipe.Run(ctx, lines)
		close(done)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if pipe.Hibernating() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !pipe.Hibernating() {
		t.Fatal("expected pipeline to enter hibernate")
	}

	sink.setFailUntil(0)
	close(wakeSignal)

	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if !pipe.Hibernating() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if pipe.Hibernating() {
		t.Fatal("expected pipeline to exit hibernate after successful wake retry")
	}

	entry, ok := watermarks.Get(path)
	if !ok {
		t.Fatal("expected watermark after successful wake retry")
	}
	if entry.Offset != 42 {
		t.Fatalf("watermark offset = %d, want 42", entry.Offset)
	}

	cancel()
	<-done
}

func TestHibernateWakeDisabledStaysHibernating(t *testing.T) {
	cfg := testPipelineConfig()
	cfg.Pipeline.PublishBatch = config.PublishBatchConfig{
		MaxBytes:       1 << 20,
		FlushInterval:  "10ms",
		OnFlushFailure: config.OnFlushFailureHibernate,
		MaxAttempts:    2,
		Hibernate: config.HibernateConfig{
			WakeEnabled: false,
		},
	}
	cfg.Pipeline.PublishRetry = config.PublishRetryConfig{
		InitialBackoff: "1ms",
		MaxBackoff:     "5ms",
		MaxAttempts:    2,
	}

	sink := &flakySink{failures: 10}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	pipe, err := New(cfg, sink, logger, Options{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	var wakeLoops atomic.Int32
	pipe.hibernateWakeAfter = func(_ time.Duration) <-chan time.Time {
		wakeLoops.Add(1)
		return make(chan time.Time)
	}

	lines := make(chan watcher.LineEvent, 1)
	lines <- watcher.LineEvent{
		Path:   "/tmp/test.log",
		Line:   []byte("2024-01-01T00:00:00Z\tINFO\tstay"),
		Offset: 10,
		Inode:  1,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = pipe.Run(ctx, lines) }()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if pipe.Hibernating() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !pipe.Hibernating() {
		t.Fatal("expected hibernate")
	}

	time.Sleep(150 * time.Millisecond)
	if wakeLoops.Load() != 0 {
		t.Fatalf("wake loops = %d, want 0 when wake_enabled is false", wakeLoops.Load())
	}

	cancel()
}
