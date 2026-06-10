package pipeline

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/sanjuthomas/log-forwarder/internal/config"
	"github.com/sanjuthomas/log-forwarder/internal/state"
	"github.com/sanjuthomas/log-forwarder/internal/watcher"
)

func TestPipelineHibernatesAfterBatchFlushFailure(t *testing.T) {
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
	}
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

	lines := make(chan watcher.LineEvent, 2)
	lines <- watcher.LineEvent{
		Path:   path,
		Line:   []byte("2024-01-01T00:00:00Z\tINFO\tfirst"),
		Offset: 10,
		Inode:  1,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		_ = pipe.Run(ctx, lines)
		close(done)
	}()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if pipe.Hibernating() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !pipe.Hibernating() {
		t.Fatal("expected pipeline to enter hibernate after publish failure")
	}
	if _, ok := watermarks.Get(path); ok {
		t.Fatal("watermark must not advance when hibernating")
	}

	lines <- watcher.LineEvent{
		Path:   path,
		Line:   []byte("2024-01-01T00:00:01Z\tINFO\tsecond"),
		Offset: 30,
		Inode:  1,
	}

	select {
	case <-done:
		t.Fatal("pipeline should remain running while hibernating")
	case <-time.After(200 * time.Millisecond):
	}

	cancel()
	<-done
}
