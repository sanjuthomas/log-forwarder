package pipeline

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sanjuthomas/log-forwarder/internal/config"
	"github.com/sanjuthomas/log-forwarder/internal/state"
	"github.com/sanjuthomas/log-forwarder/internal/watcher"
)

func TestDeadLetterAdvancesWatermarkOnFlushFailure(t *testing.T) {
	dlqDir := t.TempDir()
	watermarks, err := state.NewStore(filepath.Join(t.TempDir(), "watermarks.json"))
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}

	cfg := deadLetterPipelineConfig(dlqDir, 10)
	sink := &flakySink{failures: 100}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	pipe, err := New(cfg, sink, logger, Options{Watermarks: watermarks})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	const path = "/tmp/test.log"
	lines := make(chan watcher.LineEvent, 1)
	lines <- watcher.LineEvent{
		Path:   path,
		Line:   []byte("2024-01-01T00:00:00Z\tINFO\tdlq-line"),
		Offset: 42,
		Inode:  1,
	}
	close(lines)

	if err := pipe.Run(context.Background(), lines); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	entry, ok := watermarks.Get(path)
	if !ok {
		t.Fatal("expected watermark after dead letter write")
	}
	if entry.Offset != 42 {
		t.Fatalf("watermark offset = %d, want 42", entry.Offset)
	}

	if countJSONLFiles(t, dlqDir) != 1 {
		t.Fatalf("dead letter jsonl file count != 1")
	}
	if pipe.ConsecutiveDLQSnapshot() != 1 {
		t.Fatalf("consecutive DLQ = %d, want 1", pipe.ConsecutiveDLQSnapshot())
	}
	if pipe.Hibernating() {
		t.Fatal("expected not to hibernate before consecutive limit")
	}
}

func TestDeadLetterHibernatesAtConsecutiveLimit(t *testing.T) {
	dlqDir := t.TempDir()
	watermarks, err := state.NewStore(filepath.Join(t.TempDir(), "watermarks.json"))
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}

	cfg := deadLetterPipelineConfig(dlqDir, 2)
	cfg.Pipeline.PublishBatch.MaxBytes = 1 << 20
	cfg.Pipeline.PublishBatch.FlushInterval = "20ms"

	sink := &flakySink{failures: 100}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	pipe, err := New(cfg, sink, logger, Options{Watermarks: watermarks})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	const path = "/tmp/test.log"
	lines := make(chan watcher.LineEvent, 2)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		_ = pipe.Run(ctx, lines)
		close(done)
	}()

	lines <- watcher.LineEvent{
		Path:   path,
		Line:   []byte("2024-01-01T00:00:00Z\tINFO\tfirst-dlq"),
		Offset: 20,
		Inode:  1,
	}
	waitForDLQFiles(t, dlqDir, 1)
	time.Sleep(30 * time.Millisecond)

	lines <- watcher.LineEvent{
		Path:   path,
		Line:   []byte("2024-01-01T00:00:01Z\tINFO\tsecond-dlq"),
		Offset: 40,
		Inode:  1,
	}
	time.Sleep(30 * time.Millisecond)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if pipe.Hibernating() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !pipe.Hibernating() {
		t.Fatal("expected hibernate after consecutive dead letter limit")
	}
	if pipe.ConsecutiveDLQSnapshot() != 2 {
		t.Fatalf("consecutive DLQ = %d, want 2", pipe.ConsecutiveDLQSnapshot())
	}

	waitForDLQFiles(t, dlqDir, 2)

	cancel()
	<-done
}

func waitForDLQFiles(t *testing.T, dir string, want int) {
	t.Helper()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if countJSONLFiles(t, dir) >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("dead letter jsonl file count = %d, want %d", countJSONLFiles(t, dir), want)
}

func countJSONLFiles(t *testing.T, dir string) int {
	t.Helper()
	files, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir() error = %v", err)
	}
	n := 0
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".jsonl") && !strings.Contains(f.Name(), ".tmp") {
			n++
		}
	}
	return n
}

func deadLetterPipelineConfig(dlqDir string, maxConsecutive int) *config.Config {
	cfg := testPipelineConfig()
	cfg.Pipeline.PublishBatch = config.PublishBatchConfig{
		MaxBytes:       1 << 20,
		FlushInterval:  "10ms",
		OnFlushFailure: config.OnFlushFailureDeadLetter,
		MaxAttempts:    2,
		DeadLetter: config.DeadLetterConfig{
			Path:                  dlqDir,
			MaxConsecutiveBatches: maxConsecutive,
		},
	}
	cfg.Pipeline.PublishRetry = config.PublishRetryConfig{
		InitialBackoff: "1ms",
		MaxBackoff:     "5ms",
		MaxAttempts:    2,
	}
	return cfg
}
