package pipeline

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/sanjuthomas/log-forwarder/internal/config"
	"github.com/sanjuthomas/log-forwarder/internal/state"
	"github.com/sanjuthomas/log-forwarder/internal/watcher"
)

func TestPipelineErrorExitFlushesMultilineTail(t *testing.T) {
	cfg := config.Default()
	cfg.Parser = config.ParserConfig{
		Type:         "multiline",
		StartPattern: `^\d{4}-\d{2}-\d{2}`,
	}
	cfg.Transform = config.TransformConfig{
		Type:    "regex",
		Pattern: `^(?s)(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3})\s+(?P<level>\S+)\s+(?P<pid>\d+)\s+---\s+\[\s*(?P<thread>[^\]]+?)\s*\]\s+(?P<logger>\S+)\s+:\s+(?P<message>.*)$`,
		OnError: "wrap",
	}
	disablePublishBatch(cfg)
	cfg.Pipeline.PublishRetry = config.PublishRetryConfig{
		InitialBackoff: "1ms",
		MaxBackoff:     "1ms",
		MaxAttempts:    1,
	}

	sink := &flakySink{failures: 100}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	pipe, err := New(cfg, sink, logger, Options{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	const path = "/tmp/test.log"
	lines := make(chan watcher.LineEvent, 4)
	lines <- watcher.LineEvent{
		Path:   path,
		Line:   []byte("2026-06-08 10:16:22.901  ERROR 18432 --- [main] c.a.b.PaymentController : Payment failed"),
		Offset: 100,
		Inode:  1,
	}
	lines <- watcher.LineEvent{
		Path:   path,
		Line:   []byte("org.springframework.dao.DataIntegrityViolationException: could not execute statement"),
		Offset: 180,
		Inode:  1,
	}
	lines <- watcher.LineEvent{
		Path:   path,
		Line:   []byte("2026-06-08 10:16:23.001  ERROR 18432 --- [main] c.a.b.PaymentController : Retry failed"),
		Offset: 260,
		Inode:  1,
	}

	if err := pipe.Run(context.Background(), lines); err == nil {
		t.Fatal("expected publish failure")
	}
	if got := atomic.LoadInt32(&sink.calls); got < 2 {
		t.Fatalf("publish calls = %d, want at least 2 from error-path shutdown flush", got)
	}
}

func TestPipelineErrorExitFlushesPublishBuffer(t *testing.T) {
	dir := t.TempDir()
	statePath := filepath.Join(dir, "watermarks.json")
	if err := os.WriteFile(statePath, []byte("{\n  \"files\": {}\n}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })

	watermarks, err := state.NewStore(statePath)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}

	cfg := testPipelineConfig()
	cfg.Pipeline.PublishBatch = config.PublishBatchConfig{
		MaxBytes:      1 << 20,
		FlushInterval: "0",
	}
	cfg.Filter = config.FilterConfig{
		Match: "all",
		Rules: []config.FilterRuleConfig{
			{
				Type:       "field",
				Field:      "level",
				Op:         "in",
				Values:     []string{"ERROR"},
				IgnoreCase: true,
			},
		},
	}

	sink := &batchCapturingSink{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	pipe, err := New(cfg, sink, logger, Options{Watermarks: watermarks})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	const path = "/tmp/test.log"
	lines := make(chan watcher.LineEvent, 2)
	lines <- watcher.LineEvent{
		Path:   path,
		Line:   []byte("2024-01-01T00:00:00Z\tERROR\tbuffered"),
		Offset: 10,
		Inode:  1,
	}
	lines <- watcher.LineEvent{
		Path:   path,
		Line:   []byte("2024-01-01T00:00:01Z\tINFO\tfiltered"),
		Offset: 20,
		Inode:  1,
	}

	if err := pipe.Run(context.Background(), lines); err == nil {
		t.Fatal("expected watermark persist failure")
	}
	if sink.recordCount() != 1 {
		t.Fatalf("recordCount = %d, want 1 buffered record flushed on error exit", sink.recordCount())
	}
}
