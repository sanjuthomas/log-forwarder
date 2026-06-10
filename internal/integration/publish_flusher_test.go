package integration_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sanjuthomas/log-forwarder/internal/config"
	"github.com/sanjuthomas/log-forwarder/internal/sink"
)

type slowFileSink struct {
	inner sink.Sink
	delay time.Duration
}

func (s *slowFileSink) Check(ctx context.Context) error {
	if checker, ok := s.inner.(sink.Checker); ok {
		return checker.Check(ctx)
	}
	return nil
}

func (s *slowFileSink) Publish(ctx context.Context, payload []byte) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(s.delay):
	}
	return s.inner.Publish(ctx, payload)
}

func (s *slowFileSink) Close() error { return s.inner.Close() }

func (s *slowFileSink) PublishBatch(ctx context.Context, payloads [][]byte) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(s.delay):
	}
	if batcher, ok := s.inner.(sink.BatchSink); ok {
		return batcher.PublishBatch(ctx, payloads)
	}
	for _, payload := range payloads {
		if err := s.inner.Publish(ctx, payload); err != nil {
			return err
		}
	}
	return nil
}

func TestE2E_DoubleBufferBurstWithSlowSink(t *testing.T) {
	logDir, sinkPath, statePath := setupDirs(t)
	logFile := filepath.Join(logDir, "app.log")

	lines := make([]string, 12)
	for i := range lines {
		lines[i] = fmt.Sprintf("2026-06-08T10:00:0%dZ\tINFO\tburst-%d", i%10, i)
	}
	content := strings.Join(lines, "\n") + "\n"
	if err := os.WriteFile(logFile, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := tabLineConfig(logDir, sinkPath, statePath, "wrap", config.FilterConfig{})
	cfg.Pipeline.PublishBatch = config.PublishBatchConfig{
		MaxBytes:      300,
		FlushInterval: "50ms",
	}

	inner, err := sink.New(cfg.Sink)
	if err != nil {
		t.Fatalf("sink.New() error = %v", err)
	}
	startForwarder(t, cfg, harnessOptions{
		sink: &slowFileSink{inner: inner, delay: 100 * time.Millisecond},
	})
	waitForRecordCount(t, sinkPath, 12)

	records := readJSONLRecords(t, sinkPath)
	if len(records) != 12 {
		t.Fatalf("record count = %d, want 12", len(records))
	}
}
