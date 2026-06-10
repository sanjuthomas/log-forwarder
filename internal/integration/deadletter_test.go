package integration_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sanjuthomas/log-forwarder/internal/config"
)

func countDLQJSONLFiles(dir string) int {
	files, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}
	n := 0
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".jsonl") && !strings.Contains(f.Name(), ".tmp") {
			n++
		}
	}
	return n
}

func TestE2E_DeadLetterOnPublishFailure(t *testing.T) {
	logDir, sinkPath, statePath := setupDirs(t)
	dlqDir := filepath.Join(filepath.Dir(statePath), "dlq")
	logFile := filepath.Join(logDir, "app.log")

	if err := os.WriteFile(logFile, []byte("2024-01-01T00:00:00Z\tINFO\tdlq-e2e\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := tabLineRegexConfig(logDir, sinkPath, statePath, "wrap")
	cfg.Pipeline.PublishBatch = config.PublishBatchConfig{
		MaxBytes:       1 << 20,
		FlushInterval:  "20ms",
		OnFlushFailure: config.OnFlushFailureDeadLetter,
		MaxAttempts:    2,
		DeadLetter: config.DeadLetterConfig{
			Path:                  dlqDir,
			MaxConsecutiveBatches: 3,
		},
	}
	cfg.Pipeline.PublishRetry = config.PublishRetryConfig{
		InitialBackoff: "5ms",
		MaxBackoff:     "20ms",
		MaxAttempts:    2,
	}

	failSink := &alwaysFailSink{}
	h := startForwarder(t, cfg, harnessOptions{sink: failSink})
	waitForPublishAttempts(t, failSink, 2)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if countDLQJSONLFiles(dlqDir) == 1 {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}

	if n := countDLQJSONLFiles(dlqDir); n != 1 {
		t.Fatalf("dead letter jsonl file count = %d, want 1", n)
	}

	if offset := readWatermarkOffset(t, statePath, logFile); offset == 0 {
		t.Fatal("expected watermark to advance after dead letter write")
	}

	h.stop(t)
}
