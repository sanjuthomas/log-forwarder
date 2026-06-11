// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package integration_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sanjuthomas/log-forwarder/internal/config"
	"github.com/sanjuthomas/log-forwarder/internal/sink"
)

func TestE2E_PublishTimeoutFailsSlowPublish(t *testing.T) {
	logDir, sinkPath, statePath := setupDirs(t)
	logFile := filepath.Join(logDir, "app.log")

	if err := os.WriteFile(logFile, []byte("2024-01-01T00:00:00Z\tINFO\ttimeout-line\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := tabLineRegexConfig(logDir, sinkPath, statePath, "wrap")
	cfg.Pipeline.PublishTimeout = "1ms"
	cfg.Pipeline.PublishRetry = config.PublishRetryConfig{
		InitialBackoff: "1ms",
		MaxBackoff:     "5ms",
		MaxAttempts:    1,
	}
	cfg.Pipeline.PublishBatch = config.PublishBatchConfig{
		MaxBytes:      0,
		FlushInterval: "0",
	}

	inner, err := sink.New(cfg.Sink)
	if err != nil {
		t.Fatal(err)
	}

	startForwarder(t, cfg, harnessOptions{
		sink: &slowFileSink{inner: inner, delay: 50 * time.Millisecond},
	})

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		got, err := countJSONLRecords(sinkPath)
		if err != nil {
			t.Fatal(err)
		}
		if got == 0 {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("expected no published records when publish exceeds timeout")
}
