// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package integration_test

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
	"github.com/sanjuthomas/log-forwarder/internal/pipeline"
	"github.com/sanjuthomas/log-forwarder/internal/runner"
	"github.com/sanjuthomas/log-forwarder/internal/state"
	"github.com/sanjuthomas/log-forwarder/internal/watcher"
)

func TestE2E_RunnerErrorUnblocksWatcher(t *testing.T) {
	logDir, sinkPath, statePath := setupDirs(t)
	logFile := filepath.Join(logDir, "app.log")

	lines := []string{
		"2024-01-01T00:00:00Z\tINFO\tline-one\n",
		"2024-01-01T00:00:01Z\tINFO\tline-two\n",
		"2024-01-01T00:00:02Z\tINFO\tline-three\n",
	}
	if err := os.WriteFile(logFile, []byte(strings.Join(lines, "")), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := tabLineConfig(logDir, sinkPath, statePath, "wrap", config.FilterConfig{})
	cfg.Pipeline.BufferSize = 1
	cfg.Pipeline.OnFull = "block"
	cfg.Pipeline.PublishBatch.MaxBytes = 0
	cfg.Pipeline.PublishBatch.FlushInterval = "0"
	cfg.Pipeline.PublishRetry = config.PublishRetryConfig{
		InitialBackoff: "5ms",
		MaxBackoff:     "10ms",
		MaxAttempts:    2,
	}
	cfg.Watch.Poll = "10ms"

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	watermarks, err := state.NewStore(statePath)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	t.Cleanup(func() { _ = watermarks.Flush() })

	failSink := &alwaysFailSink{}
	pipe, err := pipeline.New(cfg, failSink, logger, pipeline.Options{Watermarks: watermarks})
	if err != nil {
		t.Fatalf("pipeline.New() error = %v", err)
	}

	linesCh := make(chan watcher.LineEvent, cfg.Pipeline.BufferSize)
	w := watcher.New(cfg, linesCh, watermarks, nil, logger)

	errCh := make(chan error, 2)
	runCtx, runCancel := context.WithCancel(context.Background())
	t.Cleanup(runCancel)

	go func() { errCh <- w.Run(runCtx) }()
	go func() { errCh <- pipe.Run(runCtx, linesCh) }()

	done := make(chan error, 1)
	go func() {
		done <- runner.Wait(errCh, runCancel)
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected pipeline error, got nil")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for runners after pipeline error; watcher likely blocked")
	}
}
