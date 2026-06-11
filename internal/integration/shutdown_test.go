// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package integration_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sanjuthomas/log-forwarder/internal/config"
)

type stallSink struct {
	once sync.Once
	gate chan struct{}
}

func newStallSink() *stallSink {
	return &stallSink{gate: make(chan struct{})}
}

func (s *stallSink) Publish(ctx context.Context, _ []byte) error {
	s.once.Do(func() { close(s.gate) })
	<-ctx.Done()
	return ctx.Err()
}

func (s *stallSink) Close() error { return nil }

func (s *stallSink) blocked() <-chan struct{} { return s.gate }

func TestE2E_GracefulShutdownWithFullBuffer(t *testing.T) {
	stall := newStallSink()
	logDir, sinkPath, statePath := setupDirs(t)
	logFile := filepath.Join(logDir, "app.log")

	lines := make([]string, 20)
	for i := range lines {
		lines[i] = fmt.Sprintf("2026-06-08T10:00:00Z\tINFO\tline-%d", i)
	}
	content := strings.Join(lines, "\n") + "\n"
	if err := os.WriteFile(logFile, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := tabLineConfig(logDir, sinkPath, statePath, "wrap", config.FilterConfig{})
	cfg.Pipeline.BufferSize = 2

	h := startForwarder(t, cfg, harnessOptions{sink: stall})

	select {
	case <-stall.blocked():
	case <-time.After(5 * time.Second):
		t.Fatal("pipeline did not block on publish")
	}

	for i := 0; i < 10; i++ {
		appendToFile(t, logFile, fmt.Sprintf("2026-06-08T10:00:01Z\tINFO\textra-%d\n", i))
	}
	time.Sleep(200 * time.Millisecond)

	h.cancelAndWait(t)
}
