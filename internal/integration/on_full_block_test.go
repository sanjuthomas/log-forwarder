// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package integration_test

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/sanjuthomas/log-forwarder/internal/config"
)

func TestE2E_OnFullBlockBackpressureAndReadiness(t *testing.T) {
	stall := newStallSink()
	logDir, sinkPath, statePath := setupDirs(t)
	logFile := filepath.Join(logDir, "app.log")

	if err := os.WriteFile(logFile, []byte("2024-01-01T00:00:00Z\tINFO\tline-one\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := tabLineConfig(logDir, sinkPath, statePath, "wrap", config.FilterConfig{})
	cfg.Pipeline.BufferSize = 2
	cfg.Pipeline.OnFull = "block"
	cfg.Metrics.Enabled = true
	cfg.Metrics.Host = "127.0.0.1"
	cfg.Metrics.Readiness.BufferThreshold = 0.8

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	metricsPort := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()

	h := startForwarder(t, cfg, harnessOptions{sink: stall, metricsEnabled: true, metricsPort: metricsPort})

	select {
	case <-stall.blocked():
	case <-time.After(5 * time.Second):
		t.Fatal("pipeline did not block on publish")
	}

	for i := 0; i < 4; i++ {
		appendToFile(t, logFile, fmt.Sprintf("2024-01-01T00:00:01Z\tINFO\textra-%d\n", i))
	}

	base := "http://127.0.0.1:" + strconv.Itoa(metricsPort)
	waitForReadyReason(t, base, "pipeline_buffer_high")

	h.cancelAndWait(t)

	startForwarder(t, cfg, harnessOptions{})
	waitForRecordCount(t, sinkPath, 5)
}
