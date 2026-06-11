// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package integration_test

import (
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/sanjuthomas/log-forwarder/internal/config"
)

func TestE2E_HibernateOnPublishBatchFailure(t *testing.T) {
	logDir, sinkPath, statePath := setupDirs(t)
	logFile := filepath.Join(logDir, "app.log")

	if err := os.WriteFile(logFile, []byte("2024-01-01T00:00:00Z\tINFO\thibernate-line\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := tabLineRegexConfig(logDir, sinkPath, statePath, "wrap")
	cfg.Pipeline.PublishBatch = config.PublishBatchConfig{
		MaxBytes:       1 << 20,
		FlushInterval:  "20ms",
		OnFlushFailure: config.OnFlushFailureHibernate,
		MaxAttempts:    2,
	}
	cfg.Pipeline.PublishRetry = config.PublishRetryConfig{
		InitialBackoff: "5ms",
		MaxBackoff:     "20ms",
		MaxAttempts:    2,
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	metricsPort := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()

	failSink := &alwaysFailSink{}
	h := startForwarder(t, cfg, harnessOptions{
		sink:           failSink,
		metricsEnabled: true,
		metricsPort:    metricsPort,
	})
	waitForPublishAttempts(t, failSink, 2)

	base := "http://127.0.0.1:" + strconv.Itoa(metricsPort)
	waitForReadyReason(t, base, "sink_hibernating")

	exists, err := watermarkEntryExists(statePath, logFile)
	if err != nil {
		t.Fatalf("watermarkEntryExists() error = %v", err)
	}
	if exists {
		t.Fatal("watermark must not advance when hibernating")
	}

	resp, err := http.Get(base + "/health")
	if err != nil {
		t.Fatalf("GET /health error = %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/health status = %d, want 200 while hibernating", resp.StatusCode)
	}

	h.stop(t)
}

func waitForReadyReason(t *testing.T, base, wantReason string) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(base + "/ready")
		if err != nil {
			time.Sleep(25 * time.Millisecond)
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode == http.StatusServiceUnavailable && strings.Contains(string(body), `"reason":"`+wantReason+`"`) {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for /ready reason %q", wantReason)
}
