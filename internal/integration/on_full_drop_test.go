// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package integration_test

import (
	"fmt"
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

func TestE2E_OnFullDropIncrementsMetricAndSkipsWatermark(t *testing.T) {
	stall := newStallSink()
	logDir, sinkPath, statePath := setupDirs(t)
	logFile := filepath.Join(logDir, "app.log")

	if err := os.WriteFile(logFile, []byte("2024-01-01T00:00:00Z\tINFO\tline-keep\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := tabLineConfig(logDir, sinkPath, statePath, "wrap", config.FilterConfig{})
	cfg.Pipeline.BufferSize = 1
	cfg.Pipeline.OnFull = "drop"

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

	for i := 0; i < 3; i++ {
		appendToFile(t, logFile, fmt.Sprintf("2024-01-01T00:00:01Z\tINFO\tdrop-%d\n", i))
	}

	base := "http://127.0.0.1:" + strconv.Itoa(metricsPort)
	waitForMetricSubstring(t, base, "log_forwarder_pipeline_buffer_dropped")

	h.cancelAndWait(t)

	startForwarder(t, cfg, harnessOptions{})
	waitForRecordCount(t, sinkPath, 1)

	records := readJSONLRecords(t, sinkPath)
	if records[0]["message"] != "line-keep" {
		t.Fatalf("message = %v, want line-keep (dropped lines must not publish)", records[0]["message"])
	}

	offset := waitForWatermarkOffset(t, statePath, logFile)
	if offset == 0 {
		t.Fatal("expected watermark to advance past line-keep only")
	}
}

func waitForMetricSubstring(t *testing.T, base, want string) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(base + "/metrics")
		if err != nil {
			time.Sleep(25 * time.Millisecond)
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if strings.Contains(string(body), want) {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for metric substring %q", want)
}
