// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/sanjuthomas/log-forwarder/internal/config"
)

func TestE2E_MultipleWatchSourcesSingleProcess(t *testing.T) {
	root := t.TempDir()
	logDirA := filepath.Join(root, "billing")
	logDirB := filepath.Join(root, "auth")
	statePath := filepath.Join(root, "state", "watermarks.json")
	sinkPath := filepath.Join(root, "out", "records.jsonl")

	for _, dir := range []string{logDirA, logDirB} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}

	logFileA := filepath.Join(logDirA, "app.log")
	logFileB := filepath.Join(logDirB, "app.log")
	if err := os.WriteFile(logFileA, []byte("2024-01-01T00:00:00Z\tINFO\tbilling-line\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(logFileB, []byte("2024-01-01T00:00:01Z\tINFO\tauth-line\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := tabLineConfig("", sinkPath, statePath, "wrap", config.FilterConfig{})
	cfg.Watch.Sources = []config.WatchSource{
		{Path: logDirA, Patterns: []string{"*.log"}},
		{Path: logDirB, Patterns: []string{"*.log"}},
	}

	startForwarder(t, cfg, harnessOptions{})
	waitForSinkMessages(t, sinkPath, "billing-line", "auth-line")

	if offset := waitForWatermarkOffset(t, statePath, logFileA); offset == 0 {
		t.Fatal("expected watermark for billing source")
	}
	if offset := waitForWatermarkOffset(t, statePath, logFileB); offset == 0 {
		t.Fatal("expected watermark for auth source")
	}
}
