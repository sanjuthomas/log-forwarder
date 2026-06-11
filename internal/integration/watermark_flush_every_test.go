// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package integration_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sanjuthomas/log-forwarder/internal/config"
)

func TestE2E_WatermarkFlushEveryPersistsOnCount(t *testing.T) {
	logDir, sinkPath, statePath := setupDirs(t)
	logFile := filepath.Join(logDir, "app.log")

	const lineCount = 5
	const line = "2024-01-01T00:00:00Z\tINFO\tflush-every\n"
	lines := make([]string, lineCount)
	for i := range lines {
		lines[i] = strings.TrimSuffix(line, "\n")
	}
	content := strings.Join(lines, "\n") + "\n"
	if err := os.WriteFile(logFile, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	expectedOffsetAfterThree := byteOffsetAfterLines(content, 3)

	cfg := tabLineConfig(logDir, sinkPath, statePath, "wrap", config.FilterConfig{})
	cfg.Watch.State.FlushInterval = "0"
	cfg.Watch.State.FlushEvery = 3

	h := startForwarder(t, cfg, harnessOptions{skipPeriodicFlush: true})
	waitForRecordCount(t, sinkPath, lineCount)
	h.crashStop(t)

	offset, ok := readPersistedWatermarkOffset(t, statePath, logFile)
	if !ok {
		t.Fatal("expected on-disk watermark after flush_every threshold")
	}
	if offset != expectedOffsetAfterThree {
		t.Fatalf("persisted watermark offset = %d, want %d (flush_every)", offset, expectedOffsetAfterThree)
	}

	startForwarder(t, cfg, harnessOptions{})
	// Lines 1–3 were persisted before crash; lines 4–5 replay on restart.
	waitForRecordCount(t, sinkPath, lineCount+2)
}

func byteOffsetAfterLines(content string, n int) int64 {
	offset := int64(0)
	remaining := content
	for i := 0; i < n; i++ {
		pos := strings.Index(remaining, "\n")
		if pos < 0 {
			break
		}
		offset += int64(pos + 1)
		remaining = remaining[pos+1:]
	}
	return offset
}
