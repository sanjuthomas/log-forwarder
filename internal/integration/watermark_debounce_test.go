// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package integration_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestE2E_DebouncedWatermarkReplayAfterCrash(t *testing.T) {
	logDir, sinkPath, statePath := setupDirs(t)
	logFile := filepath.Join(logDir, "app.log")

	const lineCount = 5
	lines := make([]string, lineCount)
	for i := range lines {
		lines[i] = fmt.Sprintf("2024-01-01T00:00:0%dZ\tINFO\tdebounce-line-%d", i, i+1)
	}
	if err := os.WriteFile(logFile, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := debouncedTabLineConfig(logDir, sinkPath, statePath, "100ms")
	h1 := startForwarder(t, cfg, harnessOptions{skipPeriodicFlush: true})
	waitForRecordCount(t, sinkPath, lineCount)
	h1.crashStop(t)

	if _, ok := readPersistedWatermarkOffset(t, statePath, logFile); ok {
		t.Fatal("expected no on-disk watermark after crash before flush")
	}

	startForwarder(t, cfg, harnessOptions{})
	waitForRecordCount(t, sinkPath, lineCount*2)

	records := readJSONLRecords(t, sinkPath)
	if len(records) != lineCount*2 {
		t.Fatalf("len(records) = %d, want %d (replay within debounce window)", len(records), lineCount*2)
	}
	for i := 1; i <= lineCount; i++ {
		msg := fmt.Sprintf("debounce-line-%d", i)
		if got := countSinkMessages(records, msg); got != 2 {
			t.Fatalf("message %q count = %d, want 2 after restart replay", msg, got)
		}
	}
}

func TestE2E_DebouncedWatermarkGracefulStopNoReplay(t *testing.T) {
	logDir, sinkPath, statePath := setupDirs(t)
	logFile := filepath.Join(logDir, "app.log")

	const initialLines = 5
	lines := make([]string, initialLines)
	for i := range lines {
		lines[i] = fmt.Sprintf("2024-01-01T00:00:0%dZ\tINFO\tgraceful-line-%d", i, i+1)
	}
	if err := os.WriteFile(logFile, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := debouncedTabLineConfig(logDir, sinkPath, statePath, "100ms")
	h1 := startForwarder(t, cfg, harnessOptions{skipPeriodicFlush: true})
	waitForRecordCount(t, sinkPath, initialLines)
	h1.stop(t)

	offset, ok := readPersistedWatermarkOffset(t, statePath, logFile)
	if !ok {
		t.Fatal("expected on-disk watermark after graceful stop flush")
	}
	if offset <= 0 {
		t.Fatalf("persisted watermark offset = %d, want > 0", offset)
	}

	appendToFile(t, logFile, "2024-01-01T00:00:05Z\tINFO\tgraceful-line-6\n")
	appendToFile(t, logFile, "2024-01-01T00:00:06Z\tINFO\tgraceful-line-7\n")

	startForwarder(t, cfg, harnessOptions{})
	waitForRecordCount(t, sinkPath, initialLines+2)

	records := readJSONLRecords(t, sinkPath)
	if len(records) != initialLines+2 {
		t.Fatalf("len(records) = %d, want %d", len(records), initialLines+2)
	}
	for i := 1; i <= initialLines+2; i++ {
		msg := fmt.Sprintf("graceful-line-%d", i)
		if got := countSinkMessages(records, msg); got != 1 {
			t.Fatalf("message %q count = %d, want 1 (no replay after graceful flush)", msg, got)
		}
	}
}
