// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package watcher

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/sanjuthomas/log-forwarder/internal/config"
	"github.com/sanjuthomas/log-forwarder/internal/metrics"
	"github.com/sanjuthomas/log-forwarder/internal/state"
)

func newMetricsWatcher(t *testing.T, lines chan LineEvent, watermarks *state.Store) (*Watcher, *metrics.Collector) {
	t.Helper()

	collector, shutdown, err := metrics.New(config.MetricsConfig{
		Enabled: true,
		Host:    "127.0.0.1",
		Port:    0,
	}, metrics.Snapshot{}, nil, nil)
	if err != nil {
		t.Fatalf("metrics.New() error = %v", err)
	}
	t.Cleanup(func() { _ = shutdown(context.Background()) })

	cfg := config.Default()
	w := New(cfg, lines, watermarks, collector, slog.New(slog.NewTextHandler(io.Discard, nil)))
	return w, collector
}

func prometheusCounter(t *testing.T, collector *metrics.Collector, name string) float64 {
	t.Helper()

	server := httptest.NewServer(collector.PrometheusHandler())
	t.Cleanup(server.Close)

	resp, err := http.Get(server.URL)
	if err != nil {
		t.Fatalf("GET metrics error = %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		t.Fatalf("read metrics body error = %v", err)
	}

	for _, line := range strings.Split(string(body), "\n") {
		if strings.HasPrefix(line, "#") {
			continue
		}
		if !strings.HasPrefix(line, name) {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		v, err := strconv.ParseFloat(fields[len(fields)-1], 64)
		if err != nil {
			t.Fatalf("parse counter %q: %v", line, err)
		}
		return v
	}
	return 0
}

func TestTailFile_ResumeMetricsReplayedVsRead(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")
	content := "line-one\nline-two\n"
	if err := os.WriteFile(logPath, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	inode, err := fileInode(logPath)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("fully_watermarked_no_ingest", func(t *testing.T) {
		t.Parallel()

		store, err := state.NewStore(filepath.Join(t.TempDir(), "watermarks.json"))
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Set(logPath, int64(len(content)), inode); err != nil {
			t.Fatal(err)
		}

		w, collector := newMetricsWatcher(t, make(chan LineEvent), store)
		if err := w.tailFile(logPath); err != nil {
			t.Fatalf("tailFile() error = %v", err)
		}

		if got := prometheusCounter(t, collector, "log_forwarder_lines_read"); got != 0 {
			t.Fatalf("lines_read = %v, want 0", got)
		}
		if got := prometheusCounter(t, collector, "log_forwarder_lines_replayed"); got != 0 {
			t.Fatalf("lines_replayed = %v, want 0", got)
		}
	})

	t.Run("stale_watermark_replays_existing_tail", func(t *testing.T) {
		t.Parallel()

		store, err := state.NewStore(filepath.Join(t.TempDir(), "watermarks.json"))
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Set(logPath, int64(len("line-one\n")), inode); err != nil {
			t.Fatal(err)
		}

		lines := make(chan LineEvent, 2)
		w, collector := newMetricsWatcher(t, lines, store)
		if err := w.tailFile(logPath); err != nil {
			t.Fatalf("tailFile() error = %v", err)
		}

		if got := prometheusCounter(t, collector, "log_forwarder_lines_read"); got != 0 {
			t.Fatalf("lines_read = %v, want 0", got)
		}
		if got := prometheusCounter(t, collector, "log_forwarder_lines_replayed"); got != 1 {
			t.Fatalf("lines_replayed = %v, want 1", got)
		}
	})

	t.Run("fresh_file_counts_read", func(t *testing.T) {
		t.Parallel()

		lines := make(chan LineEvent, 4)
		w, collector := newMetricsWatcher(t, lines, nil)
		if err := w.tailFile(logPath); err != nil {
			t.Fatalf("tailFile() error = %v", err)
		}

		if got := prometheusCounter(t, collector, "log_forwarder_lines_read"); got != 2 {
			t.Fatalf("lines_read = %v, want 2", got)
		}
		if got := prometheusCounter(t, collector, "log_forwarder_lines_replayed"); got != 0 {
			t.Fatalf("lines_replayed = %v, want 0", got)
		}
	})
}

func TestFileStateIsReplayed(t *testing.T) {
	t.Parallel()

	state := &fileState{
		resumed:        true,
		resumeOffset:   9,
		fileSizeAtOpen: 18,
		offset:         18,
	}
	if !state.isReplayed() {
		t.Fatal("expected replayed line at end of stale tail")
	}

	state.offset = 25
	if state.isReplayed() {
		t.Fatal("expected appended line after open to be new, not replayed")
	}

	state.resumed = false
	state.offset = 18
	if state.isReplayed() {
		t.Fatal("expected non-resumed file to never classify as replayed")
	}
}
