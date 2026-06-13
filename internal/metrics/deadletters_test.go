// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package metrics

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/sanjuthomas/log-forwarder/internal/config"
	"github.com/sanjuthomas/log-forwarder/internal/deadletter"
)

func TestDeadLettersHandlerReturnsMetadataOnly(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	_, _, err := deadletter.WriteBatch(dir, [][]byte{[]byte(`{"message":"do-not-expose"}`)}, deadletter.WriteInfo{
		FailureReason: "sink error",
		SinkType:      "kafka",
		BatchAttempts: 3,
	})
	if err != nil {
		t.Fatalf("WriteBatch() error = %v", err)
	}

	collector, shutdown, err := New(configFromTest(), Snapshot{}, nil, &DeadLetters{Dir: dir})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() { _ = shutdown(context.Background()) })

	server := httptest.NewServer(collector.server.Handler)
	t.Cleanup(server.Close)

	resp, err := http.Get(server.URL + "/deadletters")
	if err != nil {
		t.Fatalf("GET /deadletters error = %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(body), "do-not-expose") {
		t.Fatalf("response must not include record bodies: %s", body)
	}

	var entries []deadletter.Entry
	if err := json.Unmarshal(body, &entries); err != nil {
		t.Fatalf("Unmarshal() error = %v, body = %s", err, body)
	}
	if len(entries) != 1 {
		t.Fatalf("len(entries) = %d, want 1", len(entries))
	}
	if entries[0].SinkType != "kafka" || entries[0].EventCount != 1 {
		t.Fatalf("entry = %+v", entries[0])
	}
}

func TestDeadLettersHandlerMethodNotAllowed(t *testing.T) {
	t.Parallel()

	dl := &DeadLetters{Dir: t.TempDir()}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/deadletters", nil)

	dl.handler()(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
}

func TestDeadLettersEnabled(t *testing.T) {
	t.Parallel()

	if (&DeadLetters{}).enabled() {
		t.Fatal("expected disabled when dir is empty")
	}
	if !(&DeadLetters{Dir: t.TempDir()}).enabled() {
		t.Fatal("expected enabled when dir is set")
	}
}

func TestMetricsExposeSnapshotAndProcessGauges(t *testing.T) {
	collector, shutdown, err := New(config.MetricsConfig{
		Enabled: true,
		Host:    "127.0.0.1",
		Port:    9093,
		Path:    "/metrics",
	}, Snapshot{
		FilesWatched:                 func() int64 { return 4 },
		BufferDepth:                  func() int64 { return 2 },
		BufferCapacity:               128,
		PublishBufferActiveBytes:     func() int64 { return 64 },
		PublishHibernating:           func() int64 { return 1 },
		PublishConsecutiveDLQBatches: func() int64 { return 3 },
	}, nil, nil)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() { _ = shutdown(context.Background()) })

	ctx := context.Background()
	collector.RecordLineRead(ctx, 5)
	collector.RecordLineBufferDropped(ctx)
	collector.RecordPublishFailure(ctx)
	collector.RecordPublishRetry(ctx)
	collector.RecordWatermarkFlushError(ctx)

	rec := httptest.NewRecorder()
	collector.PrometheusHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	body := rec.Body.String()
	for _, want := range []string{
		"log_forwarder_lines_read",
		"log_forwarder_pipeline_buffer_dropped",
		"log_forwarder_publish_failures",
		"log_forwarder_publish_retries",
		"log_forwarder_watermark_flush_errors",
		"log_forwarder_publish_buffer_active_bytes",
		"log_forwarder_publish_hibernating",
		"log_forwarder_publish_consecutive_dlq_batches",
		"process_memory_usage",
		"process_cpu_utilization",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics body missing %q", want)
		}
	}
}

func TestDeadLettersHandlerListEntriesError(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	if err := os.Chmod(dir, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })

	dl := &DeadLetters{Dir: dir}
	rec := httptest.NewRecorder()
	dl.handler()(rec, httptest.NewRequest(http.MethodGet, "/deadletters", nil))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusInternalServerError)
	}
}

func TestDeadLettersHandlerEmptyDirectory(t *testing.T) {
	t.Parallel()

	dl := &DeadLetters{Dir: t.TempDir()}
	rec := httptest.NewRecorder()
	dl.handler()(rec, httptest.NewRequest(http.MethodGet, "/deadletters", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if body := rec.Body.String(); body != "[]\n" && body != "[]" {
		t.Fatalf("body = %q, want empty JSON array", body)
	}
}

func TestDeadLettersHandlerNotRegisteredWhenDisabled(t *testing.T) {
	t.Parallel()

	collector, shutdown, err := New(configFromTest(), Snapshot{}, nil, nil)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() { _ = shutdown(context.Background()) })

	server := httptest.NewServer(collector.server.Handler)
	t.Cleanup(server.Close)

	resp, err := http.Get(server.URL + "/deadletters")
	if err != nil {
		t.Fatalf("GET /deadletters error = %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusNotFound)
	}
}
