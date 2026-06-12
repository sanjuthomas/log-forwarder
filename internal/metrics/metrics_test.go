// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package metrics

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sanjuthomas/log-forwarder/internal/config"
)

func TestNewDisabledCollector(t *testing.T) {
	t.Parallel()

	collector, shutdown, err := New(config.MetricsConfig{}, Snapshot{}, nil, nil)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if collector == nil {
		t.Fatal("expected non-nil collector")
	}
	collector.RecordLinePublished(context.Background())
	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown() error = %v", err)
	}
}

func TestNewEnabledCollector(t *testing.T) {
	collector, shutdown, err := New(config.MetricsConfig{
		Enabled: true,
		Host:    "127.0.0.1",
		Port:    0,
		Path:    "/metrics",
	}, Snapshot{
		FilesWatched:   func() int64 { return 2 },
		BufferDepth:    func() int64 { return 1 },
		BufferCapacity: 1024,
	}, nil, nil)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if collector.provider == nil {
		t.Fatal("expected meter provider when metrics are enabled")
	}
	if collector.server.Addr != "127.0.0.1:8080" {
		t.Fatalf("server.Addr = %q, want 127.0.0.1:8080", collector.server.Addr)
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown() error = %v", err)
	}
}

func TestHealthHandler(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	healthHandler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if body := rec.Body.String(); !strings.Contains(body, `"status":"UP"`) {
		t.Fatalf("body = %q, want UP status JSON", body)
	}
	var resp healthResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if resp.ProcessID != os.Getpid() {
		t.Fatalf("process_id = %d, want %d", resp.ProcessID, os.Getpid())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}
}

func TestMetricsServerExposesApplicationMetrics(t *testing.T) {
	collector, shutdown, err := New(config.MetricsConfig{
		Enabled: true,
		Host:    "127.0.0.1",
		Port:    9091,
		Path:    "/metrics",
	}, Snapshot{
		FilesWatched:   func() int64 { return 3 },
		BufferDepth:    func() int64 { return 2 },
		BufferCapacity: 512,
	}, nil, nil)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() {
		if err := shutdown(context.Background()); err != nil {
			t.Fatalf("shutdown() error = %v", err)
		}
	})

	collector.RecordLineRead(context.Background(), 4)
	collector.RecordLineReplayed(context.Background(), 2)
	collector.RecordLinePublished(context.Background())
	collector.RecordLineBufferDropped(context.Background())
	collector.RecordPublishFailure(context.Background())
	collector.RecordPublishRetry(context.Background())
	collector.RecordPublishDuration(context.Background(), 250000000) // 250ms
	collector.RecordWatermarkFlushError(context.Background())

	mux := http.NewServeMux()
	mux.HandleFunc("/health", healthHandler)
	mux.Handle("/metrics", promhttp.HandlerFor(collector.registry, promhttp.HandlerOpts{}))
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	for _, path := range []string{"/health", "/metrics"} {
		resp, err := http.Get(server.URL + path)
		if err != nil {
			t.Fatalf("GET %s error = %v", path, err)
		}
		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			t.Fatalf("read %s body error = %v", path, err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET %s status = %d, want %d", path, resp.StatusCode, http.StatusOK)
		}
		if path == "/metrics" {
			metricsBody := string(body)
			for _, want := range []string{
				"log_forwarder_lines_read",
				"log_forwarder_lines_replayed",
				"log_forwarder_lines_published",
				"log_forwarder_pipeline_buffer_dropped",
				"log_forwarder_publish_failures",
				"log_forwarder_publish_retries",
				"log_forwarder_publish_duration",
				"log_forwarder_watermark_flush_errors",
			} {
				if !strings.Contains(metricsBody, want) {
					t.Fatalf("metrics body missing %q", want)
				}
			}
		}
	}
}

func TestCollectorStartUsesConfiguredAddress(t *testing.T) {
	collector, shutdown, err := New(config.MetricsConfig{
		Enabled: true,
		Host:    "127.0.0.1",
		Port:    0,
		Path:    "/custom-metrics",
	}, Snapshot{}, nil, nil)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() {
		if err := shutdown(context.Background()); err != nil {
			t.Fatalf("shutdown() error = %v", err)
		}
	})

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := collector.Start(logger); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if collector.server == nil {
		t.Fatal("expected metrics server to be created")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := collector.server.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("server.Shutdown() error = %v", err)
	}
}

func TestCollectorRecordMethodsNoPanicWhenDisabled(t *testing.T) {
	t.Parallel()

	collector, shutdown, err := New(config.MetricsConfig{}, Snapshot{}, nil, nil)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() { _ = shutdown(context.Background()) })

	ctx := context.Background()
	collector.RecordLineSkipped(ctx)
	collector.RecordLineFiltered(ctx)
	collector.RecordTransformError(ctx)
	collector.RecordTimestampParseFailure(ctx)
	collector.RecordPublishTruncation(ctx)
	collector.RecordDeadLetterBatch(ctx, 1)
	collector.RecordPublishBatchFlush(ctx, "timer", "success", 3, 128)
}

func TestCollectorRecordMethodsWhenEnabled(t *testing.T) {
	collector, shutdown, err := New(config.MetricsConfig{
		Enabled: true,
		Host:    "127.0.0.1",
		Port:    9092,
		Path:    "/metrics",
	}, Snapshot{}, nil, nil)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() { _ = shutdown(context.Background()) })

	ctx := context.Background()
	collector.RecordLineSkipped(ctx)
	collector.RecordLineFiltered(ctx)
	collector.RecordLineReplayed(ctx, 1)
	collector.RecordTransformError(ctx)
	collector.RecordTimestampParseFailure(ctx)
	collector.RecordPublishTruncation(ctx)
	collector.RecordDeadLetterBatch(ctx, 2)
	collector.RecordPublishBatchFlush(ctx, "size", "success", 4, 256)

	handler := collector.PrometheusHandler()
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	body := rec.Body.String()
	for _, want := range []string{
		"log_forwarder_lines_skipped",
		"log_forwarder_lines_filtered",
		"log_forwarder_lines_replayed",
		"log_forwarder_transform_errors",
		"log_forwarder_timestamp_parse_failures",
		"log_forwarder_publish_truncations",
		"log_forwarder_publish_dead_letter_batches",
		"log_forwarder_publish_batch_flushes",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics body missing %q", want)
		}
	}
}

func TestPrometheusHandlerNilCollector(t *testing.T) {
	t.Parallel()

	var c *Collector
	handler := c.PrometheusHandler()
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
}

