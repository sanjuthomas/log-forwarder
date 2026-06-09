package metrics

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sanjuthomas/log-forwarder/internal/config"
)

func TestNewDisabledCollector(t *testing.T) {
	t.Parallel()

	collector, shutdown, err := New(config.MetricsConfig{}, Snapshot{}, nil)
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
	}, nil)
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
	}, nil)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() {
		if err := shutdown(context.Background()); err != nil {
			t.Fatalf("shutdown() error = %v", err)
		}
	})

	collector.RecordLineRead(context.Background(), 4)
	collector.RecordLinePublished(context.Background())
	collector.RecordLineBufferDropped(context.Background())
	collector.RecordPublishFailure(context.Background())
	collector.RecordPublishRetry(context.Background())
	collector.RecordPublishDuration(context.Background(), 250000000) // 250ms

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
				"log_forwarder_lines_published",
				"log_forwarder_pipeline_buffer_dropped",
				"log_forwarder_publish_failures",
				"log_forwarder_publish_retries",
				"log_forwarder_publish_duration",
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
	}, Snapshot{}, nil)
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
