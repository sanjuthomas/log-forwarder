package metrics

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sanjuthomas/log-forwarder/internal/config"
)

func TestReadinessReady(t *testing.T) {
	t.Parallel()

	r := &Readiness{
		Snapshot: Snapshot{
			FilesWatched:   func() int64 { return 1 },
			BufferDepth:    func() int64 { return 1 },
			BufferCapacity: 10,
		},
		CheckSink:        func(context.Context) error { return nil },
		BufferThreshold:  0.8,
		SinkCheckEnabled: true,
	}

	rec := httptest.NewRecorder()
	r.handler()(rec, httptest.NewRequest(http.MethodGet, "/ready", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if body := rec.Body.String(); !strings.Contains(body, `"status":"READY"`) {
		t.Fatalf("body = %q", body)
	}
}

func TestReadinessSinkUnreachable(t *testing.T) {
	t.Parallel()

	r := &Readiness{
		Snapshot: Snapshot{BufferCapacity: 10, BufferDepth: func() int64 { return 0 }},
		CheckSink: func(context.Context) error {
			return errors.New("connection refused")
		},
		SinkCheckEnabled: true,
	}

	rec := httptest.NewRecorder()
	r.handler()(rec, httptest.NewRequest(http.MethodGet, "/ready", nil))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
	if body := rec.Body.String(); !strings.Contains(body, `"reason":"sink_unreachable"`) {
		t.Fatalf("body = %q", body)
	}
}

func TestReadinessPipelineBufferHigh(t *testing.T) {
	t.Parallel()

	r := &Readiness{
		Snapshot: Snapshot{
			BufferDepth:    func() int64 { return 9 },
			BufferCapacity: 10,
		},
		BufferThreshold:  0.8,
		SinkCheckEnabled: false,
	}

	rec := httptest.NewRecorder()
	r.handler()(rec, httptest.NewRequest(http.MethodGet, "/ready", nil))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
	if body := rec.Body.String(); !strings.Contains(body, `"reason":"pipeline_buffer_high"`) {
		t.Fatalf("body = %q", body)
	}
}

func TestReadinessSinkHibernating(t *testing.T) {
	t.Parallel()

	r := &Readiness{
		Snapshot: Snapshot{
			BufferCapacity: 10,
			BufferDepth:    func() int64 { return 0 },
		},
		IsHibernating:    func() bool { return true },
		SinkCheckEnabled: false,
	}

	rec := httptest.NewRecorder()
	r.handler()(rec, httptest.NewRequest(http.MethodGet, "/ready", nil))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
	if body := rec.Body.String(); !strings.Contains(body, `"reason":"sink_hibernating"`) {
		t.Fatalf("body = %q", body)
	}
}

func TestReadinessNoFilesWatched(t *testing.T) {
	t.Parallel()

	r := &Readiness{
		Snapshot: Snapshot{
			FilesWatched:   func() int64 { return 0 },
			BufferCapacity: 10,
			BufferDepth:    func() int64 { return 0 },
		},
		RequireFiles:     true,
		SinkCheckEnabled: false,
	}

	rec := httptest.NewRecorder()
	r.handler()(rec, httptest.NewRequest(http.MethodGet, "/ready", nil))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
	if body := rec.Body.String(); !strings.Contains(body, `"reason":"no_files_watched"`) {
		t.Fatalf("body = %q", body)
	}
}

func TestNewRegistersReadyHandler(t *testing.T) {
	readiness := &Readiness{
		Snapshot: Snapshot{
			BufferCapacity: 10,
			BufferDepth:    func() int64 { return 0 },
		},
		SinkCheckEnabled: false,
	}

	collector, shutdown, err := New(configFromTest(), Snapshot{
		BufferCapacity: 10,
		BufferDepth:    func() int64 { return 0 },
	}, readiness)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() {
		_ = shutdown(context.Background())
	})

	server := httptest.NewServer(collector.server.Handler)
	t.Cleanup(server.Close)

	resp, err := http.Get(server.URL + "/ready")
	if err != nil {
		t.Fatalf("GET /ready error = %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /ready status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
}

func configFromTest() config.MetricsConfig {
	return config.MetricsConfig{
		Enabled: true,
		Host:    "127.0.0.1",
		Port:    0,
		Path:    "/metrics",
	}
}
