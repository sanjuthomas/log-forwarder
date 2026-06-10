package pipeline

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/sanjuthomas/log-forwarder/internal/config"
	"github.com/sanjuthomas/log-forwarder/internal/metrics"
	"github.com/sanjuthomas/log-forwarder/internal/watcher"
)

func TestPipelineNormalizesTimestampBeforePublish(t *testing.T) {
	collector, shutdown, err := metrics.New(config.MetricsConfig{
		Enabled: true,
		Host:    "127.0.0.1",
		Port:    0,
	}, metrics.Snapshot{}, nil)
	if err != nil {
		t.Fatalf("metrics.New() error = %v", err)
	}
	t.Cleanup(func() {
		if err := shutdown(context.Background()); err != nil {
			t.Fatalf("shutdown() error = %v", err)
		}
	})

	cfg := config.Default()
	cfg.Transform = config.TransformConfig{
		Type:    "delimiter",
		Columns: []string{"timestamp", "level", "message"},
		OnError: "wrap",
	}
	cfg.Timestamp = config.TimestampConfig{
		Field:  "timestamp",
		Format: "2006-01-02 15:04:05.000",
	}

	sink := &capturingSink{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	pipe, err := New(cfg, sink, logger, Options{Metrics: collector})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	lines := make(chan watcher.LineEvent, 1)
	lines <- watcher.LineEvent{
		Path:   "/tmp/test.log",
		Line:   []byte("2026-06-08 10:16:22.901\tINFO\thello"),
		Offset: 42,
	}
	close(lines)

	if err := pipe.Run(context.Background(), lines); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	var out map[string]any
	if err := json.Unmarshal(sink.payloads[0], &out); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	want := time.Date(2026, 6, 8, 10, 16, 22, 901000000, time.UTC).Format(time.RFC3339Nano)
	if out["timestamp"] != want {
		t.Fatalf("timestamp = %v, want %q", out["timestamp"], want)
	}
}
