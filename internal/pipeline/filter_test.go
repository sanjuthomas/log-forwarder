package pipeline

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sanjuthomas/log-forwarder/internal/config"
	"github.com/sanjuthomas/log-forwarder/internal/metrics"
	"github.com/sanjuthomas/log-forwarder/internal/state"
	"github.com/sanjuthomas/log-forwarder/internal/watcher"
)

func TestPipelineFilterPassesOnlyMatchingLevels(t *testing.T) {
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
	cfg.Filter = config.FilterConfig{
		Match: "all",
		Rules: []config.FilterRuleConfig{
			{
				Type:       "field",
				Field:      "level",
				Op:         "in",
				Values:     []string{"ERROR"},
				IgnoreCase: true,
			},
		},
	}

	sink := &fakeSink{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	pipe, err := New(cfg, sink, logger, Options{Metrics: collector})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	lines := make(chan watcher.LineEvent, 2)
	lines <- watcher.LineEvent{
		Path:   "/tmp/test.log",
		Line:   []byte("2024-01-01T00:00:00Z\tINFO\tignored"),
		Offset: 10,
		Inode:  1,
	}
	lines <- watcher.LineEvent{
		Path:   "/tmp/test.log",
		Line:   []byte("2024-01-01T00:00:01Z\terror\tboom"),
		Offset: 42,
		Inode:  1,
	}
	close(lines)

	if err := pipe.Run(context.Background(), lines); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if sink.publishCalls != 1 {
		t.Fatalf("publishCalls = %d, want 1", sink.publishCalls)
	}

	body := prometheusBody(t, collector)
	if !strings.Contains(body, "log_forwarder_lines_filtered") {
		t.Fatalf("metrics missing filtered counter: %s", body)
	}
}

func TestPipelineFilterAdvancesWatermarkWhenFiltered(t *testing.T) {
	cfg := config.Default()
	cfg.Transform = config.TransformConfig{
		Type:    "delimiter",
		Columns: []string{"timestamp", "level", "message"},
		OnError: "wrap",
	}
	cfg.Filter = config.FilterConfig{
		Match: "all",
		Rules: []config.FilterRuleConfig{
			{
				Type:       "field",
				Field:      "level",
				Op:         "in",
				Values:     []string{"ERROR"},
				IgnoreCase: true,
			},
		},
	}

	watermarks, err := state.NewStore(filepath.Join(t.TempDir(), "watermarks.json"))
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}

	sink := &fakeSink{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	pipe, err := New(cfg, sink, logger, Options{Watermarks: watermarks})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	const path = "/tmp/test.log"
	lines := make(chan watcher.LineEvent, 1)
	lines <- watcher.LineEvent{
		Path:   path,
		Line:   []byte("2024-01-01T00:00:00Z\tINFO\tignored"),
		Offset: 99,
		Inode:  7,
	}
	close(lines)

	if err := pipe.Run(context.Background(), lines); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if sink.publishCalls != 0 {
		t.Fatalf("publishCalls = %d, want 0", sink.publishCalls)
	}

	entry, ok := watermarks.Get(path)
	if !ok {
		t.Fatal("expected watermark entry for filtered line")
	}
	if entry.Offset != 99 || entry.Inode != 7 {
		t.Fatalf("watermark = %+v, want offset 99 inode 7", entry)
	}
}
