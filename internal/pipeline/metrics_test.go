package pipeline

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sanjuthomas/log-forwarder/internal/config"
	"github.com/sanjuthomas/log-forwarder/internal/metrics"
	"github.com/sanjuthomas/log-forwarder/internal/watcher"
)

type fakeSink struct {
	publishCalls int
}

func (f *fakeSink) Publish(_ context.Context, _ []byte) error {
	f.publishCalls++
	return nil
}

func (f *fakeSink) Close() error { return nil }

func prometheusBody(t *testing.T, collector *metrics.Collector) string {
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
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("metrics status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	return string(body)
}

func TestPipelineRecordsPublishMetrics(t *testing.T) {
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

	sink := &fakeSink{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	pipe, err := New(cfg, sink, logger, Options{Metrics: collector})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	lines := make(chan watcher.LineEvent, 1)
	lines <- watcher.LineEvent{
		Path:   "/tmp/test.log",
		Line:   []byte("2024-01-01T00:00:00Z\tINFO\thello"),
		Offset: 42,
	}
	close(lines)

	if err := pipe.Run(context.Background(), lines); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if sink.publishCalls != 1 {
		t.Fatalf("publishCalls = %d, want 1", sink.publishCalls)
	}

	body := prometheusBody(t, collector)
	if !strings.Contains(body, "log_forwarder_lines_published") {
		t.Fatalf("metrics missing published counter: %s", body)
	}
	if !strings.Contains(body, "log_forwarder_kafka_publish_duration") {
		t.Fatalf("metrics missing publish duration histogram: %s", body)
	}
}

func TestPipelineRecordsSkippedLineMetrics(t *testing.T) {
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
		Type:    "regex",
		Pattern: `^(?P<timestamp>\d{4}-\d{2}-\d{2}T\S+)\s+(?P<level>\S+)\s+(?P<message>.*)$`,
		OnError: "skip",
	}

	sink := &fakeSink{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	pipe, err := New(cfg, sink, logger, Options{Metrics: collector})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	lines := make(chan watcher.LineEvent, 1)
	lines <- watcher.LineEvent{
		Path: "/tmp/test.log",
		Line: []byte("this line does not match the regex"),
	}
	close(lines)

	if err := pipe.Run(context.Background(), lines); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if sink.publishCalls != 0 {
		t.Fatalf("publishCalls = %d, want 0", sink.publishCalls)
	}

	body := prometheusBody(t, collector)
	if !strings.Contains(body, "log_forwarder_lines_skipped") {
		t.Fatalf("metrics missing skipped counter: %s", body)
	}
	if !strings.Contains(body, "log_forwarder_transform_errors") {
		t.Fatalf("metrics missing transform error counter: %s", body)
	}
}

func TestPipelineMultilineParserPublishesOneRecord(t *testing.T) {
	cfg := config.Default()
	cfg.Parser = config.ParserConfig{
		Type:         "multiline",
		StartPattern: `^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}`,
	}
	cfg.Transform = config.TransformConfig{
		Type:    "regex",
		Pattern: `^(?s)(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3})\s+(?P<level>\S+)\s+(?P<pid>\d+)\s+---\s+\[\s*(?P<thread>[^\]]+?)\s*\]\s+(?P<logger>\S+)\s+:\s+(?P<message>.*)$`,
		OnError: "wrap",
	}

	sink := &fakeSink{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	pipe, err := New(cfg, sink, logger, Options{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	lines := make(chan watcher.LineEvent, 4)
	lines <- watcher.LineEvent{
		Path:   "/tmp/test.log",
		Line:   []byte("2026-06-08 10:16:22.901  ERROR 18432 --- [main] c.a.b.PaymentController : Payment failed"),
		Offset: 100,
		Inode:  1,
	}
	lines <- watcher.LineEvent{
		Path:   "/tmp/test.log",
		Line:   []byte("org.springframework.dao.DataIntegrityViolationException: could not execute statement"),
		Offset: 180,
		Inode:  1,
	}
	lines <- watcher.LineEvent{
		Path:   "/tmp/test.log",
		Line:   []byte("        at com.acme.billing.controller.PaymentController.processPayment(PaymentController.java:87)"),
		Offset: 260,
		Inode:  1,
	}
	close(lines)

	if err := pipe.Run(context.Background(), lines); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if sink.publishCalls != 1 {
		t.Fatalf("publishCalls = %d, want 1", sink.publishCalls)
	}
}
