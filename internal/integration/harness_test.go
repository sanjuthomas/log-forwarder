package integration_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sanjuthomas/log-forwarder/internal/config"
	"github.com/sanjuthomas/log-forwarder/internal/metrics"
	"github.com/sanjuthomas/log-forwarder/internal/pipeline"
	"github.com/sanjuthomas/log-forwarder/internal/sink"
	"github.com/sanjuthomas/log-forwarder/internal/state"
	"github.com/sanjuthomas/log-forwarder/internal/watcher"
)

const springBootRegex = `^(?s)(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3})\s+(?P<level>\S+)\s+(?P<pid>\d+)\s+---\s+\[\s*(?P<thread>[^\]]+?)\s*\]\s+(?P<logger>\S+)\s+:\s+(?P<message>.*)$`

type forwarderHarness struct {
	cancel   context.CancelFunc
	errCh    chan error
	sink     sink.Sink
	shutdown func(context.Context) error
}

type harnessOptions struct {
	sink           sink.Sink
	metricsEnabled bool
	metricsPort    int
}

func startForwarder(t *testing.T, cfg *config.Config, opts harnessOptions) *forwarderHarness {
	t.Helper()

	if cfg.Watch.Poll == "" {
		cfg.Watch.Poll = "50ms"
	} else {
		cfg.Watch.Poll = "50ms"
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	watermarks, err := state.NewStore(cfg.StatePath())
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}

	recordSink := opts.sink
	if recordSink == nil {
		recordSink, err = sink.New(cfg.Sink)
		if err != nil {
			t.Fatalf("sink.New() error = %v", err)
		}
	}

	if checker, ok := recordSink.(sink.Checker); ok {
		checkCtx, checkCancel := context.WithTimeout(context.Background(), 2*time.Second)
		if err := checker.Check(checkCtx); err != nil {
			checkCancel()
			t.Fatalf("sink check error = %v", err)
		}
		checkCancel()
	}

	if opts.metricsEnabled {
		cfg.Metrics.Enabled = true
		cfg.Metrics.Host = "127.0.0.1"
		if opts.metricsPort == 0 {
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatalf("listen metrics port: %v", err)
			}
			opts.metricsPort = ln.Addr().(*net.TCPAddr).Port
			_ = ln.Close()
		}
		cfg.Metrics.Port = opts.metricsPort
		if cfg.Metrics.Path == "" {
			cfg.Metrics.Path = "/metrics"
		}
	} else {
		cfg.Metrics.Enabled = false
	}

	lines := make(chan watcher.LineEvent, cfg.Pipeline.BufferSize)
	var w *watcher.Watcher

	collector, shutdownMetrics, err := metrics.New(cfg.Metrics, metrics.Snapshot{
		FilesWatched: func() int64 {
			if w == nil {
				return 0
			}
			return int64(w.FileCount())
		},
		BufferDepth:    func() int64 { return int64(len(lines)) },
		BufferCapacity: int64(cfg.Pipeline.BufferSize),
	})
	if err != nil {
		t.Fatalf("metrics.New() error = %v", err)
	}
	if err := collector.Start(logger); err != nil {
		t.Fatalf("collector.Start() error = %v", err)
	}

	pipe, err := pipeline.New(cfg, recordSink, logger, pipeline.Options{
		Watermarks: watermarks,
		Metrics:    collector,
	})
	if err != nil {
		t.Fatalf("pipeline.New() error = %v", err)
	}

	w = watcher.New(cfg, lines, watermarks, collector, logger)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 2)
	go func() { errCh <- w.Run(ctx) }()
	go func() { errCh <- pipe.Run(ctx, lines) }()

	h := &forwarderHarness{
		cancel:   cancel,
		errCh:    errCh,
		sink:     recordSink,
		shutdown: shutdownMetrics,
	}
	t.Cleanup(func() { h.stop(t) })
	return h
}

func (h *forwarderHarness) stop(t *testing.T) {
	t.Helper()
	if h.cancel == nil {
		return
	}
	h.cancel()
	h.cancel = nil

	deadline := time.After(3 * time.Second)
	for i := 0; i < 2; i++ {
		select {
		case <-h.errCh:
		case <-deadline:
			t.Log("timeout waiting for forwarder goroutines")
			return
		}
	}

	if h.sink != nil {
		_ = h.sink.Close()
	}
	if h.shutdown != nil {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = h.shutdown(shutdownCtx)
		shutdownCancel()
	}
}

func springBootConfig(logDir, sinkPath, statePath string) *config.Config {
	return &config.Config{
		Watch: config.WatchConfig{
			Poll: "50ms",
			Sources: []config.WatchSource{
				{Path: logDir, Patterns: []string{"*.log", "*.log.*"}},
			},
			State: config.StateConfig{Path: statePath},
		},
		Sink: config.SinkConfig{
			Type: "file",
			File: &config.FileSinkConfig{Path: sinkPath},
		},
		Parser: config.ParserConfig{
			Type:         "multiline",
			StartPattern: `^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}`,
		},
		Transform: config.TransformConfig{
			Type:    "regex",
			Pattern: springBootRegex,
			OnError: "wrap",
		},
		Enrichers: []config.EnricherConfig{
			{Type: "static", Fields: map[string]string{"application_id": "integration-test"}},
			{Type: "host"},
		},
		Pipeline: config.PipelineConfig{
			BufferSize: 64,
			OnFull:     "block",
			PublishRetry: config.PublishRetryConfig{
				InitialBackoff: "5ms",
				MaxBackoff:     "20ms",
				MaxAttempts:    0,
			},
		},
		Metrics: config.MetricsConfig{Enabled: false},
		Logging: config.LoggingConfig{Level: "error", Format: "text"},
	}
}

func tabLineRegexConfig(logDir, sinkPath, statePath, onError string) *config.Config {
	return &config.Config{
		Watch: config.WatchConfig{
			Poll: "50ms",
			Sources: []config.WatchSource{
				{Path: logDir, Patterns: []string{"*.log"}},
			},
			State: config.StateConfig{Path: statePath},
		},
		Sink: config.SinkConfig{
			Type: "file",
			File: &config.FileSinkConfig{Path: sinkPath},
		},
		Parser: config.ParserConfig{Type: "line"},
		Transform: config.TransformConfig{
			Type:    "regex",
			Pattern: `^(?P<timestamp>\S+)\t(?P<level>\S+)\t(?P<message>.+)$`,
			OnError: onError,
		},
		Enrichers: []config.EnricherConfig{{Type: "host"}},
		Pipeline: config.PipelineConfig{
			BufferSize: 64,
			OnFull:     "block",
		},
		Metrics: config.MetricsConfig{Enabled: false},
		Logging: config.LoggingConfig{Level: "error", Format: "text"},
	}
}

func appendToFile(t *testing.T, path, content string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("OpenFile(%q) error = %v", path, err)
	}
	if _, err := f.WriteString(content); err != nil {
		_ = f.Close()
		t.Fatalf("WriteString() error = %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func countJSONLRecords(path string) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	defer f.Close()

	count := 0
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		if scanner.Text() != "" {
			count++
		}
	}
	return count, scanner.Err()
}

func waitForRecordCount(t *testing.T, path string, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		got, err := countJSONLRecords(path)
		if err != nil {
			t.Fatalf("countJSONLRecords(%q) error = %v", path, err)
		}
		if got >= want {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	got, _ := countJSONLRecords(path)
	t.Fatalf("timeout waiting for %d records in %s, got %d", want, path, got)
}

// waitForRecordCountFlush stops the forwarder on timeout so the multiline parser
// flushes its trailing buffer (same as process shutdown in production).
func waitForRecordCountFlush(t *testing.T, h *forwarderHarness, path string, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		got, err := countJSONLRecords(path)
		if err != nil {
			t.Fatalf("countJSONLRecords(%q) error = %v", path, err)
		}
		if got >= want {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}

	h.stop(t)
	time.Sleep(50 * time.Millisecond)

	got, err := countJSONLRecords(path)
	if err != nil {
		t.Fatalf("countJSONLRecords(%q) error = %v", path, err)
	}
	if got >= want {
		return
	}
	t.Fatalf("timeout waiting for %d records in %s, got %d", want, path, got)
}

func readJSONLRecords(t *testing.T, path string) []map[string]any {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%q) error = %v", path, err)
	}
	var records []map[string]any
	scanner := bufio.NewScanner(bytes.NewReader(data))
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("json.Unmarshal() error = %v, line = %q", err, line)
		}
		records = append(records, record)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scanner error = %v", err)
	}
	return records
}

func setupDirs(t *testing.T) (logDir, sinkPath, statePath string) {
	t.Helper()
	root := t.TempDir()
	logDir = filepath.Join(root, "logs")
	statePath = filepath.Join(root, "state", "watermarks.json")
	sinkPath = filepath.Join(root, "out", "records.jsonl")
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	return logDir, sinkPath, statePath
}
