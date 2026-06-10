package integration_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/sanjuthomas/log-forwarder/internal/config"
)

func TestE2E_DelimiterTransformFileAndHTTP(t *testing.T) {
	t.Run("file", func(t *testing.T) {
		logDir, sinkPath, statePath := setupDirs(t)
		logFile := filepath.Join(logDir, "app.log")
		if err := os.WriteFile(logFile, []byte("2024-01-01T00:00:00Z\tINFO\tdelimiter-file\n"), 0o644); err != nil {
			t.Fatal(err)
		}

		cfg := delimiterTabConfig(logDir, sinkPath, statePath)
		startForwarder(t, cfg, harnessOptions{})
		waitForRecordCount(t, sinkPath, 1)

		records := readJSONLRecords(t, sinkPath)
		assertDelimiterRecord(t, records[0], "delimiter-file")
	})

	t.Run("http", func(t *testing.T) {
		logDir, _, statePath := setupDirs(t)
		logFile := filepath.Join(logDir, "app.log")

		var mu sync.Mutex
		var bodies [][]byte
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				w.WriteHeader(http.StatusOK)
				return
			}
			body, _ := io.ReadAll(r.Body)
			if len(body) == 0 {
				w.WriteHeader(http.StatusOK)
				return
			}
			mu.Lock()
			bodies = append(bodies, body)
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		}))
		t.Cleanup(srv.Close)

		if err := os.WriteFile(logFile, []byte("2024-01-01T00:00:00Z\tINFO\tdelimiter-http\n"), 0o644); err != nil {
			t.Fatal(err)
		}

		cfg := delimiterTabConfig(logDir, "", statePath)
		cfg.Sink = config.SinkConfig{
			Type: "http-noauth",
			HTTPNoauth: &config.HTTPNoauthSinkConfig{
				URL:     srv.URL,
				Method:  "POST",
				Timeout: "5s",
			},
		}
		startForwarder(t, cfg, harnessOptions{})

		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			mu.Lock()
			n := len(bodies)
			mu.Unlock()
			if n >= 1 {
				break
			}
			time.Sleep(25 * time.Millisecond)
		}

		mu.Lock()
		defer mu.Unlock()
		if len(bodies) != 1 {
			t.Fatalf("received %d HTTP bodies, want 1", len(bodies))
		}
		var record map[string]any
		if err := json.Unmarshal(bodies[0], &record); err != nil {
			t.Fatalf("json.Unmarshal() error = %v", err)
		}
		assertDelimiterRecord(t, record, "delimiter-http")
	})
}

func delimiterTabConfig(logDir, sinkPath, statePath string) *config.Config {
	return &config.Config{
		Watch: config.WatchConfig{
			Poll: "50ms",
			Sources: []config.WatchSource{
				{Path: logDir, Patterns: []string{"*.log"}},
			},
			State: config.StateConfig{Path: statePath, FlushInterval: "0"},
		},
		Sink: config.SinkConfig{
			Type: "file",
			File: &config.FileSinkConfig{Path: sinkPath},
		},
		Parser: config.ParserConfig{Type: "line"},
		Transform: config.TransformConfig{
			Type:      "delimiter",
			Delimiter: "\t",
			Columns:   []string{"timestamp", "level", "message"},
			OnError:   "wrap",
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

func assertDelimiterRecord(t *testing.T, record map[string]any, wantMessage string) {
	t.Helper()
	if record["level"] != "INFO" {
		t.Fatalf("level = %v, want INFO", record["level"])
	}
	if record["message"] != wantMessage {
		t.Fatalf("message = %v, want %q", record["message"], wantMessage)
	}
	if record["timestamp"] != "2024-01-01T00:00:00Z" {
		t.Fatalf("timestamp = %v", record["timestamp"])
	}
	if _, ok := record["hostname"]; !ok {
		t.Fatal("expected hostname enricher field")
	}
}
