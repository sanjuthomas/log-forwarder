package integration_test

import (
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/sanjuthomas/log-forwarder/internal/config"
	"github.com/sanjuthomas/log-forwarder/internal/deadletter"
)

func TestE2E_DeadLettersEndpointMetadata(t *testing.T) {
	logDir, sinkPath, statePath := setupDirs(t)
	dlqDir := filepath.Join(filepath.Dir(statePath), "dlq")
	logFile := filepath.Join(logDir, "app.log")

	if err := os.WriteFile(logFile, []byte("2024-01-01T00:00:00Z\tINFO\tdlq-api-line\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := tabLineRegexConfig(logDir, sinkPath, statePath, "wrap")
	cfg.Pipeline.PublishBatch = config.PublishBatchConfig{
		MaxBytes:       1 << 20,
		FlushInterval:  "20ms",
		OnFlushFailure: config.OnFlushFailureDeadLetter,
		MaxAttempts:    2,
		DeadLetter: config.DeadLetterConfig{
			Path:                  dlqDir,
			MaxConsecutiveBatches: 3,
		},
	}
	cfg.Pipeline.PublishRetry = config.PublishRetryConfig{
		InitialBackoff: "5ms",
		MaxBackoff:     "20ms",
		MaxAttempts:    2,
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	metricsPort := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()

	failSink := &alwaysFailSink{}
	startForwarder(t, cfg, harnessOptions{
		sink:           failSink,
		metricsEnabled: true,
		metricsPort:    metricsPort,
	})
	waitForPublishAttempts(t, failSink, 2)

	base := "http://127.0.0.1:" + strconv.Itoa(metricsPort)
	deadline := time.Now().Add(5 * time.Second)
	var body []byte
	for time.Now().Before(deadline) {
		resp, err := http.Get(base + "/deadletters")
		if err != nil {
			time.Sleep(25 * time.Millisecond)
			continue
		}
		body, _ = io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode == http.StatusOK && strings.Contains(string(body), `"event_count"`) {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if len(body) == 0 {
		t.Fatal("timed out waiting for GET /deadletters")
	}
	if strings.Contains(string(body), "dlq-api-line") {
		t.Fatalf("/deadletters must not return record bodies: %s", body)
	}

	var entries []deadletter.Entry
	if err := json.Unmarshal(body, &entries); err != nil {
		t.Fatalf("Unmarshal() error = %v, body = %s", err, body)
	}
	if len(entries) != 1 {
		t.Fatalf("len(entries) = %d, want 1", len(entries))
	}
	if entries[0].EventCount != 1 || entries[0].Bytes <= 0 {
		t.Fatalf("entry = %+v", entries[0])
	}
	if entries[0].FailureReason == "" {
		t.Fatal("expected failure_reason in /deadletters response")
	}
}
