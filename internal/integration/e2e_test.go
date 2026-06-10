package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sanjuthomas/log-forwarder/internal/config"
	"github.com/sanjuthomas/log-forwarder/internal/sink"
)

func TestE2E_SpringBootSingleLineToFileSink(t *testing.T) {
	logDir, sinkPath, statePath := setupDirs(t)
	logFile := filepath.Join(logDir, "app.log")

	content := strings.Join([]string{
		"2026-06-08 10:16:22.901  INFO 18432 --- [main] c.example.App : hello integration",
		"2026-06-08 10:16:23.901  INFO 18432 --- [main] c.example.App : flush-sentinel",
	}, "\n") + "\n"
	if err := os.WriteFile(logFile, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := springBootConfig(logDir, sinkPath, statePath)
	startForwarder(t, cfg, harnessOptions{})
	waitForRecordCount(t, sinkPath, 1)

	records := readJSONLRecords(t, sinkPath)
	if records[0]["level"] != "INFO" {
		t.Fatalf("level = %v, want INFO", records[0]["level"])
	}
	if records[0]["message"] != "hello integration" {
		t.Fatalf("message = %v", records[0]["message"])
	}
	if records[0]["application_id"] != "integration-test" {
		t.Fatalf("application_id = %v", records[0]["application_id"])
	}
	if _, ok := records[0]["hostname"]; !ok {
		t.Fatal("expected hostname enricher field")
	}
}

func TestE2E_SpringBootMultilineStackTrace(t *testing.T) {
	logDir, sinkPath, statePath := setupDirs(t)
	logFile := filepath.Join(logDir, "app.log")

	content := strings.Join([]string{
		"2026-06-08 10:16:22.901  ERROR 18432 --- [nio-8080-exec-5] c.example.PaymentController : Payment failed",
		"org.springframework.dao.DataIntegrityViolationException: could not execute statement",
		"        at org.springframework.orm.jpa.vendor.HibernateJpaDialect.convertHibernateAccessException(HibernateJpaDialect.java:290)",
		"Caused by: org.postgresql.util.PSQLException: ERROR: constraint",
		"2026-06-08 10:16:23.901  INFO 18432 --- [main] c.example.App : flush-sentinel",
	}, "\n") + "\n"
	if err := os.WriteFile(logFile, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := springBootConfig(logDir, sinkPath, statePath)
	startForwarder(t, cfg, harnessOptions{})
	waitForRecordCount(t, sinkPath, 1)

	records := readJSONLRecords(t, sinkPath)
	msg, ok := records[0]["message"].(string)
	if !ok {
		t.Fatalf("message type = %T", records[0]["message"])
	}
	if !strings.Contains(msg, "Payment failed") {
		t.Fatalf("message missing header: %q", msg)
	}
	if !strings.Contains(msg, "PSQLException") {
		t.Fatalf("message missing stack trace: %q", msg)
	}
	if strings.Count(msg, "\n") < 3 {
		t.Fatalf("expected multiline message, got %q", msg)
	}
}

func TestE2E_WatermarkResumeAfterRestart(t *testing.T) {
	logDir, sinkPath, statePath := setupDirs(t)
	logFile := filepath.Join(logDir, "app.log")

	lines := []string{
		"2026-06-08 10:00:00.000  INFO 1 --- [main] c.example.App : line-one",
		"2026-06-08 10:00:01.000  INFO 1 --- [main] c.example.App : line-two",
		"2026-06-08 10:00:02.000  INFO 1 --- [main] c.example.App : line-three",
	}
	if err := os.WriteFile(logFile, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := springBootConfig(logDir, sinkPath, statePath)
	cfg.Parser = config.ParserConfig{Type: "line"}
	h1 := startForwarder(t, cfg, harnessOptions{})
	waitForRecordCount(t, sinkPath, 3)
	h1.stop(t)

	appendToFile(t, logFile, "2026-06-08 10:00:03.000  INFO 1 --- [main] c.example.App : line-four\n")
	appendToFile(t, logFile, "2026-06-08 10:00:04.000  INFO 1 --- [main] c.example.App : line-five\n")

	startForwarder(t, cfg, harnessOptions{})
	waitForRecordCount(t, sinkPath, 5)

	records := readJSONLRecords(t, sinkPath)
	if len(records) != 5 {
		t.Fatalf("len(records) = %d, want 5", len(records))
	}
	last := records[4]["message"]
	if last != "line-five" {
		t.Fatalf("last message = %v, want line-five", last)
	}
}

func TestE2E_LogRotation(t *testing.T) {
	logDir, sinkPath, statePath := setupDirs(t)
	logFile := filepath.Join(logDir, "app.log")

	before := "2026-06-08 10:00:00.000  INFO 1 --- [main] c.example.App : before-rotate\n"
	if err := os.WriteFile(logFile, []byte(before), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := springBootConfig(logDir, sinkPath, statePath)
	cfg.Parser = config.ParserConfig{Type: "line"}
	// Only tail the active log file; *.log.* would also match app.log.1 after rotation
	// and re-publish archived content as a false duplicate.
	cfg.Watch.Sources[0].Patterns = []string{"*.log"}
	startForwarder(t, cfg, harnessOptions{})
	waitForRecordCount(t, sinkPath, 1)

	if err := os.Rename(logFile, logFile+".1"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(logFile, []byte("2026-06-08 10:01:00.000  INFO 1 --- [main] c.example.App : after-rotate\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	waitForSinkMessages(t, sinkPath, "before-rotate", "after-rotate")

	records := readJSONLRecords(t, sinkPath)
	messages := sinkMessages(records)
	if !containsAll(messages, "before-rotate", "after-rotate") {
		t.Fatalf("messages = %v", messages)
	}
}

func TestE2E_TransformOnErrorSkip(t *testing.T) {
	logDir, sinkPath, statePath := setupDirs(t)
	logFile := filepath.Join(logDir, "app.log")

	content := "not-a-valid-line\n2024-01-01T00:00:00Z\tINFO\tgood-line\n"
	if err := os.WriteFile(logFile, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := tabLineRegexConfig(logDir, sinkPath, statePath, "skip")
	startForwarder(t, cfg, harnessOptions{})
	waitForRecordCount(t, sinkPath, 1)

	records := readJSONLRecords(t, sinkPath)
	if len(records) != 1 {
		t.Fatalf("len(records) = %d, want 1", len(records))
	}
	if records[0]["message"] != "good-line" {
		t.Fatalf("message = %v", records[0]["message"])
	}
}

func TestE2E_TransformOnErrorWrap(t *testing.T) {
	logDir, sinkPath, statePath := setupDirs(t)
	logFile := filepath.Join(logDir, "app.log")

	if err := os.WriteFile(logFile, []byte("not-a-valid-line\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := tabLineRegexConfig(logDir, sinkPath, statePath, "wrap")
	startForwarder(t, cfg, harnessOptions{})
	waitForRecordCount(t, sinkPath, 1)

	records := readJSONLRecords(t, sinkPath)
	if records[0]["_raw"] != "not-a-valid-line" {
		t.Fatalf("_raw = %v", records[0]["_raw"])
	}
	if _, ok := records[0]["_error"]; !ok {
		t.Fatal("expected _error field")
	}
}

func TestE2E_HTTPSinkReceivesJSON(t *testing.T) {
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

	if err := os.WriteFile(logFile, []byte("2024-01-01T00:00:00Z\tINFO\thttp-test\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := tabLineRegexConfig(logDir, "", statePath, "wrap")
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
	if record["message"] != "http-test" {
		t.Fatalf("message = %v", record["message"])
	}
}

func TestE2E_PublishRetryRecovers(t *testing.T) {
	logDir, sinkPath, statePath := setupDirs(t)
	logFile := filepath.Join(logDir, "app.log")

	if err := os.WriteFile(logFile, []byte("2024-01-01T00:00:00Z\tINFO\tretry-me\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := tabLineRegexConfig(logDir, sinkPath, statePath, "wrap")
	cfg.Pipeline.PublishRetry = config.PublishRetryConfig{
		InitialBackoff: "5ms",
		MaxBackoff:     "20ms",
		MaxAttempts:    0,
	}

	flaky := &flakyFileSink{
		innerPath: sinkPath,
		failures:  2,
	}
	startForwarder(t, cfg, harnessOptions{sink: flaky})
	waitForRecordCount(t, sinkPath, 1)

	if got := atomic.LoadInt32(&flaky.calls); got < 3 {
		t.Fatalf("publish calls = %d, want at least 3", got)
	}
}

func TestE2E_MetricsHealthAndCounters(t *testing.T) {
	logDir, sinkPath, statePath := setupDirs(t)
	logFile := filepath.Join(logDir, "app.log")

	if err := os.WriteFile(logFile, []byte("2024-01-01T00:00:00Z\tINFO\tmetrics-line\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := tabLineRegexConfig(logDir, sinkPath, statePath, "wrap")
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	metricsPort := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()

	startForwarder(t, cfg, harnessOptions{metricsEnabled: true, metricsPort: metricsPort})
	waitForRecordCount(t, sinkPath, 1)

	base := "http://127.0.0.1:" + strconv.Itoa(metricsPort)
	resp, err := http.Get(base + "/health")
	if err != nil {
		t.Fatalf("GET /health error = %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/health status = %d", resp.StatusCode)
	}

	resp, err = http.Get(base + "/ready")
	if err != nil {
		t.Fatalf("GET /ready error = %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/ready status = %d", resp.StatusCode)
	}

	resp, err = http.Get(base + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics error = %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	metricsBody := string(body)
	if !strings.Contains(metricsBody, "log_forwarder_lines_read") {
		t.Fatalf("metrics missing lines_read: %s", metricsBody)
	}
	if !strings.Contains(metricsBody, "log_forwarder_lines_published") {
		t.Fatalf("metrics missing lines_published: %s", metricsBody)
	}
}

type flakyFileSink struct {
	innerPath string
	failures  int32
	calls     int32
	inner     sink.Sink
}

func (f *flakyFileSink) init() error {
	if f.inner != nil {
		return nil
	}
	s, err := sink.New(config.SinkConfig{
		Type: "file",
		File: &config.FileSinkConfig{Path: f.innerPath},
	})
	if err != nil {
		return err
	}
	f.inner = s
	return nil
}

func (f *flakyFileSink) Publish(ctx context.Context, payload []byte) error {
	if err := f.init(); err != nil {
		return err
	}
	call := atomic.AddInt32(&f.calls, 1)
	if int(call) <= int(f.failures) {
		return errors.New("injected publish failure")
	}
	return f.inner.Publish(ctx, payload)
}

func (f *flakyFileSink) Close() error {
	if f.inner == nil {
		return nil
	}
	return f.inner.Close()
}

func containsAll(values []string, want ...string) bool {
	for _, w := range want {
		found := false
		for _, v := range values {
			if v == w {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
