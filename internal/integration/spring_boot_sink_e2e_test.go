// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package integration_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sanjuthomas/log-forwarder/internal/config"
	"github.com/sanjuthomas/log-forwarder/internal/sink"
)

type captureSink struct {
	mu       sync.Mutex
	payloads [][]byte
}

func (c *captureSink) Publish(ctx context.Context, payload []byte) error {
	return c.PublishBatch(ctx, [][]byte{payload})
}

func (c *captureSink) PublishBatch(_ context.Context, payloads [][]byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, p := range payloads {
		cp := make([]byte, len(p))
		copy(cp, p)
		c.payloads = append(c.payloads, cp)
	}
	return nil
}

func (c *captureSink) Close() error { return nil }

func (c *captureSink) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.payloads)
}

func (c *captureSink) first() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.payloads) == 0 {
		return nil
	}
	return c.payloads[0]
}

var _ sink.Sink = (*captureSink)(nil)

func springBootStackTraceFixture() string {
	return strings.Join([]string{
		"2026-06-08 10:16:22.901  ERROR 18432 --- [nio-8080-exec-5] c.example.PaymentController : Payment failed",
		"org.springframework.dao.DataIntegrityViolationException: could not execute statement",
		"        at org.springframework.orm.jpa.vendor.HibernateJpaDialect.convertHibernateAccessException(HibernateJpaDialect.java:290)",
		"Caused by: org.postgresql.util.PSQLException: ERROR: constraint",
		"2026-06-08 10:16:23.901  INFO 18432 --- [main] c.example.App : flush-sentinel",
	}, "\n") + "\n"
}

func TestE2E_SpringBootMultilineKafkaSink(t *testing.T) {
	logDir, _, statePath := setupDirs(t)
	logFile := filepath.Join(logDir, "app.log")
	if err := os.WriteFile(logFile, []byte(springBootStackTraceFixture()), 0o644); err != nil {
		t.Fatal(err)
	}

	capture := &captureSink{}
	cfg := springBootConfig(logDir, "", statePath)
	startForwarder(t, cfg, harnessOptions{sink: capture})

	waitForCaptureCount(t, capture, 1)
	assertSpringBootStackRecord(t, capture.first())
}

func TestE2E_SpringBootMultilineHTTPSink(t *testing.T) {
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

	if err := os.WriteFile(logFile, []byte(springBootStackTraceFixture()), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := springBootConfig(logDir, "", statePath)
	cfg.Sink.Type = "http-noauth"
	cfg.Sink.File = nil
	cfg.Sink.HTTPNoauth = &config.HTTPNoauthSinkConfig{
		URL:     srv.URL,
		Method:  "POST",
		Timeout: "5s",
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
	assertSpringBootStackRecord(t, bodies[0])
}

func waitForCaptureCount(t *testing.T, capture *captureSink, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if capture.count() >= want {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %d captured payloads, got %d", want, capture.count())
}

func assertSpringBootStackRecord(t *testing.T, payload []byte) {
	t.Helper()
	var record map[string]any
	if err := json.Unmarshal(payload, &record); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if record["level"] != "ERROR" {
		t.Fatalf("level = %v, want ERROR", record["level"])
	}
	msg, ok := record["message"].(string)
	if !ok {
		t.Fatalf("message type = %T", record["message"])
	}
	if !strings.Contains(msg, "Payment failed") {
		t.Fatalf("message missing header: %q", msg)
	}
	if !strings.Contains(msg, "PSQLException") {
		t.Fatalf("message missing stack trace: %q", msg)
	}
}
