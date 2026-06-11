// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package integration_test

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/sanjuthomas/log-forwarder/internal/config"
	"github.com/sanjuthomas/log-forwarder/internal/sink"
)

type thresholdSink struct {
	inner     sink.Sink
	mu        sync.Mutex
	failUntil int
	calls     int
}

func (s *thresholdSink) Publish(ctx context.Context, payload []byte) error {
	return s.PublishBatch(ctx, [][]byte{payload})
}

func (s *thresholdSink) PublishBatch(ctx context.Context, payloads [][]byte) error {
	s.mu.Lock()
	s.calls++
	fail := s.calls <= s.failUntil
	s.mu.Unlock()
	if fail {
		return errors.New("publish failed")
	}
	if batcher, ok := s.inner.(sink.BatchSink); ok {
		return batcher.PublishBatch(ctx, payloads)
	}
	for _, payload := range payloads {
		if err := s.inner.Publish(ctx, payload); err != nil {
			return err
		}
	}
	return nil
}

func (s *thresholdSink) Close() error { return nil }

func (s *thresholdSink) setFailUntil(n int) {
	s.mu.Lock()
	s.failUntil = n
	s.mu.Unlock()
}

func TestE2E_HibernateWakeRecoversWithoutRestart(t *testing.T) {
	logDir, sinkPath, statePath := setupDirs(t)
	logFile := filepath.Join(logDir, "app.log")

	if err := os.WriteFile(logFile, []byte("2024-01-01T00:00:00Z\tINFO\twake-line\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := tabLineRegexConfig(logDir, sinkPath, statePath, "wrap")
	cfg.Pipeline.PublishBatch = config.PublishBatchConfig{
		MaxBytes:       1 << 20,
		FlushInterval:  "20ms",
		OnFlushFailure: config.OnFlushFailureHibernate,
		MaxAttempts:    2,
		Hibernate: config.HibernateConfig{
			WakeEnabled:  true,
			WakeInterval: "50ms",
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

	fileSink, err := sink.New(config.SinkConfig{
		Type: "file",
		File: &config.FileSinkConfig{Path: sinkPath},
	})
	if err != nil {
		t.Fatal(err)
	}
	threshold := &thresholdSink{inner: fileSink, failUntil: 100}
	startForwarder(t, cfg, harnessOptions{
		sink:           threshold,
		metricsEnabled: true,
		metricsPort:    metricsPort,
	})

	base := "http://127.0.0.1:" + strconv.Itoa(metricsPort)
	waitForReadyReason(t, base, "sink_hibernating")

	threshold.setFailUntil(0)
	waitForReadyOK(t, base)
	waitForRecordCount(t, sinkPath, 1)
	time.Sleep(50 * time.Millisecond)
	waitForWatermarkOffset(t, statePath, logFile)
}

func waitForReadyOK(t *testing.T, base string) {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := httpGetReady(base)
		if err == nil && resp == 200 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("timed out waiting for /ready 200")
}

func httpGetReady(base string) (int, error) {
	resp, err := http.Get(base + "/ready")
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	return resp.StatusCode, nil
}

// Ensure thresholdSink satisfies sink.Sink at compile time.
var _ sink.Sink = (*thresholdSink)(nil)
