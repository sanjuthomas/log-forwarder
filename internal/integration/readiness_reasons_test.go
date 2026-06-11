// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package integration_test

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sanjuthomas/log-forwarder/internal/config"
	"github.com/sanjuthomas/log-forwarder/internal/sink"
)

type flakyCheckSink struct {
	inner     sink.Sink
	failCheck int32
}

func (f *flakyCheckSink) Publish(ctx context.Context, payload []byte) error {
	return f.inner.Publish(ctx, payload)
}

func (f *flakyCheckSink) PublishBatch(ctx context.Context, payloads [][]byte) error {
	if batcher, ok := f.inner.(sink.BatchSink); ok {
		return batcher.PublishBatch(ctx, payloads)
	}
	for _, payload := range payloads {
		if err := f.inner.Publish(ctx, payload); err != nil {
			return err
		}
	}
	return nil
}

func (f *flakyCheckSink) Check(context.Context) error {
	if atomic.LoadInt32(&f.failCheck) > 0 {
		return errors.New("connection refused")
	}
	if checker, ok := f.inner.(sink.Checker); ok {
		return checker.Check(context.Background())
	}
	return nil
}

func (f *flakyCheckSink) Close() error { return f.inner.Close() }

func TestE2E_ReadinessReasonsOnLiveForwarder(t *testing.T) {
	t.Run("sink_unreachable", func(t *testing.T) {
		logDir, sinkPath, statePath := setupDirs(t)
		logFile := filepath.Join(logDir, "app.log")
		if err := os.WriteFile(logFile, []byte("2024-01-01T00:00:00Z\tINFO\tready-check\n"), 0o644); err != nil {
			t.Fatal(err)
		}

		cfg := tabLineRegexConfig(logDir, sinkPath, statePath, "wrap")
		cfg.Metrics.Readiness.SinkCheck = boolPtr(true)

		inner, err := sink.New(cfg.Sink)
		if err != nil {
			t.Fatal(err)
		}

		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		metricsPort := ln.Addr().(*net.TCPAddr).Port
		_ = ln.Close()

		flaky := &flakyCheckSink{inner: inner}
		startForwarder(t, cfg, harnessOptions{
			sink:           flaky,
			metricsEnabled: true,
			metricsPort:    metricsPort,
		})
		atomic.StoreInt32(&flaky.failCheck, 1)

		base := "http://127.0.0.1:" + strconv.Itoa(metricsPort)
		waitForReadyReason(t, base, "sink_unreachable")
	})

	t.Run("pipeline_buffer_high", func(t *testing.T) {
		stall := newStallSink()
		logDir, sinkPath, statePath := setupDirs(t)
		logFile := filepath.Join(logDir, "app.log")
		if err := os.WriteFile(logFile, []byte("2024-01-01T00:00:00Z\tINFO\tline-one\n"), 0o644); err != nil {
			t.Fatal(err)
		}

		cfg := tabLineConfig(logDir, sinkPath, statePath, "wrap", config.FilterConfig{})
		cfg.Pipeline.BufferSize = 2
		cfg.Metrics.Readiness.BufferThreshold = 0.8

		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		metricsPort := ln.Addr().(*net.TCPAddr).Port
		_ = ln.Close()

		h := startForwarder(t, cfg, harnessOptions{
			sink:           stall,
			metricsEnabled: true,
			metricsPort:    metricsPort,
		})

		select {
		case <-stall.blocked():
		case <-time.After(5 * time.Second):
			t.Fatal("pipeline did not block on publish")
		}

		for i := 0; i < 4; i++ {
			appendToFile(t, logFile, "2024-01-01T00:00:01Z\tINFO\textra\n")
		}

		base := "http://127.0.0.1:" + strconv.Itoa(metricsPort)
		waitForReadyReason(t, base, "pipeline_buffer_high")
		h.cancelAndWait(t)
	})

	t.Run("no_files_watched", func(t *testing.T) {
		logDir, sinkPath, statePath := setupDirs(t)
		_ = os.RemoveAll(logDir)

		cfg := tabLineRegexConfig(logDir, sinkPath, statePath, "wrap")
		cfg.Metrics.Readiness.RequireFiles = true

		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		metricsPort := ln.Addr().(*net.TCPAddr).Port
		_ = ln.Close()

		startForwarder(t, cfg, harnessOptions{metricsEnabled: true, metricsPort: metricsPort})

		base := "http://127.0.0.1:" + strconv.Itoa(metricsPort)
		waitForReadyReason(t, base, "no_files_watched")
	})
}

func boolPtr(v bool) *bool { return &v }
