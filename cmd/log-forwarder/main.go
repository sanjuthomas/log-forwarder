// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

// Package main is the entrypoint for the built-in log-forwarder binary.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/sanjuthomas/log-forwarder/internal/atc"
	"github.com/sanjuthomas/log-forwarder/internal/config"
	applogging "github.com/sanjuthomas/log-forwarder/internal/logging"
	"github.com/sanjuthomas/log-forwarder/internal/metrics"
	"github.com/sanjuthomas/log-forwarder/internal/pipeline"
	"github.com/sanjuthomas/log-forwarder/internal/runner"
	"github.com/sanjuthomas/log-forwarder/internal/sink"
	"github.com/sanjuthomas/log-forwarder/internal/state"
	"github.com/sanjuthomas/log-forwarder/internal/watcher"
)

func main() {
	configPath := flag.String("config", "", "path to YAML config file (optional)")
	resetWatermarks := flag.Bool("reset-watermarks", false, "archive a corrupt watermark file and start fresh")
	flag.Parse()

	cfg, err := loadConfig(*configPath)
	if err != nil {
		slog.New(slog.NewTextHandler(os.Stderr, nil)).Error("load config", "error", err)
		os.Exit(1)
	}

	logger, logCloser, err := applogging.New(cfg.Logging)
	if err != nil {
		slog.New(slog.NewTextHandler(os.Stderr, nil)).Error("create logger", "error", err)
		os.Exit(1)
	}
	defer logCloser.Close()

	wmOpts := watermarkOptions(cfg, *resetWatermarks)
	watermarks, err := state.NewStore(cfg.StatePath(), wmOpts)
	if err != nil {
		logger.Error("load watermarks", "path", cfg.StatePath(), "error", err)
		os.Exit(1)
	}
	if backup := watermarks.CorruptBackupPath(); backup != "" {
		logger.Warn("archived corrupt watermark file and started fresh",
			"path", cfg.StatePath(),
			"backup", backup,
		)
	}
	flushCtx, flushCancel := context.WithCancel(context.Background())
	defer func() {
		flushCancel()
		if err := watermarks.Flush(); err != nil {
			logger.Error("flush watermarks", "error", err)
		}
	}()

	recordSink, err := sink.New(cfg.Sink)
	if err != nil {
		logger.Error("create sink", "type", cfg.Sink.Type, "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := recordSink.Close(); err != nil {
			logger.Error("close sink", "error", err)
		}
	}()

	if err := checkSinkAtStartup(recordSink, cfg); err != nil {
		logger.Error("sink unavailable at startup; refusing to start forwarder", "type", cfg.Sink.Type, "error", err)
		os.Exit(1)
	}
	logger.Info("sink connectivity verified", "type", cfg.Sink.Type)

	if err := cfg.ValidateDeadLetterAtStartup(); err != nil {
		logger.Error("dead letter path unavailable at startup; refusing to start forwarder", "error", err)
		os.Exit(1)
	}

	signalCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	runCtx, runCancel := context.WithCancel(context.Background())
	defer runCancel()

	atcClient := atc.NewClient(cfg.ATC)
	atcInstance := atc.NewInstance(cfg)
	var deregisterOnce sync.Once
	deregisterFromATC := func() {
		deregisterOnce.Do(func() {
			if atcClient == nil {
				return
			}
			deregCtx, deregCancel := context.WithTimeout(context.Background(), cfg.ATC.TimeoutDuration())
			defer deregCancel()
			deregInstance := atcInstance
			deregInstance.Timestamp = time.Now().UTC().Format(time.RFC3339Nano)
			logger.Info("atc registration status", "status", "deregistering", "url", cfg.ATC.EndpointURL(), "hostname", deregInstance.Hostname, "port", deregInstance.Port, "process_id", deregInstance.ProcessID)
			if err := atcClient.Deregister(deregCtx, deregInstance); err != nil {
				logATCDeregistrationStatus(logger, cfg, err, deregInstance)
			} else {
				logATCDeregistrationStatus(logger, cfg, nil, deregInstance)
			}
		})
	}
	defer deregisterFromATC()

	cancelRunners := func() {
		runCancel()
	}

	lines := make(chan watcher.LineEvent, cfg.Pipeline.BufferSize)

	var forwarderWatcher *watcher.Watcher
	var forwarderPipe *pipeline.Pipeline
	snapshot := metrics.Snapshot{
		FilesWatched: func() int64 {
			if forwarderWatcher == nil {
				return 0
			}
			return int64(forwarderWatcher.FileCount())
		},
		BufferDepth: func() int64 {
			return int64(len(lines))
		},
		BufferCapacity: int64(cfg.Pipeline.BufferSize),
		PublishBufferActiveBytes: func() int64 {
			if forwarderPipe == nil {
				return 0
			}
			return forwarderPipe.PublishBufferActiveBytes()
		},
		PublishHibernating: func() int64 {
			if forwarderPipe == nil {
				return 0
			}
			return forwarderPipe.HibernatingSnapshot()
		},
		PublishConsecutiveDLQBatches: func() int64 {
			if forwarderPipe == nil {
				return 0
			}
			return forwarderPipe.ConsecutiveDLQSnapshot()
		},
	}
	readiness := buildReadiness(cfg, recordSink, snapshot, func() bool {
		if forwarderPipe == nil {
			return false
		}
		return forwarderPipe.Hibernating()
	})
	collector, shutdownMetrics, err := metrics.New(cfg.Metrics, snapshot, readiness, buildDeadLetters(cfg))
	if err != nil {
		logger.Error("create metrics collector", "error", err)
		os.Exit(1)
	}
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		if err := shutdownMetrics(shutdownCtx); err != nil {
			logger.Error("shutdown metrics", "error", err)
		}
	}()
	if err := collector.Start(logger); err != nil {
		logger.Error("start metrics server", "error", err)
		os.Exit(1)
	}

	startPeriodicWatermarkFlush(watermarks, flushCtx, cfg.StatePath(), logger, collector, wmOpts.FlushInterval)

	forwarderPipe, err = pipeline.New(cfg, recordSink, logger, pipeline.Options{
		Watermarks: watermarks,
		Metrics:    collector,
	})
	if err != nil {
		logger.Error("create pipeline", "error", err)
		os.Exit(1)
	}
	pipe := forwarderPipe

	forwarderWatcher = watcher.New(cfg, lines, watermarks, collector, logger)

	errCh := make(chan error, 2)
	go func() { errCh <- forwarderWatcher.Run(runCtx) }()
	go func() { errCh <- pipe.Run(runCtx, lines) }()

	if interval := cfg.Logging.StatusIntervalDuration(); interval > 0 {
		go logStatus(runCtx, logger, forwarderWatcher, interval)
	}

	startAttrs := []any{
		"sources", cfg.Watch.Entries(),
		"sink_type", cfg.Sink.Type,
		"state_path", cfg.StatePath(),
		"metrics_enabled", cfg.Metrics.Enabled,
	}
	if cfg.Metrics.Enabled {
		startAttrs = append(startAttrs,
			"metrics_addr", cfg.Metrics.Addr(),
			"metrics_path", cfg.Metrics.MetricsPath(),
			"readiness_path", cfg.Metrics.Readiness.ReadyPath(),
		)
	}
	if cfg.Pipeline.OnFull == "drop" {
		logger.Warn("pipeline.on_full is drop — lines discarded when the buffer is full are permanent loss and are not replayed on restart",
			"buffer_size", cfg.Pipeline.BufferSize,
		)
	}
	logger.Info("log forwarder started", startAttrs...)

	if atcClient != nil {
		regCtx, regCancel := context.WithTimeout(context.Background(), cfg.ATC.TimeoutDuration())
		regInstance := atcInstance
		regInstance.Timestamp = time.Now().UTC().Format(time.RFC3339Nano)
		logger.Info("atc registration status", "status", "registering", "url", cfg.ATC.EndpointURL(), "hostname", regInstance.Hostname, "port", regInstance.Port, "process_id", regInstance.ProcessID)
		err := atcClient.Register(regCtx, regInstance)
		logATCRegistrationStatus(logger, cfg, err, regInstance)
		regCancel()
	} else {
		logATCRegistrationStatus(logger, cfg, nil, atcInstance)
	}

	runnerDone := make(chan error, 1)
	go func() {
		runnerDone <- runner.Wait(errCh, cancelRunners)
	}()

	select {
	case err := <-runnerDone:
		deregisterFromATC()
		if err != nil {
			logger.Error("forwarder stopped", "error", err)
			os.Exit(1)
		}
	case <-signalCtx.Done():
		logger.Info("shutdown signal received")
		deregisterFromATC()
		cancelRunners()
		if err := <-runnerDone; err != nil {
			logger.Error("forwarder stopped", "error", err)
			os.Exit(1)
		}
	}

	close(lines)
	logger.Info("log forwarder stopped")
}

func logATCRegistrationStatus(logger *slog.Logger, cfg *config.Config, err error, inst atc.Instance) {
	if !cfg.ATC.Enabled {
		logger.Info("atc registration status", "status", "disabled")
		return
	}
	attrs := []any{
		"status", "registered",
		"url", cfg.ATC.EndpointURL(),
		"hostname", inst.Hostname,
		"port", inst.Port,
		"process_id", inst.ProcessID,
		"timestamp", inst.Timestamp,
	}
	if err != nil {
		attrs[1] = "failed"
		attrs = append(attrs, "error", err)
		logger.Warn("atc registration status", attrs...)
		return
	}
	logger.Info("atc registration status", attrs...)
}

func logATCDeregistrationStatus(logger *slog.Logger, cfg *config.Config, err error, inst atc.Instance) {
	attrs := []any{
		"status", "deregistered",
		"url", cfg.ATC.EndpointURL(),
		"hostname", inst.Hostname,
		"port", inst.Port,
		"process_id", inst.ProcessID,
		"timestamp", inst.Timestamp,
	}
	if err != nil {
		attrs[1] = "deregistration_failed"
		attrs = append(attrs, "error", err)
		logger.Warn("atc registration status", attrs...)
		return
	}
	logger.Info("atc registration status", attrs...)
}

func checkSinkAtStartup(s sink.Sink, cfg *config.Config) error {
	checker, ok := s.(sink.Checker)
	if !ok {
		return nil
	}

	pingCtx, cancel := context.WithTimeout(context.Background(), cfg.SinkConnectTimeout())
	defer cancel()
	return checker.Check(pingCtx)
}

func buildDeadLetters(cfg *config.Config) *metrics.DeadLetters {
	path := cfg.Pipeline.PublishBatch.DeadLetter.Path
	if path == "" {
		return nil
	}
	return &metrics.DeadLetters{Dir: path}
}

func buildReadiness(cfg *config.Config, recordSink sink.Sink, snapshot metrics.Snapshot, isHibernating func() bool) *metrics.Readiness {
	if !cfg.Metrics.Enabled {
		return nil
	}

	readiness := &metrics.Readiness{
		Snapshot:         snapshot,
		BufferThreshold:  cfg.Metrics.Readiness.BufferThresholdOrDefault(),
		RequireFiles:     cfg.Metrics.Readiness.RequireFiles,
		SinkCheckEnabled: cfg.Metrics.Readiness.SinkCheckEnabled(),
		SinkCheckTimeout: cfg.Metrics.Readiness.SinkCheckTimeoutDuration(cfg.SinkConnectTimeout()),
		IsHibernating:    isHibernating,
	}
	if checker, ok := recordSink.(sink.Checker); ok {
		readiness.CheckSink = checker.Check
	}
	return readiness
}

func logStatus(ctx context.Context, logger *slog.Logger, w *watcher.Watcher, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			logger.Info("forwarder status", "watched_files", w.FileCount())
		}
	}
}

func loadConfig(path string) (*config.Config, error) {
	if path == "" {
		return config.Default(), nil
	}
	return config.Load(path)
}

func watermarkOptions(cfg *config.Config, resetWatermarks bool) state.Options {
	flushInterval, flushEvery := cfg.Watch.State.PersistOptions()
	return state.Options{
		FlushInterval:  flushInterval,
		FlushEvery:     flushEvery,
		ResetOnCorrupt: cfg.Watch.State.ResetOnCorrupt || resetWatermarks,
	}
}

func startPeriodicWatermarkFlush(watermarks *state.Store, ctx context.Context, statePath string, logger *slog.Logger, collector *metrics.Collector, flushInterval time.Duration) {
	if flushInterval <= 0 {
		return
	}
	watermarks.SetOnPeriodicFlushError(func(err error) {
		logger.Error("periodic watermark flush failed", "path", statePath, "error", err)
		collector.RecordWatermarkFlushError(context.Background())
	})
	go watermarks.RunPeriodicFlush(ctx)
}
