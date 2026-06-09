package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sanjuthomas/log-forwarder/internal/config"
	applogging "github.com/sanjuthomas/log-forwarder/internal/logging"
	"github.com/sanjuthomas/log-forwarder/internal/metrics"
	"github.com/sanjuthomas/log-forwarder/internal/pipeline"
	"github.com/sanjuthomas/log-forwarder/internal/sink"
	"github.com/sanjuthomas/log-forwarder/internal/state"
	"github.com/sanjuthomas/log-forwarder/internal/watcher"
)

func main() {
	configPath := flag.String("config", "", "path to YAML config file (optional)")
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

	watermarks, err := state.NewStore(cfg.StatePath())
	if err != nil {
		logger.Error("load watermarks", "path", cfg.StatePath(), "error", err)
		os.Exit(1)
	}

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

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	lines := make(chan watcher.LineEvent, cfg.Pipeline.BufferSize)

	var forwarderWatcher *watcher.Watcher
	collector, shutdownMetrics, err := metrics.New(cfg.Metrics, metrics.Snapshot{
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
	})
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

	pipe, err := pipeline.New(cfg, recordSink, logger, pipeline.Options{
		Watermarks: watermarks,
		Metrics:    collector,
	})
	if err != nil {
		logger.Error("create pipeline", "error", err)
		os.Exit(1)
	}

	forwarderWatcher = watcher.New(cfg, lines, watermarks, collector, logger)

	errCh := make(chan error, 2)
	go func() { errCh <- forwarderWatcher.Run(ctx) }()
	go func() { errCh <- pipe.Run(ctx, lines) }()

	if interval := cfg.Logging.StatusIntervalDuration(); interval > 0 {
		go logStatus(ctx, logger, forwarderWatcher, interval)
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
		)
	}
	logger.Info("log forwarder started", startAttrs...)

	if err := <-errCh; err != nil && err != context.Canceled {
		logger.Error("forwarder stopped", "error", err)
		os.Exit(1)
	}

	close(lines)
	logger.Info("log forwarder stopped")
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
