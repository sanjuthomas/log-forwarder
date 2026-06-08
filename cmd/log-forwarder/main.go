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
	"github.com/sanjuthomas/log-forwarder/internal/pipeline"
	"github.com/sanjuthomas/log-forwarder/internal/runtime"
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

	kafkaSink, err := sink.NewKafka(cfg.Kafka)
	if err != nil {
		logger.Error("create kafka sink", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := kafkaSink.Close(); err != nil {
			logger.Error("close kafka sink", "error", err)
		}
	}()

	pingCtx, cancel := context.WithTimeout(context.Background(), cfg.Kafka.ConnectTimeoutDuration())
	err = sink.CheckConnectivity(pingCtx, cfg.Kafka)
	cancel()
	if err != nil {
		logger.Error(
			"kafka unavailable at startup; refusing to start forwarder",
			"brokers", cfg.Kafka.Brokers,
			"topic", cfg.Kafka.Topic,
			"error", err,
		)
		os.Exit(1)
	}
	logger.Info("kafka connectivity verified", "brokers", cfg.Kafka.Brokers, "topic", cfg.Kafka.Topic)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	lines := make(chan watcher.LineEvent, cfg.Pipeline.BufferSize)
	stats := &runtime.Stats{}

	pipe, err := pipeline.New(cfg, kafkaSink, logger, pipeline.Options{
		Watermarks: watermarks,
		Stats:      stats,
	})
	if err != nil {
		logger.Error("create pipeline", "error", err)
		os.Exit(1)
	}

	w := watcher.New(cfg, lines, watermarks, logger)

	errCh := make(chan error, 2)
	go func() { errCh <- w.Run(ctx) }()
	go func() { errCh <- pipe.Run(ctx, lines) }()

	if interval := cfg.Logging.StatusIntervalDuration(); interval > 0 {
		go logStatus(ctx, logger, w, stats, interval)
	}

	logger.Info("log forwarder started",
		"sources", cfg.Watch.Entries(),
		"topic", cfg.Kafka.Topic,
		"state_path", cfg.StatePath(),
	)

	if err := <-errCh; err != nil && err != context.Canceled {
		logger.Error("forwarder stopped", "error", err)
		os.Exit(1)
	}

	close(lines)
	logger.Info("log forwarder stopped",
		"lines_published", stats.LinesPublished.Load(),
		"publish_failures", stats.PublishFailures.Load(),
	)
}

func logStatus(ctx context.Context, logger *slog.Logger, w *watcher.Watcher, stats *runtime.Stats, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			logger.Info("forwarder status",
				"watched_files", w.FileCount(),
				"lines_published", stats.LinesPublished.Load(),
				"publish_failures", stats.PublishFailures.Load(),
			)
		}
	}
}

func loadConfig(path string) (*config.Config, error) {
	if path == "" {
		return config.Default(), nil
	}
	return config.Load(path)
}
