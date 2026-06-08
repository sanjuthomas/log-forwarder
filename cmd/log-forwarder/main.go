package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/sanjuthomas/log-forwarder/internal/config"
	"github.com/sanjuthomas/log-forwarder/internal/pipeline"
	"github.com/sanjuthomas/log-forwarder/internal/sink"
	"github.com/sanjuthomas/log-forwarder/internal/watcher"
)

func main() {
	configPath := flag.String("config", "", "path to YAML config file (optional)")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cfg, err := loadConfig(*configPath)
	if err != nil {
		logger.Error("load config", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	lines := make(chan watcher.LineEvent, cfg.Pipeline.BufferSize)

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

	pipe, err := pipeline.New(cfg, kafkaSink, logger)
	if err != nil {
		logger.Error("create pipeline", "error", err)
		os.Exit(1)
	}

	w := watcher.New(cfg, lines, logger)

	errCh := make(chan error, 2)
	go func() { errCh <- w.Run(ctx) }()
	go func() { errCh <- pipe.Run(ctx, lines) }()

	logger.Info("log forwarder started",
		"sources", cfg.Watch.Entries(),
		"topic", cfg.Kafka.Topic,
	)

	if err := <-errCh; err != nil && err != context.Canceled {
		logger.Error("forwarder stopped", "error", err)
		os.Exit(1)
	}

	close(lines)
	logger.Info("log forwarder stopped")
}

func loadConfig(path string) (*config.Config, error) {
	if path == "" {
		return config.Default(), nil
	}
	return config.Load(path)
}
