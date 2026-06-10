// Package main demonstrates registering custom transformers and enrichers.
//
// Build with:
//
//	go build -o log-forwarder-custom ./examples/custom
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/sanjuthomas/log-forwarder/internal/config"
	"github.com/sanjuthomas/log-forwarder/internal/enrich"
	"github.com/sanjuthomas/log-forwarder/internal/pipeline"
	"github.com/sanjuthomas/log-forwarder/internal/sink"
	"github.com/sanjuthomas/log-forwarder/internal/state"
	"github.com/sanjuthomas/log-forwarder/internal/transform"
	"github.com/sanjuthomas/log-forwarder/internal/watcher"
)

func init() {
	transform.Register("uppercase_tab", func(cfg config.TransformConfig) (transform.Transformer, error) {
		base, err := transform.New(config.TransformConfig{Type: "tab", Columns: cfg.Columns})
		if err != nil {
			return nil, err
		}
		return &uppercaseTab{base: base}, nil
	})

	enrich.Register("region", func(cfg config.EnricherConfig) (enrich.Enricher, error) {
		region := cfg.Fields["region"]
		if region == "" {
			region = "unknown"
		}
		return &regionEnricher{region: region}, nil
	})
}

type uppercaseTab struct {
	base transform.Transformer
}

func (u *uppercaseTab) Transform(line []byte) (transform.Record, error) {
	record, err := u.base.Transform(line)
	if err != nil {
		return nil, err
	}
	if msg, ok := record["message"].(string); ok {
		record["message"] = strings.ToUpper(msg)
	}
	return record, nil
}

type regionEnricher struct {
	region string
}

func (r *regionEnricher) Enrich(record transform.Record) transform.Record {
	record["region"] = r.region
	return record
}

func main() {
	configPath := flag.String("config", "", "path to YAML config file (optional)")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	var cfg *config.Config
	var err error
	if *configPath == "" {
		cfg = config.Default()
	} else {
		cfg, err = config.Load(*configPath)
	}
	if err != nil {
		logger.Error("load config", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	lines := make(chan watcher.LineEvent, cfg.Pipeline.BufferSize)

	watermarks, err := state.NewStore(cfg.StatePath(), watermarkOptions(cfg))
	if err != nil {
		logger.Error("load watermarks", "path", cfg.StatePath(), "error", err)
		os.Exit(1)
	}
	flushCtx, flushCancel := context.WithCancel(context.Background())
	go watermarks.RunPeriodicFlush(flushCtx)
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
	defer recordSink.Close()

	if checker, ok := recordSink.(sink.Checker); ok {
		pingCtx, cancel := context.WithTimeout(context.Background(), cfg.SinkConnectTimeout())
		err = checker.Check(pingCtx)
		cancel()
		if err != nil {
			logger.Error(
				"sink unavailable at startup; refusing to start forwarder",
				"type", cfg.Sink.Type,
				"error", err,
			)
			os.Exit(1)
		}
	}

	pipe, err := pipeline.New(cfg, recordSink, logger, pipeline.Options{
		Watermarks: watermarks,
	})
	if err != nil {
		logger.Error("create pipeline", "error", err)
		os.Exit(1)
	}

	w := watcher.New(cfg, lines, watermarks, nil, logger)
	errCh := make(chan error, 2)
	go func() { errCh <- w.Run(ctx) }()
	go func() { errCh <- pipe.Run(ctx, lines) }()

	logger.Info("custom log forwarder started")
	if err := <-errCh; err != nil && err != context.Canceled {
		logger.Error("forwarder stopped", "error", err)
		os.Exit(1)
	}
	close(lines)
}

func watermarkOptions(cfg *config.Config) state.Options {
	flushInterval, flushEvery := cfg.Watch.State.PersistOptions()
	return state.Options{
		FlushInterval: flushInterval,
		FlushEvery:    flushEvery,
	}
}
