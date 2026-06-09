package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/sanjuthomas/log-forwarder/internal/config"
	"github.com/sanjuthomas/log-forwarder/internal/enrich"
	"github.com/sanjuthomas/log-forwarder/internal/metrics"
	"github.com/sanjuthomas/log-forwarder/internal/sink"
	"github.com/sanjuthomas/log-forwarder/internal/state"
	"github.com/sanjuthomas/log-forwarder/internal/transform"
	"github.com/sanjuthomas/log-forwarder/internal/watcher"
)

// Pipeline processes log lines through transform, enrich, and publish stages.
type Pipeline struct {
	cfg         *config.Config
	transformer transform.Transformer
	enrichers   []enrich.Enricher
	sink        sink.Sink
	watermarks  *state.Store
	metrics     *metrics.Collector
	logger      *slog.Logger
}

type Options struct {
	Watermarks *state.Store
	Metrics    *metrics.Collector
}

func New(cfg *config.Config, s sink.Sink, logger *slog.Logger, opts Options) (*Pipeline, error) {
	t, err := transform.New(cfg.Transform)
	if err != nil {
		return nil, err
	}
	chain, err := enrich.NewChain(cfg.Enrichers)
	if err != nil {
		return nil, err
	}
	return &Pipeline{
		cfg:         cfg,
		transformer: t,
		enrichers:   chain,
		sink:        s,
		watermarks:  opts.Watermarks,
		metrics:     opts.Metrics,
		logger:      logger,
	}, nil
}

func (p *Pipeline) Run(ctx context.Context, lines <-chan watcher.LineEvent) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-lines:
			if !ok {
				return nil
			}
			if err := p.process(ctx, event); err != nil {
				return err
			}
		}
	}
}

func (p *Pipeline) process(ctx context.Context, event watcher.LineEvent) error {
	record, err := p.transformer.Transform(event.Line)
	skipPublish := false
	if err != nil {
		p.metrics.RecordTransformError(ctx)
		switch p.cfg.Transform.OnError {
		case "skip":
			p.logger.Debug("skipping line", "path", event.Path, "error", err)
			skipPublish = true
			p.metrics.RecordLineSkipped(ctx)
		case "wrap":
			record = transform.Record{
				"_raw":   string(event.Line),
				"_path":  event.Path,
				"_error": err.Error(),
			}
		default:
			return err
		}
	}

	if !skipPublish {
		record["_path"] = event.Path
		record = enrich.Apply(p.enrichers, record)

		payload, err := json.Marshal(record)
		if err != nil {
			return fmt.Errorf("marshal record: %w", err)
		}

		if err := p.publishWithRetry(ctx, payload); err != nil {
			return err
		}
		p.metrics.RecordLinePublished(ctx)
	}

	if p.watermarks != nil {
		if err := p.watermarks.Set(event.Path, event.Offset, event.Inode); err != nil {
			return fmt.Errorf("update watermark: %w", err)
		}
	}
	return nil
}

func (p *Pipeline) publishWithRetry(ctx context.Context, payload []byte) error {
	backoff := time.Second
	for {
		start := time.Now()
		err := p.sink.Publish(ctx, payload)
		p.metrics.RecordKafkaPublishDuration(ctx, time.Since(start))
		if err == nil {
			return nil
		}

		p.metrics.RecordPublishFailure(ctx)
		p.logger.Warn("kafka publish failed, retrying", "error", err, "retry_in", backoff)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
			p.metrics.RecordPublishRetry(ctx)
		}

		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}
