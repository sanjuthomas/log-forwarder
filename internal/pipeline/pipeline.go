package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/sanjuthomas/log-forwarder/internal/config"
	"github.com/sanjuthomas/log-forwarder/internal/enrich"
	"github.com/sanjuthomas/log-forwarder/internal/runtime"
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
	stats       *runtime.Stats
	logger      *slog.Logger
}

type Options struct {
	Watermarks *state.Store
	Stats      *runtime.Stats
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
	stats := opts.Stats
	if stats == nil {
		stats = &runtime.Stats{}
	}
	return &Pipeline{
		cfg:         cfg,
		transformer: t,
		enrichers:   chain,
		sink:        s,
		watermarks:  opts.Watermarks,
		stats:       stats,
		logger:      logger,
	}, nil
}

func (p *Pipeline) Stats() *runtime.Stats {
	return p.stats
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
		switch p.cfg.Transform.OnError {
		case "skip":
			p.logger.Debug("skipping line", "path", event.Path, "error", err)
			skipPublish = true
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
		p.stats.LinesPublished.Add(1)
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
		err := p.sink.Publish(ctx, payload)
		if err == nil {
			return nil
		}

		p.stats.PublishFailures.Add(1)
		p.logger.Warn("kafka publish failed, retrying", "error", err, "retry_in", backoff)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}

		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}
