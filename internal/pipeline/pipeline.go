package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/sanjuthomas/log-forwarder/internal/config"
	"github.com/sanjuthomas/log-forwarder/internal/enrich"
	"github.com/sanjuthomas/log-forwarder/internal/sink"
	"github.com/sanjuthomas/log-forwarder/internal/transform"
	"github.com/sanjuthomas/log-forwarder/internal/watcher"
)

// Pipeline processes log lines through transform, enrich, and publish stages.
type Pipeline struct {
	cfg         *config.Config
	transformer transform.Transformer
	enrichers   []enrich.Enricher
	sink        sink.Sink
	logger      *slog.Logger
}

func New(cfg *config.Config, s sink.Sink, logger *slog.Logger) (*Pipeline, error) {
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
				p.logger.Error("process line failed",
					"path", event.Path,
					"error", err,
				)
			}
		}
	}
}

func (p *Pipeline) process(ctx context.Context, event watcher.LineEvent) error {
	record, err := p.transformer.Transform(event.Line)
	if err != nil {
		switch p.cfg.Transform.OnError {
		case "skip":
			p.logger.Debug("skipping line", "path", event.Path, "error", err)
			return nil
		case "wrap":
			record = transform.Record{
				"_raw":  string(event.Line),
				"_path": event.Path,
				"_error": err.Error(),
			}
		default:
			return err
		}
	}

	record["_path"] = event.Path
	record = enrich.Apply(p.enrichers, record)

	payload, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshal record: %w", err)
	}

	return p.sink.Publish(ctx, payload)
}
