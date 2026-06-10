package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/sanjuthomas/log-forwarder/internal/config"
	"github.com/sanjuthomas/log-forwarder/internal/enrich"
	"github.com/sanjuthomas/log-forwarder/internal/filter"
	"github.com/sanjuthomas/log-forwarder/internal/metrics"
	"github.com/sanjuthomas/log-forwarder/internal/parser"
	"github.com/sanjuthomas/log-forwarder/internal/sink"
	"github.com/sanjuthomas/log-forwarder/internal/state"
	"github.com/sanjuthomas/log-forwarder/internal/transform"
	"github.com/sanjuthomas/log-forwarder/internal/watcher"
)

// Pipeline processes log lines through transform, enrich, and publish stages.
type Pipeline struct {
	cfg         *config.Config
	parser      parser.Parser
	transformer transform.Transformer
	filter      filter.Predicate
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
	p, err := parser.New(cfg.Parser)
	if err != nil {
		return nil, err
	}
	t, err := transform.New(cfg.Transform)
	if err != nil {
		return nil, err
	}
	chain, err := enrich.NewChain(cfg.Enrichers)
	if err != nil {
		return nil, err
	}
	f, err := filter.New(cfg.Filter)
	if err != nil {
		return nil, err
	}
	return &Pipeline{
		cfg:         cfg,
		parser:      p,
		transformer: t,
		filter:      f,
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
			return p.flush(ctx)
		case event, ok := <-lines:
			if !ok {
				return p.flush(ctx)
			}
			records, err := p.parser.Feed(event)
			if err != nil {
				return err
			}
			for _, record := range records {
				if err := p.process(ctx, record); err != nil {
					return err
				}
			}
		}
	}
}

func (p *Pipeline) flush(ctx context.Context) error {
	records, err := p.parser.Flush()
	if err != nil {
		return err
	}
	for _, record := range records {
		if err := p.process(ctx, record); err != nil {
			return err
		}
	}
	return nil
}

func (p *Pipeline) process(ctx context.Context, event parser.Event) error {
	record, err := p.transformer.Transform(event.Data)
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
				"_raw":   string(event.Data),
				"_path":  event.Path,
				"_error": err.Error(),
			}
		default:
			return err
		}
	}

	if !skipPublish {
		record["_path"] = event.Path
		if !p.filter.Match(record) {
			p.metrics.RecordLineFiltered(ctx)
		} else {
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
	}

	if p.watermarks != nil {
		if err := p.watermarks.Set(event.Path, event.Offset, event.Inode); err != nil {
			return fmt.Errorf("update watermark: %w", err)
		}
	}
	return nil
}

func (p *Pipeline) publishWithRetry(ctx context.Context, payload []byte) error {
	retry := p.cfg.Pipeline.PublishRetry
	backoff := retry.InitialBackoffDuration()
	maxBackoff := retry.MaxBackoffDuration()
	maxAttempts := retry.MaxAttempts
	publishTimeout := p.cfg.Pipeline.PublishTimeoutDuration()

	attempt := 0
	for {
		attempt++

		publishCtx := ctx
		var cancel context.CancelFunc
		if publishTimeout > 0 {
			publishCtx, cancel = context.WithTimeout(ctx, publishTimeout)
		}

		start := time.Now()
		err := p.sink.Publish(publishCtx, payload)
		if cancel != nil {
			cancel()
		}
		p.metrics.RecordPublishDuration(ctx, time.Since(start))
		if err == nil {
			return nil
		}

		p.metrics.RecordPublishFailure(ctx)

		if maxAttempts > 0 && attempt >= maxAttempts {
			return fmt.Errorf("publish failed after %d attempts: %w", attempt, err)
		}

		p.logger.Warn("publish failed, retrying",
			"error", err,
			"attempt", attempt,
			"retry_in", backoff,
		)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
			p.metrics.RecordPublishRetry(ctx)
		}

		if backoff < maxBackoff {
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}
