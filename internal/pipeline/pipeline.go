package pipeline

import (
	"context"
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
	"github.com/sanjuthomas/log-forwarder/internal/timestamp"
	"github.com/sanjuthomas/log-forwarder/internal/transform"
	"github.com/sanjuthomas/log-forwarder/internal/watcher"
)

// Pipeline processes log lines through transform, enrich, and publish stages.
type Pipeline struct {
	cfg          *config.Config
	parser       parser.Parser
	transformer  transform.Transformer
	normalizer   *timestamp.Normalizer
	filter       filter.Predicate
	enrichers    []enrich.Enricher
	sink         sink.Sink
	watermarks   *state.Store
	metrics      *metrics.Collector
	logger       *slog.Logger
	batchEnabled bool
	flusher      *publishFlusher
	runCtx       context.Context
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
	normalizer, err := timestamp.New(cfg.Timestamp)
	if err != nil {
		return nil, err
	}
	f, err := filter.New(cfg.Filter)
	if err != nil {
		return nil, err
	}

	batchEnabled := cfg.Pipeline.PublishBatch.Enabled()

	pipe := &Pipeline{
		cfg:          cfg,
		parser:       p,
		transformer:  t,
		normalizer:   normalizer,
		filter:       f,
		enrichers:    chain,
		sink:         s,
		watermarks:   opts.Watermarks,
		metrics:      opts.Metrics,
		logger:       logger,
		batchEnabled: batchEnabled,
	}
	if batchEnabled {
		pipe.flusher = newPublishFlusher(pipe)
	}
	return pipe, nil
}

func (p *Pipeline) PublishBufferActiveBytes() int64 {
	if p.flusher == nil {
		return 0
	}
	return p.flusher.activeBytes()
}

func (p *Pipeline) Run(ctx context.Context, lines <-chan watcher.LineEvent) error {
	p.runCtx = ctx
	defer func() { p.runCtx = nil }()

	var flushTick <-chan time.Time
	if p.batchEnabled {
		if interval := p.cfg.Pipeline.PublishBatch.FlushIntervalDuration(); interval > 0 {
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			flushTick = ticker.C
		}
	}

	for {
		select {
		case <-ctx.Done():
			return p.shutdown(ctx)
		case <-flushTick:
			if err := p.flushPublishBuffer(ctx, "timer"); err != nil {
				return err
			}
		case event, ok := <-lines:
			if !ok {
				return p.shutdown(ctx)
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

func (p *Pipeline) shutdown(ctx context.Context) error {
	if err := p.flushParser(ctx); err != nil {
		return err
	}
	if p.flusher != nil {
		return p.flusher.shutdown(ctx)
	}
	return nil
}

func (p *Pipeline) flushParser(ctx context.Context) error {
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
		if p.normalizer != nil {
			if failed := p.normalizer.Normalize(record); failed {
				p.metrics.RecordTimestampParseFailure(ctx)
				p.logger.Debug("timestamp parse failed, using processing time",
					"path", event.Path,
					"field", p.cfg.Timestamp.FieldOrDefault(),
				)
			}
		}
		if !p.filter.Match(record) {
			p.metrics.RecordLineFiltered(ctx)
		} else {
			record = enrich.Apply(p.enrichers, record)

			payload, truncated, err := marshalPublishPayload(
				record,
				p.cfg.Pipeline.MaxPublishBytes,
				p.cfg.Pipeline.TruncateFieldOrDefault(),
				p.cfg.Pipeline.TruncateSuffixOrDefault(),
			)
			if err != nil {
				return err
			}
			if truncated {
				p.metrics.RecordPublishTruncation(ctx)
				p.logger.Debug("truncated record for publish",
					"path", event.Path,
					"max_publish_bytes", p.cfg.Pipeline.MaxPublishBytes,
					"payload_bytes", len(payload),
				)
			}

			if err := p.enqueuePublish(ctx, pendingPublish{
				payload: payload,
				path:    event.Path,
				offset:  event.Offset,
				inode:   event.Inode,
			}); err != nil {
				return err
			}
			return nil
		}
	}

	if p.watermarks != nil {
		if err := p.watermarks.Set(event.Path, event.Offset, event.Inode); err != nil {
			return fmt.Errorf("update watermark: %w", err)
		}
	}
	return nil
}
