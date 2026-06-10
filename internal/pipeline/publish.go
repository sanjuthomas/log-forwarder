package pipeline

import (
	"context"
	"fmt"
	"time"

	"github.com/sanjuthomas/log-forwarder/internal/config"
	"github.com/sanjuthomas/log-forwarder/internal/sink"
)

func publishToSink(ctx context.Context, s sink.Sink, payloads [][]byte) error {
	if len(payloads) == 0 {
		return nil
	}
	if bs, ok := s.(sink.BatchSink); ok {
		return bs.PublishBatch(ctx, payloads)
	}
	for _, payload := range payloads {
		if err := s.Publish(ctx, payload); err != nil {
			return err
		}
	}
	return nil
}

func (p *Pipeline) enqueuePublish(ctx context.Context, item pendingPublish) error {
	if !p.batchEnabled {
		return p.publishAndAdvance(ctx, []pendingPublish{item})
	}
	return p.flusher.enqueue(ctx, item)
}

func (p *Pipeline) flushPublishBuffer(ctx context.Context, reason string) error {
	if p.flusher == nil {
		return nil
	}
	return p.flusher.triggerFlush(ctx, reason)
}

func (p *Pipeline) flushItems(ctx context.Context, items []pendingPublish, reason string) error {
	if len(items) == 0 {
		return nil
	}

	batchBytes := 0
	for _, item := range items {
		batchBytes += len(item.payload)
	}

	if err := p.publishAndAdvance(ctx, items); err != nil {
		if p.batchEnabled && p.cfg.Pipeline.PublishBatch.OnFlushFailureOrDefault() == config.OnFlushFailureHibernate {
			p.enterHibernate(ctx, err)
			p.metrics.RecordPublishBatchFlush(ctx, reason, "hibernate", len(items), batchBytes)
			return nil
		}
		p.metrics.RecordPublishBatchFlush(ctx, reason, "error", len(items), batchBytes)
		return err
	}

	p.metrics.RecordPublishBatchFlush(ctx, reason, "success", len(items), batchBytes)
	return nil
}

func (p *Pipeline) publishAndAdvance(ctx context.Context, items []pendingPublish) error {
	if len(items) == 0 {
		return nil
	}

	payloads := make([][]byte, len(items))
	for i, item := range items {
		payloads[i] = item.payload
	}
	if err := p.publishBatchWithRetry(ctx, payloads); err != nil {
		return err
	}

	for _, item := range items {
		p.metrics.RecordLinePublished(ctx)
		if p.watermarks != nil {
			if err := p.watermarks.Set(item.path, item.offset, item.inode); err != nil {
				return fmt.Errorf("update watermark: %w", err)
			}
		}
	}
	return nil
}

func (p *Pipeline) publishBatchWithRetry(ctx context.Context, payloads [][]byte) error {
	retry := p.cfg.Pipeline.PublishRetry
	backoff := retry.InitialBackoffDuration()
	maxBackoff := retry.MaxBackoffDuration()
	maxAttempts := p.cfg.Pipeline.PublishBatch.MaxAttemptsOrDefault(retry)
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
		err := publishToSink(publishCtx, p.sink, payloads)
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
			"batch_size", len(payloads),
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
