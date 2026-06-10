package pipeline

import (
	"context"
	"fmt"

	"github.com/sanjuthomas/log-forwarder/internal/config"
	"github.com/sanjuthomas/log-forwarder/internal/deadletter"
)

func (p *Pipeline) handleDeadLetterFlush(ctx context.Context, items []pendingPublish, publishErr error, reason string, batchBytes int) error {
	payloads := make([][]byte, len(items))
	for i, item := range items {
		payloads[i] = item.payload
	}

	dlqPath := p.cfg.Pipeline.PublishBatch.DeadLetter.Path
	filename, writtenBytes, err := deadletter.WriteBatch(dlqPath, payloads, deadletter.WriteInfo{
		FailureReason: publishErr.Error(),
		SinkType:      p.cfg.Sink.Type,
		BatchAttempts: p.cfg.Pipeline.PublishBatch.MaxAttemptsOrDefault(p.cfg.Pipeline.PublishRetry),
	})
	if err != nil {
		p.metrics.RecordPublishBatchFlush(ctx, reason, "error", len(items), batchBytes)
		return fmt.Errorf("write dead letter batch: %w", err)
	}

	if err := p.advanceWatermarks(ctx, items); err != nil {
		return err
	}

	consecutive := p.incrementConsecutiveDLQ()
	p.metrics.RecordDeadLetterBatch(ctx, writtenBytes)
	p.metrics.RecordPublishBatchFlush(ctx, reason, "dead_letter", len(items), batchBytes)
	p.logger.Warn("wrote failed publish batch to dead letter",
		"path", dlqPath,
		"filename", filename,
		"records", len(items),
		"publish_error", publishErr,
		"consecutive_dlq_batches", consecutive,
	)

	maxConsecutive := p.cfg.Pipeline.PublishBatch.DeadLetter.MaxConsecutiveBatchesOrDefault()
	if consecutive >= maxConsecutive {
		p.enterHibernate(ctx, fmt.Errorf(
			"dead letter consecutive batch limit %d reached after publish failure: %w",
			maxConsecutive, publishErr,
		), nil)
		p.metrics.RecordPublishBatchFlush(ctx, reason, "hibernate", len(items), batchBytes)
	}
	return nil
}

func (p *Pipeline) advanceWatermarks(ctx context.Context, items []pendingPublish) error {
	for _, item := range items {
		if p.watermarks != nil {
			if err := p.watermarks.Set(item.path, item.offset, item.inode); err != nil {
				return fmt.Errorf("update watermark: %w", err)
			}
		}
	}
	return nil
}

func (p *Pipeline) resetConsecutiveDLQ() {
	p.dlqMu.Lock()
	p.consecutiveDLQ = 0
	p.dlqMu.Unlock()
}

func (p *Pipeline) incrementConsecutiveDLQ() int {
	p.dlqMu.Lock()
	defer p.dlqMu.Unlock()
	p.consecutiveDLQ++
	return p.consecutiveDLQ
}

func (p *Pipeline) ConsecutiveDLQSnapshot() int64 {
	p.dlqMu.Lock()
	defer p.dlqMu.Unlock()
	return int64(p.consecutiveDLQ)
}

func (p *Pipeline) deadLetterEnabled() bool {
	return p.batchEnabled &&
		p.cfg.Pipeline.PublishBatch.OnFlushFailureOrDefault() == config.OnFlushFailureDeadLetter
}
