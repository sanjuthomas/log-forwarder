// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package pipeline

import (
	"context"
	"time"
)

func (p *Pipeline) Hibernating() bool {
	p.hibernateMu.RLock()
	defer p.hibernateMu.RUnlock()
	return p.hibernating
}

func (p *Pipeline) enterHibernate(_ context.Context, err error, batch []pendingPublish) {
	p.hibernateMu.Lock()
	if len(batch) > 0 {
		p.hibernateBatch = append([]pendingPublish(nil), batch...)
	}
	startWake := false
	if p.hibernating {
		p.hibernateDetail = err.Error()
		p.hibernateMu.Unlock()
		return
	}
	p.hibernating = true
	p.hibernateDetail = err.Error()
	if p.cfg.Pipeline.PublishBatch.Hibernate.WakeEnabled && !p.hibernateWakeRunning {
		p.hibernateWakeRunning = true
		startWake = true
	}
	p.hibernateMu.Unlock()

	p.logger.Error("sink hibernating after publish batch failure", "error", err)
	if startWake {
		go p.runHibernateWakeLoop()
	}
}

func (p *Pipeline) exitHibernate() {
	p.hibernateMu.Lock()
	defer p.hibernateMu.Unlock()
	if !p.hibernating {
		return
	}
	p.hibernating = false
	p.hibernateDetail = ""
	p.hibernateBatch = nil
	p.logger.Info("resumed publishing after hibernate")
}

func (p *Pipeline) hibernateBatchSnapshot() []pendingPublish {
	p.hibernateMu.RLock()
	defer p.hibernateMu.RUnlock()
	return append([]pendingPublish(nil), p.hibernateBatch...)
}

func (p *Pipeline) retryHibernateBatch(ctx context.Context) {
	if !p.Hibernating() {
		return
	}
	batch := p.hibernateBatchSnapshot()
	if len(batch) == 0 {
		return
	}
	_ = p.flushItems(ctx, batch, "wake")
}

func (p *Pipeline) runHibernateWakeLoop() {
	defer func() {
		p.hibernateMu.Lock()
		p.hibernateWakeRunning = false
		p.hibernateMu.Unlock()
	}()

	interval := p.cfg.Pipeline.PublishBatch.Hibernate.WakeIntervalDuration()
	if interval <= 0 {
		return
	}

	for {
		if !p.waitHibernateWake(interval) {
			return
		}
		if !p.Hibernating() {
			return
		}

		ctx := p.runCtx
		if ctx == nil {
			ctx = context.Background()
		}
		p.retryHibernateBatch(ctx)
		if !p.Hibernating() {
			return
		}
	}
}

func (p *Pipeline) waitHibernateWake(interval time.Duration) bool {
	runCtx := p.runCtx
	if runCtx == nil {
		runCtx = context.Background()
	}

	if p.hibernateWakeAfter != nil {
		select {
		case <-runCtx.Done():
			return false
		case <-p.hibernateWakeAfter(interval):
			return true
		}
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	select {
	case <-runCtx.Done():
		return false
	case <-ticker.C:
		return true
	}
}

func (p *Pipeline) waitWhileHibernating(ctx context.Context) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		if !p.Hibernating() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (p *Pipeline) HibernatingSnapshot() int64 {
	if p.Hibernating() {
		return 1
	}
	return 0
}
