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

func (p *Pipeline) enterHibernate(_ context.Context, err error) {
	p.hibernateMu.Lock()
	if p.hibernating {
		p.hibernateMu.Unlock()
		return
	}
	p.hibernating = true
	p.hibernateDetail = err.Error()
	p.hibernateMu.Unlock()

	p.logger.Error("sink hibernating after publish batch failure", "error", err)
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
