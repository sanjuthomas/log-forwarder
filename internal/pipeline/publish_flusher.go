package pipeline

import (
	"context"
	"sync"
)

type publishFlusher struct {
	p        *Pipeline
	mu       sync.Mutex
	cond     *sync.Cond
	maxBytes int

	active publishBuffer

	flushRunning bool
	flushDone    chan struct{}
	flushErr      error
	shuttingDown  bool
}

func newPublishFlusher(p *Pipeline) *publishFlusher {
	f := &publishFlusher{
		p:        p,
		maxBytes: p.cfg.Pipeline.PublishBatch.MaxBytesLimit(),
	}
	f.cond = sync.NewCond(&f.mu)
	return f
}

func (f *publishFlusher) activeBytes() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return int64(f.active.bufferedBytes())
}

func (f *publishFlusher) enqueue(ctx context.Context, item pendingPublish) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	for {
		if f.shuttingDown {
			return ctx.Err()
		}
		if f.p.Hibernating() {
			if err := f.p.waitWhileHibernating(ctx); err != nil {
				return err
			}
			continue
		}

		maxBytes := f.maxBytes

		if maxBytes > 0 && len(item.payload) > maxBytes {
			if err := f.swapAndStartFlushLocked(ctx, "size"); err != nil {
				return err
			}
			f.active.append(item)
			return f.waitForFlushLocked(ctx)
		}

		if maxBytes > 0 && f.active.bytes > 0 && f.active.bytes+len(item.payload) > maxBytes {
			if err := f.swapAndStartFlushLocked(ctx, "size"); err != nil {
				return err
			}
			continue
		}

		f.active.append(item)
		return nil
	}
}

func (f *publishFlusher) triggerFlush(ctx context.Context, reason string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.p.Hibernating() {
		return nil
	}
	if f.active.len() == 0 {
		return nil
	}
	return f.swapAndStartFlushLocked(ctx, reason)
}

func (f *publishFlusher) shutdown(ctx context.Context) error {
	f.mu.Lock()
	f.shuttingDown = true
	f.mu.Unlock()
	f.cond.Broadcast()

	if err := f.syncFlush(ctx, "shutdown"); err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	return f.flushErr
}

func (f *publishFlusher) syncFlush(ctx context.Context, reason string) error {
	for {
		f.mu.Lock()
		if err := f.waitForFlushLocked(ctx); err != nil {
			f.mu.Unlock()
			return err
		}
		if f.active.len() == 0 {
			f.mu.Unlock()
			return nil
		}

		f.flushErr = nil
		batch := f.active.drain()
		f.flushRunning = true
		f.flushDone = make(chan struct{})
		done := f.flushDone
		f.mu.Unlock()

		err := f.p.flushItems(ctx, batch, reason)

		f.mu.Lock()
		f.flushRunning = false
		f.flushErr = err
		close(done)
		f.cond.Broadcast()
		f.mu.Unlock()

		if err != nil {
			return err
		}
	}
}

func (f *publishFlusher) swapAndStartFlushLocked(ctx context.Context, reason string) error {
	if err := f.waitForFlushLocked(ctx); err != nil {
		return err
	}
	if f.active.len() == 0 {
		return nil
	}

	f.flushErr = nil
	batch := f.active.drain()
	f.flushRunning = true
	f.flushDone = make(chan struct{})
	done := f.flushDone
	f.mu.Unlock()

	go f.runFlush(batch, reason, done)

	f.mu.Lock()
	return nil
}

func (f *publishFlusher) runFlush(batch []pendingPublish, reason string, done chan struct{}) {
	ctx := f.p.runCtx
	if ctx == nil {
		ctx = context.Background()
	}
	err := f.p.flushItems(ctx, batch, reason)

	f.mu.Lock()
	f.flushRunning = false
	f.flushErr = err
	close(done)
	f.cond.Broadcast()
	f.mu.Unlock()
}

func (f *publishFlusher) waitForFlushLocked(ctx context.Context) error {
	for f.flushRunning {
		done := f.flushDone
		f.mu.Unlock()
		select {
		case <-ctx.Done():
			f.mu.Lock()
			return ctx.Err()
		case <-done:
			f.mu.Lock()
			if f.flushErr != nil {
				return f.flushErr
			}
		}
	}
	return nil
}
