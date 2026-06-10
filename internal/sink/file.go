package sink

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/sanjuthomas/log-forwarder/internal/config"
)

type fileSink struct {
	path string
	mu   sync.Mutex
	file *os.File
}

func newFileSink(cfg config.SinkConfig) (Sink, error) {
	if cfg.File == nil || cfg.File.Path == "" {
		return nil, fmt.Errorf("sink.file.path is required")
	}
	return &fileSink{path: cfg.File.Path}, nil
}

func (f *fileSink) Check(_ context.Context) error {
	return f.open()
}

func (f *fileSink) Publish(ctx context.Context, payload []byte) error {
	return f.PublishBatch(ctx, [][]byte{payload})
}

func (f *fileSink) PublishBatch(_ context.Context, payloads [][]byte) error {
	if len(payloads) == 0 {
		return nil
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.file == nil {
		if err := f.openLocked(); err != nil {
			return err
		}
	}

	for _, payload := range payloads {
		if _, err := f.file.Write(payload); err != nil {
			return fmt.Errorf("file publish: %w", err)
		}
		if _, err := f.file.Write([]byte("\n")); err != nil {
			return fmt.Errorf("file publish: %w", err)
		}
	}
	return nil
}

func (f *fileSink) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.file == nil {
		return nil
	}
	err := f.file.Close()
	f.file = nil
	return err
}

func (f *fileSink) open() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.openLocked()
}

func (f *fileSink) openLocked() error {
	if f.file != nil {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(f.path), 0o755); err != nil {
		return fmt.Errorf("create sink file directory: %w", err)
	}
	file, err := os.OpenFile(f.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open sink file: %w", err)
	}
	f.file = file
	return nil
}

func init() {
	Register("file", newFileSink)
}
