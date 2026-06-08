package logging

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/sanjuthomas/log-forwarder/internal/config"
)

type noopCloser struct{}

func (noopCloser) Close() error { return nil }

func New(cfg config.LoggingConfig) (*slog.Logger, io.Closer, error) {
	level := slog.LevelInfo
	switch cfg.Level {
	case "", "info":
		level = slog.LevelInfo
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}

	var (
		out    io.Writer = os.Stderr
		closer io.Closer = noopCloser{}
	)
	if cfg.File != "" {
		if err := os.MkdirAll(filepath.Dir(cfg.File), 0o755); err != nil {
			return nil, nil, err
		}
		f, err := os.OpenFile(cfg.File, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			return nil, nil, err
		}
		out = f
		closer = f
	}

	opts := &slog.HandlerOptions{Level: level}
	var handler slog.Handler
	if cfg.Format == "json" {
		handler = slog.NewJSONHandler(out, opts)
	} else {
		handler = slog.NewTextHandler(out, opts)
	}

	return slog.New(handler), closer, nil
}
