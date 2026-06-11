// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package pipeline

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/sanjuthomas/log-forwarder/internal/config"
	"github.com/sanjuthomas/log-forwarder/internal/state"
	"github.com/sanjuthomas/log-forwarder/internal/watcher"
)

func TestPipelineWatermarkNotAdvancedWhenPublishFails(t *testing.T) {
	watermarks, err := state.NewStore(filepath.Join(t.TempDir(), "watermarks.json"))
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}

	cfg := config.Default()
	cfg.Transform = config.TransformConfig{
		Type:    "delimiter",
		Columns: []string{"timestamp", "level", "message"},
		OnError: "wrap",
	}
	cfg.Pipeline.PublishRetry = config.PublishRetryConfig{
		InitialBackoff: "1ms",
		MaxBackoff:     "5ms",
		MaxAttempts:    2,
	}
	disablePublishBatch(cfg)

	const path = "/tmp/test.log"
	sink := &flakySink{failures: 10}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	pipe, err := New(cfg, sink, logger, Options{Watermarks: watermarks})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	lines := make(chan watcher.LineEvent, 1)
	lines <- watcher.LineEvent{
		Path:   path,
		Line:   []byte("2024-01-01T00:00:00Z\tINFO\thello"),
		Offset: 42,
		Inode:  9,
	}
	close(lines)

	if err := pipe.Run(context.Background(), lines); err == nil {
		t.Fatal("expected error when publish retries are exhausted")
	}
	if _, ok := watermarks.Get(path); ok {
		t.Fatal("watermark must not advance when publish fails")
	}
}

func TestPipelineWatermarkAdvancedAfterPublishRetrySucceeds(t *testing.T) {
	watermarks, err := state.NewStore(filepath.Join(t.TempDir(), "watermarks.json"))
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}

	cfg := config.Default()
	cfg.Transform = config.TransformConfig{
		Type:    "delimiter",
		Columns: []string{"timestamp", "level", "message"},
		OnError: "wrap",
	}
	cfg.Pipeline.PublishRetry = config.PublishRetryConfig{
		InitialBackoff: "10ms",
		MaxBackoff:     "50ms",
		MaxAttempts:    0,
	}

	const path = "/tmp/test.log"
	sink := &flakySink{failures: 2}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	pipe, err := New(cfg, sink, logger, Options{Watermarks: watermarks})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	lines := make(chan watcher.LineEvent, 1)
	lines <- watcher.LineEvent{
		Path:   path,
		Line:   []byte("2024-01-01T00:00:00Z\tINFO\thello"),
		Offset: 42,
		Inode:  9,
	}
	close(lines)

	if err := pipe.Run(context.Background(), lines); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	entry, ok := watermarks.Get(path)
	if !ok {
		t.Fatal("expected watermark after successful publish")
	}
	if entry.Offset != 42 || entry.Inode != 9 {
		t.Fatalf("watermark = %+v, want offset 42 inode 9", entry)
	}
}

func TestPipelineWatermarkStallsOnSecondLineUntilFirstLinePublishes(t *testing.T) {
	watermarks, err := state.NewStore(filepath.Join(t.TempDir(), "watermarks.json"))
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}

	cfg := config.Default()
	cfg.Transform = config.TransformConfig{
		Type:    "delimiter",
		Columns: []string{"timestamp", "level", "message"},
		OnError: "wrap",
	}
	cfg.Pipeline.PublishRetry = config.PublishRetryConfig{
		InitialBackoff: "1ms",
		MaxBackoff:     "5ms",
		MaxAttempts:    2,
	}
	disablePublishBatch(cfg)

	const path = "/tmp/test.log"
	sink := &flakySink{failures: 10}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	pipe, err := New(cfg, sink, logger, Options{Watermarks: watermarks})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	lines := make(chan watcher.LineEvent, 2)
	lines <- watcher.LineEvent{
		Path:   path,
		Line:   []byte("2024-01-01T00:00:00Z\tINFO\tfirst"),
		Offset: 10,
		Inode:  1,
	}
	lines <- watcher.LineEvent{
		Path:   path,
		Line:   []byte("2024-01-01T00:00:01Z\tINFO\tsecond"),
		Offset: 30,
		Inode:  1,
	}
	close(lines)

	if err := pipe.Run(context.Background(), lines); err == nil {
		t.Fatal("expected error when first line publish retries are exhausted")
	}
	if entry, ok := watermarks.Get(path); ok {
		t.Fatalf("watermark must not advance when publish fails, got %+v", entry)
	}
}
