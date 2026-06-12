// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package pipeline

import (
	"context"
	"io"
	"log/slog"
	"strconv"
	"testing"
	"time"

	"github.com/sanjuthomas/log-forwarder/internal/config"
	"github.com/sanjuthomas/log-forwarder/internal/watcher"
)

func springBootLineEvent(n int, offset int64) watcher.LineEvent {
	line := []byte("2026-06-08 10:16:2" + strconv.Itoa(n%10) + ".901  INFO 18432 --- [main] c.example.App : line-" + strconv.Itoa(n))
	return watcher.LineEvent{
		Path:   "/tmp/app.log",
		Line:   line,
		Offset: offset,
		Inode:  1,
	}
}

func TestPipelineMultilineIdleFlushPublishesTailRecord(t *testing.T) {
	cfg := config.Default()
	cfg.Parser = config.ParserConfig{
		Type:          "multiline",
		StartPattern:  `^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}`,
		FlushInterval: "50ms",
	}
	cfg.Transform = config.TransformConfig{
		Type:    "regex",
		Pattern: `^(?s)(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3})\s+(?P<level>\S+)\s+(?P<pid>\d+)\s+---\s+\[\s*(?P<thread>[^\]]+?)\s*\]\s+(?P<logger>\S+)\s+:\s+(?P<message>.*)$`,
		OnError: "wrap",
	}
	disablePublishBatch(cfg)

	sink := &batchCapturingSink{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	pipe, err := New(cfg, sink, logger, Options{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	lines := make(chan watcher.LineEvent, 8)
	var offset int64
	for i := 1; i <= 7; i++ {
		event := springBootLineEvent(i, offset+int64(len(springBootLineEvent(i, 0).Line))+1)
		offset = event.Offset
		lines <- event
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- pipe.Run(ctx, lines)
	}()

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if sink.recordCount() == 7 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := sink.recordCount(); got != 7 {
		t.Fatalf("recordCount = %d, want 7 after multiline idle flush", got)
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run() error = %v", err)
	}
}

func TestPipelineMultilineIdleFlushDisabled(t *testing.T) {
	cfg := config.Default()
	cfg.Parser = config.ParserConfig{
		Type:          "multiline",
		StartPattern:  `^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}`,
		FlushInterval: "0",
	}
	cfg.Transform = config.TransformConfig{
		Type:    "regex",
		Pattern: `^(?s)(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3})\s+(?P<level>\S+)\s+(?P<pid>\d+)\s+---\s+\[\s*(?P<thread>[^\]]+?)\s*\]\s+(?P<logger>\S+)\s+:\s+(?P<message>.*)$`,
		OnError: "wrap",
	}
	disablePublishBatch(cfg)

	sink := &batchCapturingSink{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	pipe, err := New(cfg, sink, logger, Options{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	lines := make(chan watcher.LineEvent, 8)
	var offset int64
	for i := 1; i <= 7; i++ {
		event := springBootLineEvent(i, offset+int64(len(springBootLineEvent(i, 0).Line))+1)
		offset = event.Offset
		lines <- event
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- pipe.Run(ctx, lines)
	}()

	time.Sleep(150 * time.Millisecond)
	if got := sink.recordCount(); got != 6 {
		t.Fatalf("recordCount = %d, want 6 when parser idle flush is disabled", got)
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if got := sink.recordCount(); got != 7 {
		t.Fatalf("recordCount after shutdown = %d, want 7", got)
	}
}
