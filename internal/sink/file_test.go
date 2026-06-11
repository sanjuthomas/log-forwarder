// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package sink

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/sanjuthomas/log-forwarder/internal/config"
)

func TestFileSinkWritesJSONL(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "out", "records.jsonl")

	s, err := New(config.SinkConfig{
		Type: "file",
		File: &config.FileSinkConfig{Path: path},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })

	if err := s.Publish(context.Background(), []byte(`{"level":"INFO"}`)); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	want := "{\"level\":\"INFO\"}\n"
	if string(data) != want {
		t.Fatalf("file contents = %q, want %q", string(data), want)
	}
}

func TestFileSinkPublishBatchWritesJSONL(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "out", "records.jsonl")

	s, err := New(config.SinkConfig{
		Type: "file",
		File: &config.FileSinkConfig{Path: path},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })

	batchSink, ok := s.(BatchSink)
	if !ok {
		t.Fatal("expected file sink to implement BatchSink")
	}
	if err := batchSink.PublishBatch(context.Background(), [][]byte{
		[]byte(`{"level":"INFO","message":"one"}`),
		[]byte(`{"level":"ERROR","message":"two"}`),
	}); err != nil {
		t.Fatalf("PublishBatch() error = %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	want := "{\"level\":\"INFO\",\"message\":\"one\"}\n{\"level\":\"ERROR\",\"message\":\"two\"}\n"
	if string(data) != want {
		t.Fatalf("file contents = %q, want %q", string(data), want)
	}
}
