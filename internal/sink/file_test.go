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
