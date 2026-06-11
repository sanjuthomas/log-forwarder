// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPollInterval(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.Watch.Poll = "250ms"
	if got := cfg.PollInterval(); got != 250*time.Millisecond {
		t.Fatalf("PollInterval() = %v, want 250ms", got)
	}
}

func TestLoggingStatusIntervalDuration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want time.Duration
	}{
		{name: "empty", in: "", want: 0},
		{name: "zero", in: "0", want: 0},
		{name: "valid", in: "45s", want: 45 * time.Second},
		{name: "invalid", in: "not-a-duration", want: 30 * time.Second},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := LoggingConfig{StatusInterval: tc.in}.StatusIntervalDuration()
			if got != tc.want {
				t.Fatalf("StatusIntervalDuration() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestValidateTransformOnError(t *testing.T) {
	t.Parallel()

	cfg := validConfigFromRegistryTest()
	cfg.Transform.OnError = "drop"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error for invalid transform.on_error")
	}
}

func TestValidateLoggingLevelAndFormat(t *testing.T) {
	t.Parallel()

	cfg := validConfigFromRegistryTest()
	cfg.Logging.Level = "trace"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error for invalid logging.level")
	}

	cfg = validConfigFromRegistryTest()
	cfg.Logging.Format = "xml"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error for invalid logging.format")
	}
}

func TestValidateLoggingStatusInterval(t *testing.T) {
	t.Parallel()

	cfg := validConfigFromRegistryTest()
	cfg.Logging.StatusInterval = "not-a-duration"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error for invalid logging.status_interval")
	}
}

func TestLoadValidConfigFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	content := []byte(`
watch:
  paths: ["./logs"]
  patterns: ["*.log"]
  poll: 1s
sink:
  type: file
  file:
    path: ./output/logs.jsonl
transform:
  type: tab
  on_error: wrap
pipeline:
  buffer_size: 128
  on_full: block
`)
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Sink.Type != "file" {
		t.Fatalf("sink.type = %q, want file", cfg.Sink.Type)
	}
	if cfg.Pipeline.BufferSize != 128 {
		t.Fatalf("buffer_size = %d, want 128", cfg.Pipeline.BufferSize)
	}
}

func TestLoadMissingFile(t *testing.T) {
	t.Parallel()

	_, err := Load(filepath.Join(t.TempDir(), "missing.yaml"))
	if err == nil {
		t.Fatal("expected error for missing config file")
	}
}

func TestValidateDeadLetterAtStartupSkipsWhenHibernate(t *testing.T) {
	t.Parallel()

	cfg := validConfigFromRegistryTest()
	cfg.Pipeline.PublishBatch.OnFlushFailure = OnFlushFailureHibernate
	if err := cfg.ValidateDeadLetterAtStartup(); err != nil {
		t.Fatalf("ValidateDeadLetterAtStartup() error = %v", err)
	}
}

func TestValidateDeadLetterAtStartupRequiresPath(t *testing.T) {
	t.Parallel()

	cfg := validConfigFromRegistryTest()
	cfg.Pipeline.PublishBatch.OnFlushFailure = OnFlushFailureDeadLetter
	if err := cfg.ValidateDeadLetterAtStartup(); err == nil {
		t.Fatal("expected error when dead_letter.path is missing")
	}
}

func TestValidateDeadLetterPathOutsideWatchDir(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cfg := validConfigFromRegistryTest()
	cfg.Watch.Paths = []string{filepath.Join(dir, "logs")}
	cfg.Watch.Patterns = []string{"*.log"}
	if err := validateDeadLetterPath(cfg, filepath.Join(dir, "dlq")); err != nil {
		t.Fatalf("validateDeadLetterPath() error = %v", err)
	}
}

func TestValidateDeadLetterPathInsideWatchDir(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	watchDir := filepath.Join(dir, "logs")
	cfg := validConfigFromRegistryTest()
	cfg.Watch.Sources = []WatchSource{
		{Path: watchDir, Patterns: []string{"*.log"}},
	}
	if err := validateDeadLetterPath(cfg, filepath.Join(watchDir, "dlq")); err == nil {
		t.Fatal("expected error when dead letter path is inside watched directory")
	}
}

func TestValidateDeadLetterAtStartupWritablePath(t *testing.T) {
	t.Parallel()

	cfg := validConfigFromRegistryTest()
	cfg.Pipeline.PublishBatch.OnFlushFailure = OnFlushFailureDeadLetter
	cfg.Pipeline.PublishBatch.DeadLetter.Path = filepath.Join(t.TempDir(), "dlq")
	if err := cfg.ValidateDeadLetterAtStartup(); err != nil {
		t.Fatalf("ValidateDeadLetterAtStartup() error = %v", err)
	}
}

func validConfigFromRegistryTest() *Config {
	return validConfig()
}
