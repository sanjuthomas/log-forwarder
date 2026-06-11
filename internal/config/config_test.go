// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package config

import (
	"path/filepath"
	"testing"
)

func TestWatchConfigEntriesFromSources(t *testing.T) {
	t.Parallel()

	cfg := WatchConfig{
		Sources: []WatchSource{
			{Path: "./logs/nginx", Patterns: []string{"*.log"}},
			{Path: "./logs/app", Patterns: []string{"*.log", "*.out"}},
		},
	}

	entries := cfg.Entries()
	if len(entries) != 2 {
		t.Fatalf("len(entries) = %d, want 2", len(entries))
	}
	if entries[0].Path != "./logs/nginx" {
		t.Fatalf("entries[0].path = %q", entries[0].Path)
	}
	if len(entries[0].Patterns) != 1 || entries[0].Patterns[0] != "*.log" {
		t.Fatalf("entries[0].patterns = %v", entries[0].Patterns)
	}
}

func TestWatchConfigEntriesFromPathsAndPatterns(t *testing.T) {
	t.Parallel()

	cfg := WatchConfig{
		Paths:    []string{"./a", "./b"},
		Patterns: []string{"*.log", "*.out"},
	}

	entries := cfg.Entries()
	if len(entries) != 2 {
		t.Fatalf("len(entries) = %d, want 2", len(entries))
	}
	if len(entries[0].Patterns) != 2 || len(entries[1].Patterns) != 2 {
		t.Fatalf("expected shared patterns on each path, got %+v", entries)
	}
}

func TestValidateWatchSources(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		Watch: WatchConfig{
			Sources: []WatchSource{
				{Path: "./logs/app", Patterns: []string{"*.log"}},
			},
			Poll: "1s",
		},
		Sink: SinkConfig{
			Type: "kafka",
			Kafka: &KafkaConfig{
				Brokers: []string{"localhost:9092"},
				Topic:   "logs",
			},
		},
		Transform: TransformConfig{
			Type:    "tab",
			OnError: "wrap",
		},
		Pipeline: PipelineConfig{
			BufferSize: 1024,
			OnFull:     "block",
		},
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestValidateWatchSourcesRequirePatterns(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.Watch.Sources = []WatchSource{
		{Path: "./logs/app"},
	}

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error for empty source patterns")
	}
}

func TestValidateRegexRequiresPattern(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.Transform = TransformConfig{
		Type:    "regex",
		OnError: "wrap",
	}

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error when regex pattern is missing")
	}
}

func TestValidateMultilineParserRequiresStartPattern(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.Parser = ParserConfig{Type: "multiline"}

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error when multiline start_pattern is missing")
	}
}

func TestValidateFileSinkRequiresPath(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.Sink = SinkConfig{Type: "file", File: &FileSinkConfig{}}

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error when sink.file.path is missing")
	}
}

func TestValidateHTTPNoauthSinkRequiresURL(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.Sink = SinkConfig{Type: "http-noauth", HTTPNoauth: &HTTPNoauthSinkConfig{}}

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error when sink.http_noauth.url is missing")
	}
}

func TestValidatePipelineRejectsNegativeMaxPublishBytes(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.Pipeline.MaxPublishBytes = -1

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error for negative max_publish_bytes")
	}
}

func TestValidateHibernateWakeIntervalRequiresPositiveWhenEnabled(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.Pipeline.PublishBatch.Hibernate.WakeEnabled = true
	cfg.Pipeline.PublishBatch.Hibernate.WakeInterval = "0"

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error for wake_interval 0 when wake_enabled")
	}
}

func TestValidateHibernateWakeIntervalRejectsInvalidDuration(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.Pipeline.PublishBatch.Hibernate.WakeInterval = "not-a-duration"

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error for invalid hibernate.wake_interval")
	}
}

func TestValidateDeadLetterRequiresPath(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.Pipeline.PublishBatch.OnFlushFailure = OnFlushFailureDeadLetter

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error when dead_letter.path is missing")
	}
}

func TestValidateDeadLetterPathWritable(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.Watch.Sources = []WatchSource{{Path: t.TempDir(), Patterns: []string{"*.log"}}}
	cfg.Pipeline.PublishBatch.OnFlushFailure = OnFlushFailureDeadLetter
	cfg.Pipeline.PublishBatch.DeadLetter.Path = filepath.Join(t.TempDir(), "dlq")

	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if err := cfg.ValidateDeadLetterAtStartup(); err != nil {
		t.Fatalf("ValidateDeadLetterAtStartup() error = %v", err)
	}
}

func TestValidatePublishBatchRejectsInvalidOnFlushFailure(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.Pipeline.PublishBatch.OnFlushFailure = "exit"

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error for invalid publish_batch.on_flush_failure")
	}
}

func TestValidatePublishBatchRejectsNegativeMaxAttempts(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.Pipeline.PublishBatch.MaxAttempts = -1

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error for negative publish_batch.max_attempts")
	}
}

func TestValidatePublishBatchRejectsInvalidFlushInterval(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.Pipeline.PublishBatch.FlushInterval = "not-a-duration"

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error for invalid publish_batch.flush_interval")
	}
}

func TestValidateMultilineParserRejectsInvalidStartPattern(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.Parser = ParserConfig{
		Type:         "multiline",
		StartPattern: `(?P<bad`,
	}

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error for invalid start_pattern")
	}
}
