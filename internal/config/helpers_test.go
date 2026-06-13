// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package config

import (
	"path/filepath"
	"testing"
	"time"
)

func TestPipelineConfigDefaultsAndHelpers(t *testing.T) {
	t.Parallel()

	var pipeline PipelineConfig
	if got := pipeline.TruncateFieldOrDefault(); got != DefaultTruncateField {
		t.Fatalf("TruncateFieldOrDefault() = %q", got)
	}
	if got := pipeline.TruncateSuffixOrDefault(); got != DefaultTruncateSuffix {
		t.Fatalf("TruncateSuffixOrDefault() = %q", got)
	}

	pipeline.TruncateField = "body"
	pipeline.TruncateSuffix = "..."
	if got := pipeline.TruncateFieldOrDefault(); got != "body" {
		t.Fatalf("TruncateFieldOrDefault() = %q", got)
	}
	if got := pipeline.TruncateSuffixOrDefault(); got != "..." {
		t.Fatalf("TruncateSuffixOrDefault() = %q", got)
	}

	var deadLetter DeadLetterConfig
	if got := deadLetter.MaxConsecutiveBatchesOrDefault(); got != DefaultDeadLetterMaxConsecutiveBatches {
		t.Fatalf("MaxConsecutiveBatchesOrDefault() = %d", got)
	}
	deadLetter.MaxConsecutiveBatches = 5
	if got := deadLetter.MaxConsecutiveBatchesOrDefault(); got != 5 {
		t.Fatalf("MaxConsecutiveBatchesOrDefault() = %d", got)
	}

	var hibernate HibernateConfig
	if got := hibernate.WakeIntervalDuration(); got != DefaultHibernateWakeInterval {
		t.Fatalf("WakeIntervalDuration() = %v", got)
	}
	hibernate.WakeInterval = "0"
	if got := hibernate.WakeIntervalDuration(); got != 0 {
		t.Fatalf("WakeIntervalDuration() = %v", got)
	}
	hibernate.WakeInterval = "bad"
	if got := hibernate.WakeIntervalDuration(); got != DefaultHibernateWakeInterval {
		t.Fatalf("WakeIntervalDuration() = %v", got)
	}
	hibernate.WakeInterval = "2m"
	if got := hibernate.WakeIntervalDuration(); got != 2*time.Minute {
		t.Fatalf("WakeIntervalDuration() = %v", got)
	}
}

func TestPublishBatchConfigHelpers(t *testing.T) {
	t.Parallel()

	retry := PublishRetryConfig{MaxAttempts: 7}
	var batch PublishBatchConfig

	if got := batch.MaxAttemptsOrDefault(retry); got != 7 {
		t.Fatalf("MaxAttemptsOrDefault() = %d", got)
	}
	batch.MaxAttempts = 3
	if got := batch.MaxAttemptsOrDefault(retry); got != 3 {
		t.Fatalf("MaxAttemptsOrDefault() = %d", got)
	}

	if batch.Enabled() {
		t.Fatal("expected disabled publish batch by default")
	}

	batch.MaxBytes = 1024
	if !batch.Enabled() || !batch.SizeTriggerEnabled() {
		t.Fatal("expected size-triggered publish batch")
	}
	if got := batch.MaxBytesLimit(); got != 1024 {
		t.Fatalf("MaxBytesLimit() = %d", got)
	}

	batch.MaxBytes = 0
	batch.FlushInterval = "250ms"
	if !batch.Enabled() {
		t.Fatal("expected interval-triggered publish batch")
	}
	if got := batch.FlushIntervalDuration(); got != 250*time.Millisecond {
		t.Fatalf("FlushIntervalDuration() = %v", got)
	}

	batch.FlushInterval = "0"
	if batch.Enabled() {
		t.Fatal("expected disabled when flush interval is zero")
	}
	if got := batch.MaxBytesLimit(); got != 0 {
		t.Fatalf("MaxBytesLimit() = %d", got)
	}

	batch.MaxBytes = 512
	batch.FlushInterval = ""
	if got := batch.FlushIntervalDuration(); got != DefaultPublishBatchFlushInterval {
		t.Fatalf("FlushIntervalDuration() = %v", got)
	}
	batch.FlushInterval = "not-a-duration"
	if got := batch.FlushIntervalDuration(); got != 0 {
		t.Fatalf("FlushIntervalDuration() = %v", got)
	}
}

func TestHTTPNoauthConfigHelpers(t *testing.T) {
	t.Parallel()

	var httpCfg HTTPNoauthSinkConfig
	if got := httpCfg.MethodOrDefault(); got != "POST" {
		t.Fatalf("MethodOrDefault() = %q", got)
	}
	httpCfg.Method = "PUT"
	if got := httpCfg.MethodOrDefault(); got != "PUT" {
		t.Fatalf("MethodOrDefault() = %q", got)
	}

	if got := httpCfg.TimeoutDuration(); got != 30*time.Second {
		t.Fatalf("TimeoutDuration() = %v", got)
	}
	httpCfg.Timeout = "5s"
	if got := httpCfg.TimeoutDuration(); got != 5*time.Second {
		t.Fatalf("TimeoutDuration() = %v", got)
	}
	httpCfg.Timeout = "invalid"
	if got := httpCfg.TimeoutDuration(); got != 30*time.Second {
		t.Fatalf("TimeoutDuration() = %v", got)
	}

	if err := (HTTPNoauthSinkConfig{URL: "http://localhost:8080/ingest"}).Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if err := (HTTPNoauthSinkConfig{}).Validate(); err == nil {
		t.Fatal("expected validation error for empty url")
	}
	if err := (HTTPNoauthSinkConfig{URL: "://bad", Timeout: "nope"}).Validate(); err == nil {
		t.Fatal("expected validation error for invalid url and timeout")
	}
}

func TestSinkConnectTimeoutAndStatePersistOptions(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.Sink = SinkConfig{
		Type: "kafka",
		Kafka: &KafkaConfig{
			Brokers:        []string{"localhost:9092"},
			Topic:          "logs",
			ConnectTimeout: "3s",
		},
	}
	if got := cfg.SinkConnectTimeout(); got != 3*time.Second {
		t.Fatalf("SinkConnectTimeout() = %v", got)
	}

	cfg.Sink = SinkConfig{
		Type:       "http-noauth",
		HTTPNoauth: &HTTPNoauthSinkConfig{URL: "http://localhost:8080", Timeout: "7s"},
	}
	if got := cfg.SinkConnectTimeout(); got != 7*time.Second {
		t.Fatalf("SinkConnectTimeout() = %v", got)
	}

	cfg.Sink = SinkConfig{
		Type: "bigquery",
		BigQuery: &BigQueryConfig{
			ProjectID:      "proj",
			Dataset:        "logs",
			Table:          "events",
			ConnectTimeout: "12s",
		},
	}
	if got := cfg.SinkConnectTimeout(); got != 12*time.Second {
		t.Fatalf("SinkConnectTimeout() = %v", got)
	}

	cfg.Sink = SinkConfig{Type: "file", File: &FileSinkConfig{Path: t.TempDir() + "/out.jsonl"}}
	if got := cfg.SinkConnectTimeout(); got != 10*time.Second {
		t.Fatalf("SinkConnectTimeout() = %v", got)
	}

	state := StateConfig{FlushInterval: "500ms", FlushEvery: 10}
	interval, every := state.PersistOptions()
	if interval != 500*time.Millisecond || every != 10 {
		t.Fatalf("PersistOptions() = (%v, %d)", interval, every)
	}
}

func TestParserAndTimestampDefaults(t *testing.T) {
	t.Parallel()

	if got := (ParserConfig{}).TypeOrDefault(); got != "line" {
		t.Fatalf("TypeOrDefault() = %q", got)
	}
	if got := (ParserConfig{Type: "multiline"}).TypeOrDefault(); got != "multiline" {
		t.Fatalf("TypeOrDefault() = %q", got)
	}

	if got := (TimestampConfig{}).FieldOrDefault(); got != "timestamp" {
		t.Fatalf("FieldOrDefault() = %q", got)
	}
	if got := (TimestampConfig{Field: "ts"}).FieldOrDefault(); got != "ts" {
		t.Fatalf("FieldOrDefault() = %q", got)
	}
}

func TestRegisterTypeIgnoresEmptyName(t *testing.T) {
	t.Parallel()

	RegisterSinkType("")
	RegisterEnricherType("")
	RegisterParserType("")
	RegisterTransformType("")
	RegisterFilterType("")
}

func TestValidateSinkFilePathRejectsLoggingFileAndWatchPattern(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	watchDir := filepath.Join(dir, "logs")
	logPath := filepath.Join(dir, "forwarder.log")
	sinkPath := filepath.Join(watchDir, "app.jsonl")

	cfg := validConfig()
	cfg.Watch.Sources = []WatchSource{{Path: watchDir, Patterns: []string{"*.jsonl"}}}
	cfg.Logging.File = logPath
	cfg.Sink = SinkConfig{Type: "file", File: &FileSinkConfig{Path: logPath}}
	if err := cfg.validateSink(); err == nil {
		t.Fatal("expected error when sink.file.path equals logging.file")
	}

	cfg.Sink = SinkConfig{Type: "file", File: &FileSinkConfig{Path: sinkPath}}
	if err := cfg.validateSink(); err == nil {
		t.Fatal("expected error when sink.file.path matches watch pattern")
	}
}

func TestDefaultConfigUsesWorkingDirectory(t *testing.T) {
	t.Parallel()

	cfg := Default()
	if len(cfg.Watch.Paths) != 1 || cfg.Watch.Paths[0] == "" {
		t.Fatalf("Watch.Paths = %v", cfg.Watch.Paths)
	}
	if cfg.Sink.Kafka == nil || cfg.Sink.Kafka.Topic != "logs" {
		t.Fatalf("default kafka config = %+v", cfg.Sink.Kafka)
	}
}
