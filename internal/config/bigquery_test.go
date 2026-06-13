// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package config

import (
	"testing"
	"time"
)

func TestBigQueryConfigValidateRequiresFields(t *testing.T) {
	t.Parallel()

	cfg := BigQueryConfig{}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error for empty config")
	}

	cfg = BigQueryConfig{ProjectID: "proj", Dataset: "logs"}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error when table missing")
	}

	cfg = BigQueryConfig{ProjectID: "proj", Dataset: "logs", Table: "events"}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestBigQueryConfigConnectTimeoutDuration(t *testing.T) {
	t.Parallel()

	cfg := BigQueryConfig{}
	if got := cfg.ConnectTimeoutDuration(); got != 30*time.Second {
		t.Fatalf("ConnectTimeoutDuration() = %v, want 30s default", got)
	}

	cfg.ConnectTimeout = "5s"
	if got := cfg.ConnectTimeoutDuration(); got != 5*time.Second {
		t.Fatalf("ConnectTimeoutDuration() = %v, want 5s", got)
	}

	cfg.ConnectTimeout = "invalid"
	if got := cfg.ConnectTimeoutDuration(); got != 30*time.Second {
		t.Fatalf("ConnectTimeoutDuration() = %v, want 30s fallback", got)
	}
}

func TestBigQueryConfigWriteRetriesEnabled(t *testing.T) {
	t.Parallel()

	cfg := BigQueryConfig{}
	if !cfg.WriteRetriesEnabled() {
		t.Fatal("expected write retries enabled by default")
	}

	disabled := false
	cfg.WriteRetries = &disabled
	if cfg.WriteRetriesEnabled() {
		t.Fatal("expected write retries disabled")
	}

	enabled := true
	cfg.WriteRetries = &enabled
	if !cfg.WriteRetriesEnabled() {
		t.Fatal("expected write retries enabled")
	}
}

func TestValidateSinkBigQueryRequiresBlock(t *testing.T) {
	t.Parallel()

	cfg := validConfig()
	cfg.Sink = SinkConfig{Type: "bigquery"}
	if err := cfg.validateSink(); err == nil {
		t.Fatal("expected error when sink.bigquery block missing")
	}

	cfg.Sink = SinkConfig{
		Type: "bigquery",
		BigQuery: &BigQueryConfig{
			ProjectID: "proj",
			Dataset:   "logs",
			Table:     "events",
		},
	}
	if err := cfg.validateSink(); err != nil {
		t.Fatalf("validateSink() error = %v", err)
	}
}
