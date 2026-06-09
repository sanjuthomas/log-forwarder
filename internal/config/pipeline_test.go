package config

import (
	"testing"
	"time"
)

func TestValidatePipelinePublishRetry(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.Pipeline.PublishRetry = PublishRetryConfig{
		InitialBackoff: "1s",
		MaxBackoff:     "30s",
		MaxAttempts:    0,
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestValidatePipelinePublishRetryMaxBackoffBeforeInitial(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.Pipeline.PublishRetry = PublishRetryConfig{
		InitialBackoff: "30s",
		MaxBackoff:     "1s",
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error when max_backoff < initial_backoff")
	}
}

func TestValidatePipelinePublishRetryNegativeAttempts(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.Pipeline.PublishRetry.MaxAttempts = -1
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error for negative max_attempts")
	}
}

func TestValidatePipelinePublishTimeout(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.Pipeline.PublishTimeout = "not-a-duration"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error for invalid publish_timeout")
	}
}

func TestPipelinePublishTimeoutDurationZero(t *testing.T) {
	t.Parallel()

	cfg := Default()
	if got := cfg.Pipeline.PublishTimeoutDuration(); got != 0 {
		t.Fatalf("PublishTimeoutDuration() = %v, want 0", got)
	}

	cfg.Pipeline.PublishTimeout = "15s"
	if got := cfg.Pipeline.PublishTimeoutDuration(); got != 15*time.Second {
		t.Fatalf("PublishTimeoutDuration() = %v, want 15s", got)
	}
}
