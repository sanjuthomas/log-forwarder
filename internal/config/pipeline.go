package config

import (
	"fmt"
	"time"
)

type PublishRetryConfig struct {
	InitialBackoff string `yaml:"initial_backoff"`
	MaxBackoff     string `yaml:"max_backoff"`
	MaxAttempts    int    `yaml:"max_attempts"`
}

const (
	DefaultMaxPublishBytes = 1048576 // 1 MiB, aligned with Kafka message.max.bytes default
	DefaultTruncateField   = "message"
	DefaultTruncateSuffix  = "… [truncated]"
)

type PipelineConfig struct {
	BufferSize       int                `yaml:"buffer_size"`
	OnFull           string             `yaml:"on_full"`
	PublishTimeout   string             `yaml:"publish_timeout"`
	PublishRetry     PublishRetryConfig `yaml:"publish_retry"`
	MaxPublishBytes  int                `yaml:"max_publish_bytes"`
	TruncateField    string             `yaml:"truncate_field"`
	TruncateSuffix   string             `yaml:"truncate_suffix"`
}

func (c PipelineConfig) TruncateFieldOrDefault() string {
	if c.TruncateField == "" {
		return DefaultTruncateField
	}
	return c.TruncateField
}

func (c PipelineConfig) TruncateSuffixOrDefault() string {
	if c.TruncateSuffix == "" {
		return DefaultTruncateSuffix
	}
	return c.TruncateSuffix
}

func (c PublishRetryConfig) InitialBackoffDuration() time.Duration {
	if c.InitialBackoff == "" {
		return time.Second
	}
	d, err := time.ParseDuration(c.InitialBackoff)
	if err != nil {
		return time.Second
	}
	return d
}

func (c PublishRetryConfig) MaxBackoffDuration() time.Duration {
	if c.MaxBackoff == "" {
		return 30 * time.Second
	}
	d, err := time.ParseDuration(c.MaxBackoff)
	if err != nil {
		return 30 * time.Second
	}
	return d
}

func (c PipelineConfig) PublishTimeoutDuration() time.Duration {
	if c.PublishTimeout == "" || c.PublishTimeout == "0" {
		return 0
	}
	d, err := time.ParseDuration(c.PublishTimeout)
	if err != nil {
		return 0
	}
	return d
}

func (c *Config) validatePipeline() error {
	if c.Pipeline.BufferSize <= 0 {
		return fmt.Errorf("pipeline.buffer_size must be positive")
	}
	switch c.Pipeline.OnFull {
	case "block", "drop":
	default:
		return fmt.Errorf("pipeline.on_full must be block or drop")
	}

	if c.Pipeline.PublishTimeout != "" && c.Pipeline.PublishTimeout != "0" {
		if _, err := time.ParseDuration(c.Pipeline.PublishTimeout); err != nil {
			return fmt.Errorf("pipeline.publish_timeout: %w", err)
		}
	}

	retry := c.Pipeline.PublishRetry
	if retry.InitialBackoff != "" {
		if _, err := time.ParseDuration(retry.InitialBackoff); err != nil {
			return fmt.Errorf("pipeline.publish_retry.initial_backoff: %w", err)
		}
	}
	if retry.MaxBackoff != "" {
		if _, err := time.ParseDuration(retry.MaxBackoff); err != nil {
			return fmt.Errorf("pipeline.publish_retry.max_backoff: %w", err)
		}
	}
	if retry.MaxAttempts < 0 {
		return fmt.Errorf("pipeline.publish_retry.max_attempts must be >= 0")
	}

	initial := retry.InitialBackoffDuration()
	maximum := retry.MaxBackoffDuration()
	if maximum < initial {
		return fmt.Errorf("pipeline.publish_retry.max_backoff must be >= initial_backoff")
	}

	if c.Pipeline.MaxPublishBytes < 0 {
		return fmt.Errorf("pipeline.max_publish_bytes must be >= 0")
	}

	return nil
}
