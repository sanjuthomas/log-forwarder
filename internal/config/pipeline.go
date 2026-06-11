// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

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
	DefaultMaxPublishBytes           = 1048576 // 1 MiB, aligned with Kafka message.max.bytes default
	DefaultTruncateField             = "message"
	DefaultTruncateSuffix            = "… [truncated]"
	DefaultPublishBatchMaxBytes      = 1048576
	DefaultPublishBatchFlushInterval = 100 * time.Millisecond
	OnFlushFailureHibernate              = "hibernate"
	OnFlushFailureDeadLetter             = "dead_letter"
	DefaultHibernateWakeInterval         = 10 * time.Minute
	DefaultDeadLetterMaxConsecutiveBatches = 3
)

type DeadLetterConfig struct {
	Path                  string `yaml:"path"`
	MaxConsecutiveBatches int    `yaml:"max_consecutive_batches"`
}

func (c DeadLetterConfig) MaxConsecutiveBatchesOrDefault() int {
	if c.MaxConsecutiveBatches <= 0 {
		return DefaultDeadLetterMaxConsecutiveBatches
	}
	return c.MaxConsecutiveBatches
}

type HibernateConfig struct {
	WakeEnabled  bool   `yaml:"wake_enabled"`
	WakeInterval string `yaml:"wake_interval"`
}

func (c HibernateConfig) WakeIntervalDuration() time.Duration {
	if c.WakeInterval == "" {
		return DefaultHibernateWakeInterval
	}
	if c.WakeInterval == "0" {
		return 0
	}
	d, err := time.ParseDuration(c.WakeInterval)
	if err != nil {
		return DefaultHibernateWakeInterval
	}
	return d
}

type PublishBatchConfig struct {
	MaxBytes       int             `yaml:"max_bytes"`
	FlushInterval  string          `yaml:"flush_interval"`
	OnFlushFailure string          `yaml:"on_flush_failure"`
	MaxAttempts    int             `yaml:"max_attempts"`
	Hibernate      HibernateConfig  `yaml:"hibernate"`
	DeadLetter     DeadLetterConfig `yaml:"dead_letter"`
}

func (c PublishBatchConfig) OnFlushFailureOrDefault() string {
	if c.OnFlushFailure == "" {
		return OnFlushFailureHibernate
	}
	return c.OnFlushFailure
}

func (c PublishBatchConfig) MaxAttemptsOrDefault(retry PublishRetryConfig) int {
	if c.MaxAttempts > 0 {
		return c.MaxAttempts
	}
	return retry.MaxAttempts
}

func (c PublishBatchConfig) Enabled() bool {
	return c.MaxBytes > 0 || (c.FlushInterval != "0" && c.FlushIntervalDuration() > 0)
}

func (c PublishBatchConfig) SizeTriggerEnabled() bool {
	return c.Enabled() && c.MaxBytes > 0
}

func (c PublishBatchConfig) MaxBytesLimit() int {
	if !c.SizeTriggerEnabled() {
		return 0
	}
	return c.MaxBytes
}

func (c PublishBatchConfig) FlushIntervalDuration() time.Duration {
	if c.FlushInterval == "0" {
		return 0
	}
	if c.FlushInterval == "" {
		if c.MaxBytes > 0 {
			return DefaultPublishBatchFlushInterval
		}
		return 0
	}
	d, err := time.ParseDuration(c.FlushInterval)
	if err != nil {
		return 0
	}
	return d
}

type PipelineConfig struct {
	BufferSize       int                `yaml:"buffer_size"`
	OnFull           string             `yaml:"on_full"`
	PublishTimeout   string             `yaml:"publish_timeout"`
	PublishRetry     PublishRetryConfig `yaml:"publish_retry"`
	PublishBatch     PublishBatchConfig `yaml:"publish_batch"`
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

	if c.Pipeline.PublishBatch.MaxBytes < 0 {
		return fmt.Errorf("pipeline.publish_batch.max_bytes must be >= 0")
	}
	if c.Pipeline.PublishBatch.FlushInterval != "" && c.Pipeline.PublishBatch.FlushInterval != "0" {
		if _, err := time.ParseDuration(c.Pipeline.PublishBatch.FlushInterval); err != nil {
			return fmt.Errorf("pipeline.publish_batch.flush_interval: %w", err)
		}
	}
	if c.Pipeline.PublishBatch.MaxAttempts < 0 {
		return fmt.Errorf("pipeline.publish_batch.max_attempts must be >= 0")
	}
	switch c.Pipeline.PublishBatch.OnFlushFailureOrDefault() {
	case OnFlushFailureHibernate:
	case OnFlushFailureDeadLetter:
		if c.Pipeline.PublishBatch.DeadLetter.Path == "" {
			return fmt.Errorf("pipeline.publish_batch.dead_letter.path is required when on_flush_failure is dead_letter")
		}
		if c.Pipeline.PublishBatch.DeadLetter.MaxConsecutiveBatches < 0 {
			return fmt.Errorf("pipeline.publish_batch.dead_letter.max_consecutive_batches must be >= 0")
		}
		if err := validateDeadLetterPath(c, c.Pipeline.PublishBatch.DeadLetter.Path); err != nil {
			return err
		}
	default:
		return fmt.Errorf("pipeline.publish_batch.on_flush_failure must be hibernate or dead_letter")
	}

	hibernate := c.Pipeline.PublishBatch.Hibernate
	if hibernate.WakeInterval != "" && hibernate.WakeInterval != "0" {
		if _, err := time.ParseDuration(hibernate.WakeInterval); err != nil {
			return fmt.Errorf("pipeline.publish_batch.hibernate.wake_interval: %w", err)
		}
	}
	if hibernate.WakeEnabled && hibernate.WakeIntervalDuration() <= 0 {
		return fmt.Errorf("pipeline.publish_batch.hibernate.wake_interval must be positive when wake_enabled is true")
	}

	return nil
}
