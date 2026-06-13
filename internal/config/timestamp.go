// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package config

import (
	"fmt"
	"time"
)

const (
	DefaultTimestampField     = "timestamp"
	DefaultTimestampTimezone  = "UTC"
	DefaultTimestampOutput    = "rfc3339nano"
	TimestampSourceProcessing = "processing"
)

type TimestampConfig struct {
	Field           string `yaml:"field"`
	Format          string `yaml:"format"`
	DefaultTimezone string `yaml:"default_timezone"`
	Output          string `yaml:"output"`
}

func (c TimestampConfig) Enabled() bool {
	return c.Field != "" || c.Format != "" || c.DefaultTimezone != "" || c.Output != ""
}

func (c TimestampConfig) FieldOrDefault() string {
	if c.Field == "" {
		return DefaultTimestampField
	}
	return c.Field
}

func (c TimestampConfig) DefaultTimezoneOrDefault() string {
	if c.DefaultTimezone == "" {
		return DefaultTimestampTimezone
	}
	return c.DefaultTimezone
}

func (c TimestampConfig) OutputOrDefault() string {
	if c.Output == "" {
		return DefaultTimestampOutput
	}
	return c.Output
}

func (c *Config) validateTimestamp() error {
	if !c.Timestamp.Enabled() {
		return nil
	}

	output := c.Timestamp.OutputOrDefault()
	if output != DefaultTimestampOutput {
		return fmt.Errorf("timestamp.output must be %q", DefaultTimestampOutput)
	}

	if c.Timestamp.Format != "" {
		if err := validateTimeLayout(c.Timestamp.Format); err != nil {
			return fmt.Errorf("timestamp.format: %w", err)
		}
	}

	if _, err := time.LoadLocation(c.Timestamp.DefaultTimezoneOrDefault()); err != nil {
		return fmt.Errorf("timestamp.default_timezone: %w", err)
	}

	return nil
}

func validateTimeLayout(layout string) error {
	samples := []string{
		"2006-01-02T15:04:05Z07:00",
		"2006-01-02 15:04:05.000",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05Z",
	}
	for _, sample := range samples {
		if _, err := time.Parse(layout, sample); err == nil {
			return nil
		}
	}
	return fmt.Errorf("invalid Go time layout %q", layout)
}
