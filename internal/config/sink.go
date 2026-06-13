// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package config

import (
	"fmt"
	"net/url"
	"path/filepath"
	"time"
)

type SinkConfig struct {
	Type       string                `yaml:"type"`
	File       *FileSinkConfig       `yaml:"file,omitempty"`
	Kafka      *KafkaConfig          `yaml:"kafka,omitempty"`
	HTTPNoauth *HTTPNoauthSinkConfig `yaml:"http_noauth,omitempty"`
	BigQuery   *BigQueryConfig       `yaml:"bigquery,omitempty"`
	Options    map[string]any        `yaml:"options,omitempty"`
}

type FileSinkConfig struct {
	Path string `yaml:"path"`
}

type HTTPNoauthSinkConfig struct {
	URL     string `yaml:"url"`
	Method  string `yaml:"method"`
	Timeout string `yaml:"timeout"`
}

func (c HTTPNoauthSinkConfig) MethodOrDefault() string {
	if c.Method == "" {
		return "POST"
	}
	return c.Method
}

func (c HTTPNoauthSinkConfig) TimeoutDuration() time.Duration {
	if c.Timeout == "" {
		return 30 * time.Second
	}
	d, err := time.ParseDuration(c.Timeout)
	if err != nil {
		return 30 * time.Second
	}
	return d
}

func (c *Config) validateSink() error {
	sinkType := c.Sink.Type
	if sinkType == "" {
		sinkType = "kafka"
	}
	if !knownSinkType(sinkType) {
		return unknownTypeError("sink.type", sinkType, sinkTypes)
	}

	switch sinkType {
	case "kafka":
		if c.Sink.Kafka == nil {
			return fmt.Errorf("sink.kafka is required when sink.type is kafka")
		}
		return c.Sink.Kafka.Validate()
	case "file":
		if c.Sink.File == nil || c.Sink.File.Path == "" {
			return fmt.Errorf("sink.file.path is required when sink.type is file")
		}
		return c.validateSinkFilePath(c.Sink.File.Path)
	case "http-noauth":
		if c.Sink.HTTPNoauth == nil {
			return fmt.Errorf("sink.http_noauth is required when sink.type is http-noauth")
		}
		return c.Sink.HTTPNoauth.Validate()
	case "bigquery":
		if c.Sink.BigQuery == nil {
			return fmt.Errorf("sink.bigquery is required when sink.type is bigquery")
		}
		return c.Sink.BigQuery.Validate()
	default:
		// Custom sink registered via sink.Register; field validation deferred to factory.
		return nil
	}
}

func (c HTTPNoauthSinkConfig) Validate() error {
	if c.URL == "" {
		return fmt.Errorf("sink.http_noauth.url must not be empty")
	}
	if _, err := url.Parse(c.URL); err != nil {
		return fmt.Errorf("sink.http_noauth.url: %w", err)
	}
	if c.Timeout != "" {
		if _, err := time.ParseDuration(c.Timeout); err != nil {
			return fmt.Errorf("sink.http_noauth.timeout: %w", err)
		}
	}
	return nil
}

func (c *Config) validateSinkFilePath(path string) error {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return fmt.Errorf("sink.file.path: %w", err)
	}

	for _, source := range c.Watch.Entries() {
		watchPath, err := filepath.Abs(source.Path)
		if err != nil {
			return fmt.Errorf("watch path %q: %w", source.Path, err)
		}
		if pathInside(watchPath, absPath) {
			return fmt.Errorf(
				"sink.file.path %q must not be inside a watched directory (%q)",
				absPath, watchPath,
			)
		}
		if logFileMatchesWatchPattern(absPath, watchPath, source.Patterns) {
			return fmt.Errorf(
				"sink.file.path %q matches a watch pattern under %q",
				absPath, watchPath,
			)
		}
	}

	if c.Logging.File != "" {
		logPath, err := filepath.Abs(c.Logging.File)
		if err != nil {
			return fmt.Errorf("logging.file: %w", err)
		}
		if absPath == logPath {
			return fmt.Errorf("sink.file.path must not be the same as logging.file")
		}
	}

	return nil
}

func (c *Config) SinkConnectTimeout() time.Duration {
	if c.Sink.Kafka != nil {
		return c.Sink.Kafka.ConnectTimeoutDuration()
	}
	if c.Sink.HTTPNoauth != nil {
		return c.Sink.HTTPNoauth.TimeoutDuration()
	}
	if c.Sink.BigQuery != nil {
		return c.Sink.BigQuery.ConnectTimeoutDuration()
	}
	return 10 * time.Second
}
