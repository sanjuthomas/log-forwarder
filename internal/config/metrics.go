package config

import (
	"fmt"
	"strings"
	"time"
)

type MetricsConfig struct {
	Enabled    bool             `yaml:"enabled"`
	Host       string           `yaml:"host"`
	Port       int              `yaml:"port"`
	Path       string           `yaml:"path"`
	Readiness  ReadinessConfig  `yaml:"readiness"`
}

type ReadinessConfig struct {
	Path              string  `yaml:"path"`
	BufferThreshold   float64 `yaml:"buffer_threshold"`
	SinkCheck         *bool   `yaml:"sink_check"`
	RequireFiles      bool    `yaml:"require_files"`
	SinkCheckTimeout  string  `yaml:"sink_check_timeout"`
}

func (c MetricsConfig) Addr() string {
	host := c.Host
	if host == "" {
		host = "127.0.0.1"
	}
	port := c.Port
	if port == 0 {
		port = 8080
	}
	return fmt.Sprintf("%s:%d", host, port)
}

func (c MetricsConfig) MetricsPath() string {
	if c.Path == "" {
		return "/metrics"
	}
	path := c.Path
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return path
}

func (c MetricsConfig) DeadLettersPath() string {
	return "/deadletters"
}

func (c ReadinessConfig) ReadyPath() string {
	if c.Path == "" {
		return "/ready"
	}
	path := c.Path
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return path
}

func (c ReadinessConfig) BufferThresholdOrDefault() float64 {
	if c.BufferThreshold <= 0 {
		return 0.8
	}
	return c.BufferThreshold
}

func (c ReadinessConfig) SinkCheckEnabled() bool {
	if c.SinkCheck == nil {
		return true
	}
	return *c.SinkCheck
}

func (c ReadinessConfig) SinkCheckTimeoutDuration(fallback time.Duration) time.Duration {
	if c.SinkCheckTimeout == "" || c.SinkCheckTimeout == "0" {
		return fallback
	}
	d, err := time.ParseDuration(c.SinkCheckTimeout)
	if err != nil {
		return fallback
	}
	return d
}

func (c *Config) validateMetrics() error {
	if !c.Metrics.Enabled {
		return nil
	}
	if c.Metrics.Port < 0 || c.Metrics.Port > 65535 {
		return fmt.Errorf("metrics.port must be between 0 and 65535")
	}
	path := c.Metrics.MetricsPath()
	if path == "/" {
		return fmt.Errorf("metrics.path must not be \"/\"")
	}
	readyPath := c.Metrics.Readiness.ReadyPath()
	if readyPath == "/" {
		return fmt.Errorf("metrics.readiness.path must not be \"/\"")
	}
	if readyPath == path {
		return fmt.Errorf("metrics.readiness.path must differ from metrics.path")
	}
	threshold := c.Metrics.Readiness.BufferThreshold
	if threshold < 0 || threshold > 1 {
		return fmt.Errorf("metrics.readiness.buffer_threshold must be between 0 and 1")
	}
	if c.Metrics.Readiness.SinkCheckTimeout != "" && c.Metrics.Readiness.SinkCheckTimeout != "0" {
		if _, err := time.ParseDuration(c.Metrics.Readiness.SinkCheckTimeout); err != nil {
			return fmt.Errorf("metrics.readiness.sink_check_timeout: %w", err)
		}
	}
	return nil
}
