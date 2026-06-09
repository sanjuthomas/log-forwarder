package config

import (
	"fmt"
	"strings"
)

type MetricsConfig struct {
	Enabled bool   `yaml:"enabled"`
	Host    string `yaml:"host"`
	Port    int    `yaml:"port"`
	Path    string `yaml:"path"`
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
	return nil
}
