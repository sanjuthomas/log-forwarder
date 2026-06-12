// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package config

import (
	"fmt"
	"net/url"
	"strings"
	"time"
)

const defaultATCEndpoint = "http://localhost:8090/api/instances"

// ATCConfig controls registration with the log-forwarder-atc controller.
type ATCConfig struct {
	Enabled bool   `yaml:"enabled"`
	URL     string `yaml:"url"`
	Timeout string `yaml:"timeout"`
}

// EndpointURL returns the full PUT/DELETE registration endpoint.
func (c ATCConfig) EndpointURL() string {
	raw := strings.TrimSpace(c.URL)
	if raw == "" {
		return defaultATCEndpoint
	}
	return strings.TrimRight(raw, "/")
}

func (c ATCConfig) TimeoutDuration() time.Duration {
	if c.Timeout == "" {
		return 5 * time.Second
	}
	d, err := time.ParseDuration(c.Timeout)
	if err != nil {
		return 5 * time.Second
	}
	return d
}

func (c *Config) validateATC() error {
	if !c.ATC.Enabled {
		return nil
	}
	if !c.Metrics.Enabled {
		return fmt.Errorf("atc.enabled requires metrics.enabled so the controller can reach /health and /ready")
	}
	parsed, err := url.Parse(c.ATC.EndpointURL())
	if err != nil {
		return fmt.Errorf("atc.url: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("atc.url must use http or https")
	}
	if parsed.Host == "" {
		return fmt.Errorf("atc.url must include a host")
	}
	if parsed.Path == "" {
		return fmt.Errorf("atc.url must include a path")
	}
	if c.ATC.Timeout != "" {
		if _, err := time.ParseDuration(c.ATC.Timeout); err != nil {
			return fmt.Errorf("atc.timeout: %w", err)
		}
	}
	return nil
}
