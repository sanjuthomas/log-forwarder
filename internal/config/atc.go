// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package config

import (
	"fmt"
	"net/url"
	"strings"
	"time"
)

const defaultATCURL = "http://localhost:8090"

// ATCConfig controls registration with the log-forwarder-atc controller.
type ATCConfig struct {
	Enabled bool   `yaml:"enabled"`
	URL     string `yaml:"url"`
	Timeout string `yaml:"timeout"`
}

// BaseURL returns the ATC base URL without a trailing slash.
func (c ATCConfig) BaseURL() string {
	raw := strings.TrimSpace(c.URL)
	if raw == "" {
		raw = defaultATCURL
	}
	return strings.TrimRight(raw, "/")
}

// InstancesURL returns the full instances registration endpoint.
func (c ATCConfig) InstancesURL() string {
	return c.BaseURL() + "/api/instances"
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
	parsed, err := url.Parse(c.ATC.BaseURL())
	if err != nil {
		return fmt.Errorf("atc.url: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("atc.url must use http or https")
	}
	if parsed.Host == "" {
		return fmt.Errorf("atc.url must include a host")
	}
	if c.ATC.Timeout != "" {
		if _, err := time.ParseDuration(c.ATC.Timeout); err != nil {
			return fmt.Errorf("atc.timeout: %w", err)
		}
	}
	return nil
}
