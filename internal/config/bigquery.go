// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package config

import (
	"fmt"
	"time"
)

// BigQueryConfig configures the BigQuery Storage Write API sink.
type BigQueryConfig struct {
	ProjectID       string `yaml:"project_id"`
	Dataset         string `yaml:"dataset"`
	Table           string `yaml:"table"`
	CredentialsFile string `yaml:"credentials_file"`
	ConnectTimeout  string `yaml:"connect_timeout"`
	// WriteRetries enables automatic append retries (default true when omitted).
	WriteRetries *bool `yaml:"write_retries,omitempty"`
}

func (c BigQueryConfig) ConnectTimeoutDuration() time.Duration {
	if c.ConnectTimeout == "" {
		return 30 * time.Second
	}
	d, err := time.ParseDuration(c.ConnectTimeout)
	if err != nil {
		return 30 * time.Second
	}
	return d
}

func (c BigQueryConfig) WriteRetriesEnabled() bool {
	if c.WriteRetries == nil {
		return true
	}
	return *c.WriteRetries
}

func (c BigQueryConfig) Validate() error {
	if c.ProjectID == "" {
		return fmt.Errorf("sink.bigquery.project_id must not be empty")
	}
	if c.Dataset == "" {
		return fmt.Errorf("sink.bigquery.dataset must not be empty")
	}
	if c.Table == "" {
		return fmt.Errorf("sink.bigquery.table must not be empty")
	}
	if c.ConnectTimeout != "" {
		if _, err := time.ParseDuration(c.ConnectTimeout); err != nil {
			return fmt.Errorf("sink.bigquery.connect_timeout: %w", err)
		}
	}
	return nil
}
