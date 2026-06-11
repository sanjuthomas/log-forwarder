// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package config

import (
	"testing"
	"time"
)

func TestATCConfigDefaults(t *testing.T) {
	t.Parallel()

	cfg := ATCConfig{Enabled: true}
	if got := cfg.EndpointURL(); got != defaultATCEndpoint {
		t.Fatalf("EndpointURL() = %q, want %q", got, defaultATCEndpoint)
	}
	if got := cfg.TimeoutDuration(); got != 5*time.Second {
		t.Fatalf("TimeoutDuration() = %v, want 5s", got)
	}
}

func TestATCConfigCustomEndpoint(t *testing.T) {
	t.Parallel()

	cfg := ATCConfig{
		Enabled: true,
		URL:     "https://atc.example.com/api/instances/",
		Timeout: "2s",
	}
	if got := cfg.EndpointURL(); got != "https://atc.example.com/api/instances" {
		t.Fatalf("EndpointURL() = %q", got)
	}
	if got := cfg.TimeoutDuration(); got != 2*time.Second {
		t.Fatalf("TimeoutDuration() = %v, want 2s", got)
	}
}

func TestValidateATCDisabled(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.ATC.URL = "not-a-url"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestValidateATCEnabledRequiresValidURL(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.ATC.Enabled = true
	cfg.ATC.URL = "ftp://atc.example.com/api/instances"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error for non-http atc.url")
	}

	cfg = Default()
	cfg.ATC.Enabled = true
	cfg.ATC.URL = "http://localhost:8090"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error when atc.url has no path")
	}

	cfg = Default()
	cfg.ATC.Enabled = true
	cfg.ATC.Timeout = "not-a-duration"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error for invalid atc.timeout")
	}
}

func TestValidateATCEnabledValid(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.ATC.Enabled = true
	cfg.ATC.URL = "http://atc.internal:8090/api/instances"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}
