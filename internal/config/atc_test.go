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
	if got := cfg.BaseURL(); got != defaultATCURL {
		t.Fatalf("BaseURL() = %q, want %q", got, defaultATCURL)
	}
	if got := cfg.InstancesURL(); got != defaultATCURL+"/api/instances" {
		t.Fatalf("InstancesURL() = %q, want %q", got, defaultATCURL+"/api/instances")
	}
	if got := cfg.TimeoutDuration(); got != 5*time.Second {
		t.Fatalf("TimeoutDuration() = %v, want 5s", got)
	}
}

func TestATCConfigCustomURL(t *testing.T) {
	t.Parallel()

	cfg := ATCConfig{
		Enabled: true,
		URL:     "https://atc.example.com/",
		Timeout: "2s",
	}
	if got := cfg.BaseURL(); got != "https://atc.example.com" {
		t.Fatalf("BaseURL() = %q", got)
	}
	if got := cfg.InstancesURL(); got != "https://atc.example.com/api/instances" {
		t.Fatalf("InstancesURL() = %q", got)
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
	cfg.ATC.URL = "ftp://atc.example.com"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error for non-http atc.url")
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
	cfg.ATC.URL = "http://atc.internal:8090"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}
