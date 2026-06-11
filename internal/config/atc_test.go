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
	wantBase := "http://localhost:8090"
	if got := cfg.BaseURL(); got != wantBase {
		t.Fatalf("BaseURL() = %q, want %q", got, wantBase)
	}
	if got := cfg.InstancesURL(); got != wantBase+"/api/instances" {
		t.Fatalf("InstancesURL() = %q, want %q", got, wantBase+"/api/instances")
	}
	if got := cfg.TimeoutDuration(); got != 5*time.Second {
		t.Fatalf("TimeoutDuration() = %v, want 5s", got)
	}
}

func TestATCConfigHostPort(t *testing.T) {
	t.Parallel()

	cfg := ATCConfig{
		Enabled: true,
		Host:    "atc.internal",
		Port:    9000,
	}
	if got := cfg.BaseURL(); got != "http://atc.internal:9000" {
		t.Fatalf("BaseURL() = %q, want http://atc.internal:9000", got)
	}
}

func TestATCConfigURLOverridesHostPort(t *testing.T) {
	t.Parallel()

	cfg := ATCConfig{
		Enabled: true,
		Host:    "ignored",
		Port:    1,
		URL:     "https://atc.example.com/",
	}
	if got := cfg.BaseURL(); got != "https://atc.example.com" {
		t.Fatalf("BaseURL() = %q", got)
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
	cfg.ATC.Port = 70000
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error for invalid atc.port")
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
	cfg.ATC.Host = "atc.internal"
	cfg.ATC.Port = 8090
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}
