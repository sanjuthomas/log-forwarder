// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package config

import (
	"testing"
	"time"
)

func TestValidateStateFlushInterval(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.Watch.State.FlushInterval = "not-a-duration"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error for invalid flush_interval")
	}
}

func TestValidateStateFlushEveryNegative(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.Watch.State.FlushEvery = -1
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error for negative flush_every")
	}
}

func TestStateFlushIntervalDuration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		interval string
		wantZero bool
		want     time.Duration
	}{
		{name: "default", interval: "", want: time.Second},
		{name: "sync", interval: "0", wantZero: true},
		{name: "custom", interval: "250ms", want: 250 * time.Millisecond},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := (StateConfig{FlushInterval: tc.interval}).FlushIntervalDuration()
			if tc.wantZero {
				if got != 0 {
					t.Fatalf("FlushIntervalDuration() = %v, want 0", got)
				}
				return
			}
			if got != tc.want {
				t.Fatalf("FlushIntervalDuration() = %v, want %v", got, tc.want)
			}
		})
	}
}
