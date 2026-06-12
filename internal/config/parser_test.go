// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package config

import (
	"testing"
	"time"
)

func TestParserFlushIntervalDuration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  ParserConfig
		want int64
	}{
		{
			name: "line parser disabled",
			cfg:  ParserConfig{Type: "line"},
			want: 0,
		},
		{
			name: "multiline default",
			cfg:  ParserConfig{Type: "multiline", StartPattern: "^start"},
			want: int64(DefaultParserFlushInterval / time.Millisecond),
		},
		{
			name: "multiline explicit zero",
			cfg:  ParserConfig{Type: "multiline", StartPattern: "^start", FlushInterval: "0"},
			want: 0,
		},
		{
			name: "multiline custom",
			cfg:  ParserConfig{Type: "multiline", StartPattern: "^start", FlushInterval: "250ms"},
			want: 250,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.cfg.FlushIntervalDuration()
			if got.Milliseconds() != tt.want {
				t.Fatalf("FlushIntervalDuration() = %v, want %dms", got, tt.want)
			}
		})
	}
}

func TestValidateParserRejectsInvalidFlushInterval(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.Parser = ParserConfig{
		Type:          "multiline",
		StartPattern:  `^\d{4}`,
		FlushInterval: "not-a-duration",
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error for invalid parser.flush_interval")
	}
}
