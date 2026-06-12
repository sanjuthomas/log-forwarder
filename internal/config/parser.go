// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package config

import "time"

const DefaultParserFlushInterval = 100 * time.Millisecond

func (c ParserConfig) TypeOrDefault() string {
	if c.Type == "" {
		return "line"
	}
	return c.Type
}

// FlushIntervalDuration returns how long to wait after the last physical line
// before flushing a pending multiline record. Disabled for line parser and when
// parser.flush_interval is "0".
func (c ParserConfig) FlushIntervalDuration() time.Duration {
	if c.TypeOrDefault() != "multiline" {
		return 0
	}
	if c.FlushInterval == "0" {
		return 0
	}
	if c.FlushInterval == "" {
		return DefaultParserFlushInterval
	}
	d, err := time.ParseDuration(c.FlushInterval)
	if err != nil {
		return 0
	}
	return d
}
