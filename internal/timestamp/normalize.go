// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package timestamp

import (
	"strings"
	"time"

	"github.com/sanjuthomas/log-forwarder/internal/config"
	"github.com/sanjuthomas/log-forwarder/internal/transform"
)

var builtinLayouts = []string{
	time.RFC3339Nano,
	time.RFC3339,
	"2006-01-02 15:04:05.000",
	"2006-01-02 15:04:05",
}

// Normalizer parses event timestamps and writes normalized UTC values on records.
type Normalizer struct {
	field    string
	format   string
	layouts  []string
	location *time.Location
	now      func() time.Time
}

// New constructs a timestamp normalizer, or nil when timestamp normalization is disabled.
func New(cfg config.TimestampConfig) (*Normalizer, error) {
	if !cfg.Enabled() {
		return nil, nil
	}

	loc, err := time.LoadLocation(cfg.DefaultTimezoneOrDefault())
	if err != nil {
		return nil, err
	}

	layouts := builtinLayouts
	if cfg.Format != "" {
		layouts = []string{cfg.Format}
	}

	return &Normalizer{
		field:    cfg.FieldOrDefault(),
		format:   cfg.Format,
		layouts:  layouts,
		location: loc,
		now:      time.Now,
	}, nil
}

// Normalize updates the configured timestamp field on record.
// It returns true when parsing failed and processing time was used instead.
func (n *Normalizer) Normalize(record transform.Record) bool {
	if n == nil {
		return false
	}

	raw, ok := record[n.field]
	if !ok {
		return n.applyFallback(record, "")
	}

	value, ok := raw.(string)
	if !ok || value == "" {
		return n.applyFallback(record, stringValue(raw))
	}

	parsed, ok := parseTimestamp(value, n.layouts, n.location)
	if !ok {
		return n.applyFallback(record, value)
	}

	record[n.field] = parsed.UTC().Format(time.RFC3339Nano)
	return false
}

func (n *Normalizer) applyFallback(record transform.Record, original string) bool {
	record[n.field] = n.now().UTC().Format(time.RFC3339Nano)
	if original != "" {
		record["timestamp_raw"] = original
	}
	record["timestamp_source"] = config.TimestampSourceProcessing
	return true
}

func parseTimestamp(value string, layouts []string, loc *time.Location) (time.Time, bool) {
	for _, layout := range layouts {
		if t, ok := parseWithLayout(value, layout, loc); ok {
			return t, true
		}
	}
	return time.Time{}, false
}

func parseWithLayout(value, layout string, loc *time.Location) (time.Time, bool) {
	if layoutHasZone(layout) {
		t, err := time.Parse(layout, value)
		if err != nil {
			return time.Time{}, false
		}
		return t, true
	}

	t, err := time.ParseInLocation(layout, value, loc)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

func layoutHasZone(layout string) bool {
	return strings.Contains(layout, "Z07:00") || strings.Contains(layout, "MST")
}

func stringValue(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}
