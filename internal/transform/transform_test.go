// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package transform

import (
	"strings"
	"testing"

	"github.com/sanjuthomas/log-forwarder/internal/config"
)

func TestDelimiterTransformerNamedColumns(t *testing.T) {
	t.Parallel()

	tr, err := New(config.TransformConfig{
		Type:      "delimiter",
		Delimiter: "\t",
		Columns:   []string{"timestamp", "level", "message"},
	})
	if err != nil {
		t.Fatal(err)
	}

	record, err := tr.Transform([]byte("2024-01-01T00:00:00Z\tINFO\tstarted"))
	if err != nil {
		t.Fatal(err)
	}

	if record["timestamp"] != "2024-01-01T00:00:00Z" {
		t.Fatalf("timestamp = %v", record["timestamp"])
	}
	if record["level"] != "INFO" {
		t.Fatalf("level = %v, want INFO", record["level"])
	}
	if record["message"] != "started" {
		t.Fatalf("message = %v, want started", record["message"])
	}
}

func TestDelimiterTransformerDefaultTab(t *testing.T) {
	t.Parallel()

	tr, err := New(config.TransformConfig{
		Type:    "delimiter",
		Columns: []string{"a", "b"},
	})
	if err != nil {
		t.Fatal(err)
	}

	record, err := tr.Transform([]byte("one\ttwo"))
	if err != nil {
		t.Fatal(err)
	}

	if record["a"] != "one" || record["b"] != "two" {
		t.Fatalf("record = %v", record)
	}
}

func TestDelimiterTransformerCustomDelimiter(t *testing.T) {
	t.Parallel()

	tr, err := New(config.TransformConfig{
		Type:      "delimiter",
		Delimiter: "|",
		Columns:   []string{"timestamp", "level", "message"},
	})
	if err != nil {
		t.Fatal(err)
	}

	record, err := tr.Transform([]byte("2024-01-01T00:00:00Z|WARN|disk full"))
	if err != nil {
		t.Fatal(err)
	}

	if record["level"] != "WARN" {
		t.Fatalf("level = %v, want WARN", record["level"])
	}
	if record["message"] != "disk full" {
		t.Fatalf("message = %v, want disk full", record["message"])
	}
}

func TestDelimiterTransformerUnnamedColumns(t *testing.T) {
	t.Parallel()

	tr, err := New(config.TransformConfig{Type: "delimiter", Delimiter: ","})
	if err != nil {
		t.Fatal(err)
	}

	record, err := tr.Transform([]byte("a,b,c"))
	if err != nil {
		t.Fatal(err)
	}

	if record["field_0"] != "a" || record["field_1"] != "b" || record["field_2"] != "c" {
		t.Fatalf("record = %v", record)
	}
}

func TestDelimiterTransformerExtraColumns(t *testing.T) {
	t.Parallel()

	tr, err := New(config.TransformConfig{
		Type:      "delimiter",
		Delimiter: "\t",
		Columns:   []string{"level", "message"},
	})
	if err != nil {
		t.Fatal(err)
	}

	record, err := tr.Transform([]byte("INFO\tstarted\textra"))
	if err != nil {
		t.Fatal(err)
	}

	if record["level"] != "INFO" || record["message"] != "started" {
		t.Fatalf("record = %v", record)
	}
	if record["field_2"] != "extra" {
		t.Fatalf("field_2 = %v, want extra", record["field_2"])
	}
}

func TestDelimiterTransformerEmptyLine(t *testing.T) {
	t.Parallel()

	tr, err := New(config.TransformConfig{Type: "delimiter"})
	if err != nil {
		t.Fatal(err)
	}

	_, err = tr.Transform([]byte("   "))
	if err == nil {
		t.Fatal("expected error for empty line")
	}
}

func TestTabTransformerAlias(t *testing.T) {
	t.Parallel()

	tab, err := New(config.TransformConfig{
		Type:    "tab",
		Columns: []string{"a", "b"},
	})
	if err != nil {
		t.Fatal(err)
	}

	delimiter, err := New(config.TransformConfig{
		Type:      "delimiter",
		Delimiter: "\t",
		Columns:   []string{"a", "b"},
	})
	if err != nil {
		t.Fatal(err)
	}

	line := []byte("x\ty")
	tabRecord, err := tab.Transform(line)
	if err != nil {
		t.Fatal(err)
	}
	delimiterRecord, err := delimiter.Transform(line)
	if err != nil {
		t.Fatal(err)
	}

	if tabRecord["a"] != delimiterRecord["a"] || tabRecord["b"] != delimiterRecord["b"] {
		t.Fatalf("tab alias mismatch: tab=%v delimiter=%v", tabRecord, delimiterRecord)
	}
}

func TestRegexTransformerNamedGroups(t *testing.T) {
	t.Parallel()

	tr, err := New(config.TransformConfig{
		Type:    "regex",
		Pattern: `^(?P<level>\S+)\s+(?P<message>.*)$`,
	})
	if err != nil {
		t.Fatal(err)
	}

	record, err := tr.Transform([]byte("ERROR something failed"))
	if err != nil {
		t.Fatal(err)
	}

	if record["level"] != "ERROR" {
		t.Fatalf("level = %v, want ERROR", record["level"])
	}
	if record["message"] != "something failed" {
		t.Fatalf("message = %v, want something failed", record["message"])
	}
}

func TestRegexTransformerMultipleFields(t *testing.T) {
	t.Parallel()

	tr, err := New(config.TransformConfig{
		Type:    "regex",
		Pattern: `^(?P<timestamp>\S+)\s+(?P<level>\S+)\s+(?P<message>.*)$`,
	})
	if err != nil {
		t.Fatal(err)
	}

	record, err := tr.Transform([]byte("2024-01-01T00:00:00Z INFO service started"))
	if err != nil {
		t.Fatal(err)
	}

	if record["timestamp"] != "2024-01-01T00:00:00Z" {
		t.Fatalf("timestamp = %v", record["timestamp"])
	}
	if record["level"] != "INFO" {
		t.Fatalf("level = %v", record["level"])
	}
	if record["message"] != "service started" {
		t.Fatalf("message = %v", record["message"])
	}
}

func TestRegexTransformerSpringBootDefault(t *testing.T) {
	t.Parallel()

	tr, err := New(config.TransformConfig{
		Type:    "regex",
		Pattern: `^(?s)(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3})\s+(?P<level>\S+)\s+(?P<pid>\d+)\s+---\s+\[\s*(?P<thread>[^\]]+?)\s*\]\s+(?P<logger>\S+)\s+:\s+(?P<message>.*)$`,
	})
	if err != nil {
		t.Fatal(err)
	}

	line := "2026-06-08 10:15:23.456  INFO 18432 --- [           main] com.acme.billing.BillingApplication      : Starting BillingApplication v1.4.2 using Java 17.0.10"
	record, err := tr.Transform([]byte(line))
	if err != nil {
		t.Fatal(err)
	}

	if record["timestamp"] != "2026-06-08 10:15:23.456" {
		t.Fatalf("timestamp = %v", record["timestamp"])
	}
	if record["level"] != "INFO" {
		t.Fatalf("level = %v", record["level"])
	}
	if record["pid"] != "18432" {
		t.Fatalf("pid = %v", record["pid"])
	}
	if record["thread"] != "main" {
		t.Fatalf("thread = %v", record["thread"])
	}
	if record["logger"] != "com.acme.billing.BillingApplication" {
		t.Fatalf("logger = %v", record["logger"])
	}
	if record["message"] != "Starting BillingApplication v1.4.2 using Java 17.0.10" {
		t.Fatalf("message = %v", record["message"])
	}
}

func TestRegexTransformerSpringBootMultilineMessage(t *testing.T) {
	t.Parallel()

	tr, err := New(config.TransformConfig{
		Type:    "regex",
		Pattern: `^(?s)(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3})\s+(?P<level>\S+)\s+(?P<pid>\d+)\s+---\s+\[\s*(?P<thread>[^\]]+?)\s*\]\s+(?P<logger>\S+)\s+:\s+(?P<message>.*)$`,
	})
	if err != nil {
		t.Fatal(err)
	}

	line := "2026-06-08 10:16:22.901  ERROR 18432 --- [nio-8080-exec-5] c.a.b.controller.PaymentController       : Payment failed\norg.springframework.dao.DataIntegrityViolationException: could not execute statement\n\tat com.acme.billing.controller.PaymentController.processPayment(PaymentController.java:87)"
	record, err := tr.Transform([]byte(line))
	if err != nil {
		t.Fatal(err)
	}

	if record["level"] != "ERROR" {
		t.Fatalf("level = %v", record["level"])
	}
	msg, ok := record["message"].(string)
	if !ok {
		t.Fatalf("message type = %T", record["message"])
	}
	if !strings.Contains(msg, "Payment failed") {
		t.Fatalf("message = %q", msg)
	}
	if !strings.Contains(msg, "DataIntegrityViolationException") {
		t.Fatalf("message = %q", msg)
	}
	if !strings.Contains(msg, "processPayment") {
		t.Fatalf("message = %q", msg)
	}
}

func TestRegexTransformerNoMatch(t *testing.T) {
	t.Parallel()

	tr, err := New(config.TransformConfig{
		Type:    "regex",
		Pattern: `^(?P<level>\S+)\s+(?P<message>.*)$`,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = tr.Transform([]byte(""))
	if err == nil {
		t.Fatal("expected error when line does not match pattern")
	}
	if !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("error = %v, want match failure", err)
	}
}

func TestRegexTransformerInvalidPattern(t *testing.T) {
	t.Parallel()

	_, err := New(config.TransformConfig{
		Type:    "regex",
		Pattern: `(?P<level>\`,
	})
	if err == nil {
		t.Fatal("expected error for invalid regex pattern")
	}
}

func TestRegexTransformerMissingPattern(t *testing.T) {
	t.Parallel()

	_, err := New(config.TransformConfig{Type: "regex"})
	if err == nil {
		t.Fatal("expected error when pattern is missing")
	}
}

func TestNewUnknownTransformer(t *testing.T) {
	t.Parallel()

	_, err := New(config.TransformConfig{Type: "unknown"})
	if err == nil {
		t.Fatal("expected error for unknown transformer type")
	}
}
