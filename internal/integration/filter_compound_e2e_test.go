package integration_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sanjuthomas/log-forwarder/internal/config"
)

func TestE2E_FilterCompoundAndNotIn(t *testing.T) {
	logDir, sinkPath, statePath := setupDirs(t)
	logFile := filepath.Join(logDir, "app.log")

	content := strings.Join([]string{
		"2024-01-01T00:00:00Z\tDEBUG\tbilling\tdebug-drop",
		"2024-01-01T00:00:01Z\tINFO\tbilling\tinfo-keep",
		"2024-01-01T00:00:02Z\tWARN\tlegacy\twarn-legacy-drop",
		"2024-01-01T00:00:03Z\tERROR\tbilling\terror-keep",
	}, "\n") + "\n"
	if err := os.WriteFile(logFile, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := tabLineConfig(logDir, sinkPath, statePath, "wrap", config.FilterConfig{
		Match: "all",
		Rules: []config.FilterRuleConfig{
			{
				Type:  "compound",
				Match: "any",
				Rules: []config.FilterRuleConfig{
					{Type: "field", Field: "level", Op: "eq", Value: "INFO", IgnoreCase: true},
					{Type: "field", Field: "level", Op: "eq", Value: "ERROR", IgnoreCase: true},
				},
			},
			{Type: "field", Field: "level", Op: "not_in", Values: []string{"DEBUG", "TRACE"}, IgnoreCase: true},
			{Type: "field", Field: "service", Op: "neq", Value: "legacy", IgnoreCase: true},
		},
	})
	cfg.Transform = config.TransformConfig{
		Type:      "delimiter",
		Delimiter: "\t",
		Columns:   []string{"timestamp", "level", "service", "message"},
		OnError:   "wrap",
	}

	startForwarder(t, cfg, harnessOptions{})
	waitForRecordCount(t, sinkPath, 2)

	records := readJSONLRecords(t, sinkPath)
	messages := sinkMessages(records)
	if !containsAll(messages, "info-keep", "error-keep") {
		t.Fatalf("messages = %v, want info-keep and error-keep", messages)
	}
	if containsAny(messages, "debug-drop", "warn-legacy-drop") {
		t.Fatalf("unexpected filtered messages in sink: %v", messages)
	}
}

func TestE2E_FilterOnMissingPassPublishesWrapped(t *testing.T) {
	logDir, sinkPath, statePath := setupDirs(t)
	logFile := filepath.Join(logDir, "app.log")

	if err := os.WriteFile(logFile, []byte("not-a-valid-line\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := tabLineRegexConfig(logDir, sinkPath, statePath, "wrap")
	cfg.Filter = config.FilterConfig{
		OnMissing: "pass",
		Match:     "all",
		Rules: []config.FilterRuleConfig{
			{Type: "field", Field: "level", Op: "eq", Value: "INFO", IgnoreCase: true},
		},
	}

	startForwarder(t, cfg, harnessOptions{})
	waitForRecordCount(t, sinkPath, 1)

	records := readJSONLRecords(t, sinkPath)
	if records[0]["_raw"] != "not-a-valid-line" {
		t.Fatalf("_raw = %v", records[0]["_raw"])
	}
}

func containsAny(messages []string, vals ...string) bool {
	for _, msg := range messages {
		for _, v := range vals {
			if msg == v {
				return true
			}
		}
	}
	return false
}
