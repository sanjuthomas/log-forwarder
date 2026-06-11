// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package integration_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sanjuthomas/log-forwarder/internal/config"
	"github.com/sanjuthomas/log-forwarder/internal/sink"
)

func TestIntegration_ExampleConfigsRunEndToEnd(t *testing.T) {
	roots := []string{
		"../../configs",
		"../../examples/kafka",
	}

	var paths []string
	for _, root := range roots {
		matches, err := filepath.Glob(filepath.Join(root, "*.yaml"))
		if err != nil {
			t.Fatalf("Glob(%q) error = %v", root, err)
		}
		paths = append(paths, matches...)
	}
	if len(paths) == 0 {
		t.Fatal("no example configs found")
	}

	for _, path := range paths {
		path := path
		t.Run(filepath.Base(path), func(t *testing.T) {
			cfg, logFiles, sinkPath := prepareExampleConfigForE2E(t, path)
			for _, logFile := range logFiles {
				writeProbeLines(t, logFile, cfg)
			}

			fileSink, err := sink.New(config.SinkConfig{
				Type: "file",
				File: &config.FileSinkConfig{Path: sinkPath},
			})
			if err != nil {
				t.Fatalf("sink.New() error = %v", err)
			}

			startForwarder(t, cfg, harnessOptions{sink: fileSink})
			waitForRecordCount(t, sinkPath, 1)
		})
	}
}

func prepareExampleConfigForE2E(t *testing.T, configPath string) (*config.Config, []string, string) {
	t.Helper()

	cfg, err := config.Load(configPath)
	if err != nil {
		t.Fatalf("Load(%q) error = %v", configPath, err)
	}

	root := t.TempDir()
	sinkPath := filepath.Join(root, "out", "records.jsonl")
	if err := os.MkdirAll(filepath.Dir(sinkPath), 0o755); err != nil {
		t.Fatal(err)
	}

	cfg.Watch.State.Path = filepath.Join(root, "state", "watermarks.json")
	cfg.Sink = config.SinkConfig{
		Type: "file",
		File: &config.FileSinkConfig{Path: sinkPath},
	}
	cfg.Metrics.Enabled = false
	cfg.Logging.File = ""

	if cfg.Pipeline.PublishBatch.DeadLetter.Path != "" {
		cfg.Pipeline.PublishBatch.DeadLetter.Path = filepath.Join(root, "dlq")
	}

	logFiles := rewriteWatchPaths(cfg, root)
	return cfg, logFiles, sinkPath
}

func rewriteWatchPaths(cfg *config.Config, root string) []string {
	entries := cfg.Watch.Entries()
	newSources := make([]config.WatchSource, 0, len(entries))
	logFiles := make([]string, 0, len(entries))

	for i, src := range entries {
		dir := filepath.Join(root, "watch", fmt.Sprintf("%d", i))
		_ = os.MkdirAll(dir, 0o755)
		newSources = append(newSources, config.WatchSource{
			Path:     dir,
			Patterns: src.Patterns,
		})
		logFiles = append(logFiles, filepath.Join(dir, pickLogFileName(src.Patterns)))
	}

	cfg.Watch.Sources = newSources
	cfg.Watch.Paths = nil
	cfg.Watch.Patterns = nil
	return logFiles
}

func pickLogFileName(patterns []string) string {
	for _, p := range patterns {
		if strings.HasSuffix(p, ".log") && !strings.Contains(p, "*") {
			return p
		}
	}
	return "app.log"
}

func writeProbeLines(t *testing.T, logFile string, cfg *config.Config) {
	t.Helper()

	if err := os.MkdirAll(filepath.Dir(logFile), 0o755); err != nil {
		t.Fatal(err)
	}

	content := probeLineForConfig(cfg)
	if cfg.Parser.Type == "multiline" {
		content += "2026-06-08 10:16:23.901  INFO 18432 --- [main] c.example.App : flush-sentinel\n"
	}
	if err := os.WriteFile(logFile, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func probeLineForConfig(cfg *config.Config) string {
	level := "INFO"
	switch {
	case filterRequiresError(cfg):
		level = "ERROR"
	case filterRequiresWarnOrError(cfg):
		level = "WARN"
	}

	if cfg.Parser.Type == "multiline" {
		return fmt.Sprintf("2026-06-08 10:16:22.901  %s 18432 --- [main] c.example.App : e2e-probe\n", level)
	}

	if cfg.Transform.Type == "regex" && strings.Contains(cfg.Transform.Pattern, `(?P<level>`) &&
		!strings.Contains(cfg.Transform.Pattern, `\t`) {
		return fmt.Sprintf("2024-01-01T00:00:00Z %s e2e-probe\n", level)
	}

	return fmt.Sprintf("2024-01-01T00:00:00Z\t%s\te2e-probe\n", level)
}

func filterRequiresError(cfg *config.Config) bool {
	for _, rule := range cfg.Filter.Rules {
		if rule.Type != "field" || rule.Field != "level" {
			continue
		}
		if rule.Op == "in" && len(rule.Values) == 1 && strings.EqualFold(rule.Values[0], "ERROR") {
			return true
		}
	}
	return false
}

func filterRequiresWarnOrError(cfg *config.Config) bool {
	for _, rule := range cfg.Filter.Rules {
		if rule.Type != "field" || rule.Field != "level" || rule.Op != "in" {
			continue
		}
		hasWarn, hasError := false, false
		for _, v := range rule.Values {
			if strings.EqualFold(v, "WARN") {
				hasWarn = true
			}
			if strings.EqualFold(v, "ERROR") {
				hasError = true
			}
		}
		if hasWarn && hasError {
			return true
		}
	}
	return false
}
