package integration_test

import (
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/sanjuthomas/log-forwarder/internal/config"
)

func TestE2E_FilterErrorsOnlyToFileSink(t *testing.T) {
	logDir, sinkPath, statePath := setupDirs(t)
	logFile := filepath.Join(logDir, "app.log")

	content := strings.Join([]string{
		"2024-01-01T00:00:00Z\tINFO\tnoise-one",
		"2024-01-01T00:00:01Z\tWARN\tnoise-two",
		"2024-01-01T00:00:02Z\tERROR\tkeep-me",
	}, "\n") + "\n"
	if err := os.WriteFile(logFile, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := tabLineConfig(logDir, sinkPath, statePath, "wrap", errorOnlyFilter())
	startForwarder(t, cfg, harnessOptions{})
	waitForRecordCount(t, sinkPath, 1)

	records := readJSONLRecords(t, sinkPath)
	if len(records) != 1 {
		t.Fatalf("len(records) = %d, want 1", len(records))
	}
	if records[0]["level"] != "ERROR" {
		t.Fatalf("level = %v, want ERROR", records[0]["level"])
	}
	if records[0]["message"] != "keep-me" {
		t.Fatalf("message = %v, want keep-me", records[0]["message"])
	}
}

func TestE2E_FilterMatchAnyPassesWarnAndError(t *testing.T) {
	logDir, sinkPath, statePath := setupDirs(t)
	logFile := filepath.Join(logDir, "app.log")

	content := strings.Join([]string{
		"2024-01-01T00:00:00Z\tINFO\tdrop",
		"2024-01-01T00:00:01Z\tWARN\tkeep-warn",
		"2024-01-01T00:00:02Z\tERROR\tkeep-error",
	}, "\n") + "\n"
	if err := os.WriteFile(logFile, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := tabLineConfig(logDir, sinkPath, statePath, "wrap", config.FilterConfig{
		Match: "any",
		Rules: []config.FilterRuleConfig{
			{Type: "field", Field: "level", Op: "eq", Value: "WARN", IgnoreCase: true},
			{Type: "field", Field: "level", Op: "eq", Value: "ERROR", IgnoreCase: true},
		},
	})
	startForwarder(t, cfg, harnessOptions{})
	waitForRecordCount(t, sinkPath, 2)

	records := readJSONLRecords(t, sinkPath)
	if len(records) != 2 {
		t.Fatalf("len(records) = %d, want 2", len(records))
	}
	levels := []string{records[0]["level"].(string), records[1]["level"].(string)}
	if !containsAll(levels, "WARN", "ERROR") {
		t.Fatalf("levels = %v", levels)
	}
}

func TestE2E_FilterIncrementsMetricsCounter(t *testing.T) {
	logDir, sinkPath, statePath := setupDirs(t)
	logFile := filepath.Join(logDir, "app.log")

	content := strings.Join([]string{
		"2024-01-01T00:00:00Z\tINFO\tdrop-one",
		"2024-01-01T00:00:01Z\tWARN\tdrop-two",
		"2024-01-01T00:00:02Z\tERROR\tkeep",
	}, "\n") + "\n"
	if err := os.WriteFile(logFile, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := tabLineConfig(logDir, sinkPath, statePath, "wrap", errorOnlyFilter())
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	metricsPort := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()

	startForwarder(t, cfg, harnessOptions{metricsEnabled: true, metricsPort: metricsPort})
	waitForRecordCount(t, sinkPath, 1)

	base := "http://127.0.0.1:" + strconv.Itoa(metricsPort)
	resp, err := http.Get(base + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics error = %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	metricsBody := string(body)
	if !strings.Contains(metricsBody, "log_forwarder_lines_filtered") {
		t.Fatalf("metrics missing lines_filtered: %s", metricsBody)
	}
	if !strings.Contains(metricsBody, "log_forwarder_lines_published") {
		t.Fatalf("metrics missing lines_published: %s", metricsBody)
	}
}

func TestE2E_FilterWatermarkAdvancesOnFilteredLines(t *testing.T) {
	logDir, sinkPath, statePath := setupDirs(t)
	logFile := filepath.Join(logDir, "app.log")

	if err := os.WriteFile(logFile, []byte("2024-01-01T00:00:00Z\tINFO\tfiltered-out\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := tabLineConfig(logDir, sinkPath, statePath, "wrap", errorOnlyFilter())
	startForwarder(t, cfg, harnessOptions{})

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		got, err := countJSONLRecords(sinkPath)
		if err != nil {
			t.Fatalf("countJSONLRecords() error = %v", err)
		}
		if got == 0 {
			data, err := os.ReadFile(statePath)
			if err == nil {
				var state struct {
					Files map[string]struct {
						Offset int64 `json:"offset"`
					} `json:"files"`
				}
				if err := json.Unmarshal(data, &state); err == nil {
					if entry, ok := state.Files[logFile]; ok && entry.Offset > 0 {
						return
					}
				}
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("timeout waiting for watermark advance on filtered line")
}

func TestE2E_FilterOnMissingDropExcludesWrappedRecords(t *testing.T) {
	logDir, sinkPath, statePath := setupDirs(t)
	logFile := filepath.Join(logDir, "app.log")

	if err := os.WriteFile(logFile, []byte("not-a-valid-line\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := tabLineConfig(logDir, sinkPath, statePath, "wrap", errorOnlyFilter())
	startForwarder(t, cfg, harnessOptions{})

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		got, err := countJSONLRecords(sinkPath)
		if err != nil {
			t.Fatalf("countJSONLRecords() error = %v", err)
		}
		if got == 0 {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("expected wrapped record without level field to be filtered out")
}
