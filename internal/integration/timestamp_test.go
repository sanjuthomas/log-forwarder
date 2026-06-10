package integration_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sanjuthomas/log-forwarder/internal/config"
)

func TestE2E_SpringBootTimestampNormalizedToUTC(t *testing.T) {
	logDir, sinkPath, statePath := setupDirs(t)
	logFile := filepath.Join(logDir, "app.log")

	content := strings.Join([]string{
		"2026-06-08 10:16:22.901  INFO 18432 --- [main] c.example.App : hello integration",
		"2026-06-08 10:16:23.901  INFO 18432 --- [main] c.example.App : flush-sentinel",
	}, "\n") + "\n"
	if err := os.WriteFile(logFile, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := springBootConfig(logDir, sinkPath, statePath)
	cfg.Timestamp = config.TimestampConfig{
		Field:  "timestamp",
		Format: "2006-01-02 15:04:05.000",
	}

	startForwarder(t, cfg, harnessOptions{})
	waitForRecordCount(t, sinkPath, 1)

	records := readJSONLRecords(t, sinkPath)
	want := time.Date(2026, 6, 8, 10, 16, 22, 901000000, time.UTC).Format(time.RFC3339Nano)
	if records[0]["timestamp"] != want {
		t.Fatalf("timestamp = %v, want %q", records[0]["timestamp"], want)
	}
	if _, ok := records[0]["timestamp_source"]; ok {
		t.Fatal("did not expect timestamp_source on successful parse")
	}
}
