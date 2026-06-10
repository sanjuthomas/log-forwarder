package integration_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sanjuthomas/log-forwarder/internal/config"
)

func TestE2E_PublishBatchBurstToFileSink(t *testing.T) {
	logDir, sinkPath, statePath := setupDirs(t)
	logFile := filepath.Join(logDir, "app.log")

	lines := make([]string, 5)
	for i := range lines {
		lines[i] = fmt.Sprintf("2026-06-08T10:00:0%dZ\tINFO\tbatch-line-%d", i, i)
	}
	content := strings.Join(lines, "\n") + "\n"
	if err := os.WriteFile(logFile, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := tabLineConfig(logDir, sinkPath, statePath, "wrap", config.FilterConfig{})
	cfg.Pipeline.PublishBatch = config.PublishBatchConfig{
		MaxBytes:      400,
		FlushInterval: "50ms",
	}

	startForwarder(t, cfg, harnessOptions{})
	waitForRecordCount(t, sinkPath, 5)

	records := readJSONLRecords(t, sinkPath)
	if len(records) != 5 {
		t.Fatalf("record count = %d, want 5", len(records))
	}
}
