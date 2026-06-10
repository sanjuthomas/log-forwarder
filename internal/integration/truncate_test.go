package integration_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sanjuthomas/log-forwarder/internal/config"
)

func TestE2E_OversizedMessageTruncated(t *testing.T) {
	logDir, sinkPath, statePath := setupDirs(t)
	logFile := filepath.Join(logDir, "app.log")

	hugeMsg := strings.Repeat("x", 2*1024*1024)
	line := fmt.Sprintf("2026-06-08T10:00:00Z\tINFO\t%s\n", hugeMsg)
	if err := os.WriteFile(logFile, []byte(line), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := tabLineConfig(logDir, sinkPath, statePath, "wrap", config.FilterConfig{})
	cfg.Pipeline.MaxPublishBytes = 8192

	startForwarder(t, cfg, harnessOptions{})
	waitForRecordCount(t, sinkPath, 1)

	records := readJSONLRecords(t, sinkPath)
	if records[0]["message_truncated"] != true {
		t.Fatalf("message_truncated = %v, want true", records[0]["message_truncated"])
	}
	if records[0]["publish_truncated"] != true {
		t.Fatalf("publish_truncated = %v, want true", records[0]["publish_truncated"])
	}
	origBytes, ok := records[0]["message_original_bytes"].(float64)
	if !ok || int(origBytes) != len(hugeMsg) {
		t.Fatalf("message_original_bytes = %v, want %d", records[0]["message_original_bytes"], len(hugeMsg))
	}
}
