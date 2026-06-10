package integration_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sanjuthomas/log-forwarder/internal/config"
)

type alwaysFailSink struct {
	calls int32
}

func (s *alwaysFailSink) Publish(_ context.Context, _ []byte) error {
	atomic.AddInt32(&s.calls, 1)
	return errors.New("publish failed")
}

func (s *alwaysFailSink) Close() error { return nil }

func waitForPublishAttempts(t *testing.T, sink *alwaysFailSink, want int32) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&sink.calls) >= want {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("publish attempts = %d, want at least %d", atomic.LoadInt32(&sink.calls), want)
}

func TestE2E_WatermarkStallsWhenPublishFails(t *testing.T) {
	logDir, sinkPath, statePath := setupDirs(t)
	logFile := filepath.Join(logDir, "app.log")

	if err := os.WriteFile(logFile, []byte("2024-01-01T00:00:00Z\tINFO\tstuck-line\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := tabLineRegexConfig(logDir, sinkPath, statePath, "wrap")
	cfg.Pipeline.PublishRetry = config.PublishRetryConfig{
		InitialBackoff: "5ms",
		MaxBackoff:     "20ms",
		MaxAttempts:    2,
	}

	failSink := &alwaysFailSink{}
	h := startForwarder(t, cfg, harnessOptions{sink: failSink})
	waitForPublishAttempts(t, failSink, 2)

	exists, err := watermarkEntryExists(statePath, logFile)
	if err != nil {
		t.Fatalf("watermarkEntryExists() error = %v", err)
	}
	if exists {
		t.Fatal("watermark must not advance when publish fails")
	}
	h.stop(t)
}

func TestE2E_WatermarkResumesWithoutDuplicateAfterPublishFailure(t *testing.T) {
	logDir, sinkPath, statePath := setupDirs(t)
	logFile := filepath.Join(logDir, "app.log")

	line1 := "2024-01-01T00:00:00Z\tINFO\tline-one\n"
	line2 := "2024-01-01T00:00:01Z\tINFO\tline-two\n"
	if err := os.WriteFile(logFile, []byte(line1), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := tabLineRegexConfig(logDir, sinkPath, statePath, "wrap")
	cfg.Pipeline.PublishRetry = config.PublishRetryConfig{
		InitialBackoff: "5ms",
		MaxBackoff:     "20ms",
		MaxAttempts:    0,
	}

	h1 := startForwarder(t, cfg, harnessOptions{})
	waitForRecordCount(t, sinkPath, 1)
	h1.stop(t)

	wmAfterLineOne := readWatermarkOffset(t, statePath, logFile)

	appendToFile(t, logFile, line2)

	cfg.Pipeline.PublishRetry.MaxAttempts = 2
	failSink := &alwaysFailSink{}
	h2 := startForwarder(t, cfg, harnessOptions{sink: failSink})
	waitForPublishAttempts(t, failSink, 2)

	if offset := readWatermarkOffset(t, statePath, logFile); offset != wmAfterLineOne {
		t.Fatalf("watermark advanced during publish failure: got %d, want %d", offset, wmAfterLineOne)
	}
	h2.stop(t)

	startForwarder(t, cfg, harnessOptions{})
	waitForRecordCount(t, sinkPath, 2)

	records := readJSONLRecords(t, sinkPath)
	if len(records) != 2 {
		t.Fatalf("len(records) = %d, want 2", len(records))
	}
	if records[0]["message"] != "line-one" || records[1]["message"] != "line-two" {
		t.Fatalf("records = [%v, %v]", records[0]["message"], records[1]["message"])
	}
}
