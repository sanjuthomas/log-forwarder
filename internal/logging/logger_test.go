package logging

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sanjuthomas/log-forwarder/internal/config"
)

func TestNewStderrTextLogger(t *testing.T) {
	t.Parallel()

	logger, closer, err := New(config.LoggingConfig{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := closer.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if logger == nil {
		t.Fatal("expected non-nil logger")
	}
}

func TestNewJSONLoggerToFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	logPath := filepath.Join(dir, "nested", "forwarder.log")

	logger, closer, err := New(config.LoggingConfig{
		Level:  "debug",
		Format: "json",
		File:   logPath,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	logger.Info("startup check")
	if err := closer.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	body := string(data)
	if !strings.Contains(body, "startup check") {
		t.Fatalf("log file = %q, want startup message", body)
	}
	if !strings.Contains(body, `"msg"`) {
		t.Fatalf("log file = %q, want JSON format", body)
	}
}

func TestNewWarnLevel(t *testing.T) {
	t.Parallel()

	logger, closer, err := New(config.LoggingConfig{Level: "warn", Format: "text"})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := closer.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if logger == nil {
		t.Fatal("expected non-nil logger")
	}
}

func TestNewInvalidLogFilePath(t *testing.T) {
	t.Parallel()

	_, _, err := New(config.LoggingConfig{File: "/dev/null/impossible/forwarder.log"})
	if err == nil {
		t.Fatal("expected error when log file cannot be created")
	}
}
