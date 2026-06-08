package config

import (
	"path/filepath"
	"testing"
)

func TestValidateLoggingFileInsideWatchPath(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	watchDir := filepath.Join(dir, "logs", "app")
	logFile := filepath.Join(watchDir, "forwarder.log")

	cfg := Default()
	cfg.Watch.Sources = []WatchSource{
		{Path: watchDir, Patterns: []string{"*.log"}},
	}
	cfg.Logging.File = logFile

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error when logging.file is inside watched directory")
	}
}

func TestValidateLoggingFileMatchesWatchPattern(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	watchDir := filepath.Join(dir, "logs")
	logFile := filepath.Join(watchDir, "agent.log")

	cfg := Default()
	cfg.Watch.Sources = []WatchSource{
		{Path: watchDir, Patterns: []string{"*.log"}},
	}
	cfg.Logging.File = logFile

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error when logging.file matches watch pattern")
	}
}

func TestValidateLoggingFileOutsideWatchPath(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	watchDir := filepath.Join(dir, "logs", "app")
	logFile := filepath.Join(dir, "forwarder.log")

	cfg := Default()
	cfg.Watch.Sources = []WatchSource{
		{Path: watchDir, Patterns: []string{"*.log"}},
	}
	cfg.Logging.File = logFile

	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestValidateStatePathInsideWatchPath(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	watchDir := filepath.Join(dir, "logs", "app")

	cfg := Default()
	cfg.Watch.Sources = []WatchSource{
		{Path: watchDir, Patterns: []string{"*.json"}},
	}
	cfg.Watch.State.Path = filepath.Join(watchDir, "watermarks.json")

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error when watch.state.path matches watch pattern")
	}
}

func TestPathInside(t *testing.T) {
	t.Parallel()

	if !pathInside("/var/log/app", "/var/log/app/forwarder.log") {
		t.Fatal("expected forwarder.log to be inside /var/log/app")
	}
	if pathInside("/var/log/app", "/var/log/other/forwarder.log") {
		t.Fatal("did not expect forwarder.log outside watch dir to match")
	}
}

func TestStatePathDefault(t *testing.T) {
	t.Parallel()

	cfg := Default()
	if cfg.StatePath() != ".log-forwarder/watermarks.json" {
		t.Fatalf("StatePath() = %q", cfg.StatePath())
	}
}

func TestValidateKafkaConnectTimeout(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.Kafka.ConnectTimeout = "not-a-duration"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error for invalid connect_timeout")
	}
}
