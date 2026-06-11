package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestReadinessConfigHelpers(t *testing.T) {
	t.Parallel()

	cfg := ReadinessConfig{Path: "readyz", BufferThreshold: 0.5}
	if cfg.ReadyPath() != "/readyz" {
		t.Fatalf("ReadyPath() = %q, want /readyz", cfg.ReadyPath())
	}
	if cfg.BufferThresholdOrDefault() != 0.5 {
		t.Fatalf("BufferThresholdOrDefault() = %v, want 0.5", cfg.BufferThresholdOrDefault())
	}

	defaultThreshold := ReadinessConfig{}.BufferThresholdOrDefault()
	if defaultThreshold != 0.8 {
		t.Fatalf("default threshold = %v, want 0.8", defaultThreshold)
	}

	disabled := false
	sinkCheckCfg := ReadinessConfig{SinkCheck: &disabled}
	if sinkCheckCfg.SinkCheckEnabled() {
		t.Fatal("expected sink_check false when explicitly disabled")
	}

	fallback := 3 * time.Second
	defaultCfg := ReadinessConfig{}
	if got := defaultCfg.SinkCheckTimeoutDuration(fallback); got != fallback {
		t.Fatalf("timeout = %v, want fallback %v", got, fallback)
	}
	timeoutCfg := ReadinessConfig{SinkCheckTimeout: "2s"}
	if got := timeoutCfg.SinkCheckTimeoutDuration(fallback); got != 2*time.Second {
		t.Fatalf("timeout = %v, want 2s", got)
	}
}

func TestMetricsConfigDeadLettersPath(t *testing.T) {
	t.Parallel()

	if got := (MetricsConfig{}).DeadLettersPath(); got != "/deadletters" {
		t.Fatalf("DeadLettersPath() = %q, want /deadletters", got)
	}
}

func TestMetricsConfigDefaults(t *testing.T) {
	t.Parallel()

	cfg := MetricsConfig{Enabled: true}
	if cfg.Addr() != "127.0.0.1:8080" {
		t.Fatalf("Addr() = %q, want 127.0.0.1:8080", cfg.Addr())
	}
	if cfg.MetricsPath() != "/metrics" {
		t.Fatalf("MetricsPath() = %q, want /metrics", cfg.MetricsPath())
	}
}

func TestMetricsConfigCustomPortAndPath(t *testing.T) {
	t.Parallel()

	cfg := MetricsConfig{
		Enabled: true,
		Host:    "0.0.0.0",
		Port:    9091,
		Path:    "custom-metrics",
	}
	if cfg.Addr() != "0.0.0.0:9091" {
		t.Fatalf("Addr() = %q, want 0.0.0.0:9091", cfg.Addr())
	}
	if cfg.MetricsPath() != "/custom-metrics" {
		t.Fatalf("MetricsPath() = %q, want /custom-metrics", cfg.MetricsPath())
	}
}

func TestValidateMetricsDisabled(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.Metrics.Port = -1
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestValidateMetricsEnabledRequiresValidPort(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.Metrics.Enabled = true
	cfg.Metrics.Port = 70000
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error for invalid metrics port")
	}
}

func TestValidateMetricsEnabledRejectsRootPath(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.Metrics.Enabled = true
	cfg.Metrics.Path = "/"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error for metrics path /")
	}
}

func TestValidateMetricsReadinessConfig(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.Metrics.Enabled = true
	cfg.Metrics.Readiness.Path = "/metrics"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error when readiness path equals metrics path")
	}

	cfg = Default()
	cfg.Metrics.Enabled = true
	cfg.Metrics.Readiness.BufferThreshold = 1.5
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for buffer_threshold > 1")
	}

	cfg = Default()
	cfg.Metrics.Enabled = true
	if cfg.Metrics.Readiness.ReadyPath() != "/ready" {
		t.Fatalf("ReadyPath() = %q, want /ready", cfg.Metrics.Readiness.ReadyPath())
	}
	if !cfg.Metrics.Readiness.SinkCheckEnabled() {
		t.Fatal("expected sink_check default true")
	}
}

func TestLoadConfigWithMetricsSection(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	content := `
watch:
  poll: 1s
  paths:
    - ./logs
  patterns:
    - "*.log"
sink:
  type: kafka
  kafka:
    brokers:
      - localhost:9092
    topic: logs
transform:
  type: tab
  on_error: wrap
pipeline:
  buffer_size: 128
  on_full: block
metrics:
  enabled: true
  host: 127.0.0.1
  port: 9091
  path: /metrics
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if !cfg.Metrics.Enabled {
		t.Fatal("expected metrics.enabled = true")
	}
	if cfg.Metrics.Port != 9091 {
		t.Fatalf("metrics.port = %d, want 9091", cfg.Metrics.Port)
	}
	if cfg.Metrics.Addr() != "127.0.0.1:9091" {
		t.Fatalf("metrics.Addr() = %q, want 127.0.0.1:9091", cfg.Metrics.Addr())
	}
}
