package config

import (
	"os"
	"path/filepath"
	"testing"
)

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
