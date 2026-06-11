// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package enrich

import (
	"os"
	"strings"
	"testing"

	"github.com/sanjuthomas/log-forwarder/internal/config"
	"github.com/sanjuthomas/log-forwarder/internal/transform"
)

func TestApplyEmptyChain(t *testing.T) {
	t.Parallel()

	record := transform.Record{"message": "hello"}
	got := Apply(nil, record)
	if got["message"] != "hello" {
		t.Fatalf("Apply(nil) = %v, want original record", got)
	}
}

func TestNewChainStaticAndHost(t *testing.T) {
	t.Parallel()

	chain, err := NewChain([]config.EnricherConfig{
		{Type: "static", Fields: map[string]string{"environment": "test"}},
		{Type: "host"},
	})
	if err != nil {
		t.Fatalf("NewChain() error = %v", err)
	}
	if len(chain) != 2 {
		t.Fatalf("len(chain) = %d, want 2", len(chain))
	}

	record := Apply(chain, transform.Record{"message": "hello"})
	if record["environment"] != "test" {
		t.Fatalf("environment = %q, want test", record["environment"])
	}
	if record["message"] != "hello" {
		t.Fatalf("message = %q, want hello", record["message"])
	}

	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}
	if record["hostname"] != hostname {
		t.Fatalf("hostname = %q, want %q", record["hostname"], hostname)
	}
}

func TestNewChainUnknownType(t *testing.T) {
	t.Parallel()

	_, err := NewChain([]config.EnricherConfig{{Type: "region"}})
	if err == nil {
		t.Fatal("expected error for unknown enricher type")
	}
	if !strings.Contains(err.Error(), "unknown enricher type") {
		t.Fatalf("error = %q", err.Error())
	}
}

func TestRegisterCustomEnricher(t *testing.T) {
	const name = "custom-enricher-test"

	Register(name, func(cfg config.EnricherConfig) (Enricher, error) {
		return &staticEnricher{fields: cfg.Fields}, nil
	})
	t.Cleanup(func() {
		delete(registry, name)
	})

	chain, err := NewChain([]config.EnricherConfig{
		{Type: name, Fields: map[string]string{"custom": "yes"}},
	})
	if err != nil {
		t.Fatalf("NewChain() error = %v", err)
	}
	record := Apply(chain, transform.Record{})
	if record["custom"] != "yes" {
		t.Fatalf("custom = %q, want yes", record["custom"])
	}
}

func TestStaticEnricherOverwritesExistingFields(t *testing.T) {
	t.Parallel()

	e, err := newStaticEnricher(config.EnricherConfig{
		Fields: map[string]string{"level": "INFO", "region": "us-east-1"},
	})
	if err != nil {
		t.Fatalf("newStaticEnricher() error = %v", err)
	}

	record := e.Enrich(transform.Record{"level": "ERROR"})
	if record["level"] != "INFO" {
		t.Fatalf("level = %q, want INFO", record["level"])
	}
	if record["region"] != "us-east-1" {
		t.Fatalf("region = %q, want us-east-1", record["region"])
	}
}
