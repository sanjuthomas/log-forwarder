// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package sink

import (
	"strings"
	"testing"

	"github.com/sanjuthomas/log-forwarder/internal/config"
)

func TestNewDefaultKafkaSinkType(t *testing.T) {
	t.Parallel()

	s, err := New(config.SinkConfig{
		Kafka: &config.KafkaConfig{
			Brokers: []string{"localhost:9092"},
			Topic:   "logs",
		},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestNewKafkaSinkRequiresConfig(t *testing.T) {
	t.Parallel()

	_, err := New(config.SinkConfig{Type: "kafka", Kafka: nil})
	if err == nil {
		t.Fatal("expected error when sink.kafka is missing")
	}
	if !strings.Contains(err.Error(), "sink.kafka is required") {
		t.Fatalf("error = %q", err.Error())
	}
}

func TestNewUnknownSinkType(t *testing.T) {
	t.Parallel()

	_, err := New(config.SinkConfig{Type: "unknown-sink-typo"})
	if err == nil {
		t.Fatal("expected error for unknown sink type")
	}
	if !strings.Contains(err.Error(), "unknown sink type") {
		t.Fatalf("error = %q", err.Error())
	}
}

func TestRegisterCustomSink(t *testing.T) {
	const name = "custom-sink-factory-test"

	Register(name, func(cfg config.SinkConfig) (Sink, error) {
		return New(config.SinkConfig{
			Type: "file",
			File: cfg.File,
		})
	})
	t.Cleanup(func() {
		delete(registry, name)
	})

	_, err := New(config.SinkConfig{Type: name, File: &config.FileSinkConfig{Path: t.TempDir() + "/out.jsonl"}})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
}
