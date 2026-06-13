// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package sink

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/sanjuthomas/log-forwarder/internal/config"
	"github.com/segmentio/kafka-go"
)

func TestFileSinkCheckAndEmptyBatch(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "out.jsonl")

	s, err := New(config.SinkConfig{
		Type: "file",
		File: &config.FileSinkConfig{Path: path},
	})
	if err != nil {
		t.Fatal(err)
	}

	checker, ok := s.(Checker)
	if !ok {
		t.Fatal("expected file sink to implement Checker")
	}
	if err := checker.Check(context.Background()); err != nil {
		t.Fatalf("Check() error = %v", err)
	}

	batchSink := s.(BatchSink)
	if err := batchSink.PublishBatch(context.Background(), nil); err != nil {
		t.Fatalf("PublishBatch(empty) error = %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestHTTPNoauthSinkClose(t *testing.T) {
	t.Parallel()

	s, err := New(config.SinkConfig{
		Type:       "http-noauth",
		HTTPNoauth: &config.HTTPNoauthSinkConfig{URL: "http://127.0.0.1:1/unused"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestKafkaSinkCheckNoBrokers(t *testing.T) {
	t.Parallel()

	s := &kafkaSink{
		cfg:    config.KafkaConfig{Brokers: nil, Topic: "logs"},
		dialer: kafka.DefaultDialer,
	}
	if err := s.Check(context.Background()); err == nil {
		t.Fatal("expected error when no brokers are configured")
	}
}

func TestKafkaSinkCheckUnreachableBroker(t *testing.T) {
	t.Parallel()

	s, err := New(config.SinkConfig{
		Type: "kafka",
		Kafka: &config.KafkaConfig{
			Brokers: []string{"127.0.0.1:1"},
			Topic:   "logs",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	checker := s.(Checker)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if err := checker.Check(ctx); err == nil {
		t.Fatal("expected error when broker is unreachable")
	}
}

func TestBuildDialerSSL(t *testing.T) {
	t.Parallel()

	dialer, err := buildDialer(config.KafkaConfig{
		Brokers: []string{"kafka.example.com:9093"},
		Topic:   "logs",
		Security: &config.KafkaSecurityConfig{
			Protocol: config.KafkaProtocolSSL,
			TLS:      &config.KafkaTLSConfig{InsecureSkipVerify: true},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if dialer.TLS == nil {
		t.Fatal("expected TLS config on SSL dialer")
	}
}

func TestBuildDialerUnsupportedProtocol(t *testing.T) {
	t.Parallel()

	_, err := buildDialer(config.KafkaConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "logs",
		Security: &config.KafkaSecurityConfig{
			Protocol: "UNKNOWN",
		},
	})
	if err == nil {
		t.Fatal("expected error for unsupported protocol")
	}
}
