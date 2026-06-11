// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package sink

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/sanjuthomas/log-forwarder/internal/config"
	"github.com/segmentio/kafka-go"
)

type mockKafkaWriter struct {
	messages [][]byte
	err      error
	writeFn  func(ctx context.Context, msgs ...kafka.Message) error
}

func (m *mockKafkaWriter) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	if m.writeFn != nil {
		return m.writeFn(ctx, msgs...)
	}
	if m.err != nil {
		return m.err
	}
	for _, msg := range msgs {
		m.messages = append(m.messages, msg.Value)
	}
	return nil
}

func (m *mockKafkaWriter) Close() error {
	return nil
}

func newTestKafkaSink(writer kafkaMessageWriter) *kafkaSink {
	return &kafkaSink{
		writer: writer,
		cfg: config.KafkaConfig{
			Brokers: []string{"localhost:9092"},
			Topic:   "logs",
		},
	}
}

func TestKafkaSinkPublish(t *testing.T) {
	t.Parallel()

	writer := &mockKafkaWriter{}
	s := newTestKafkaSink(writer)

	payload := []byte(`{"message":"hello"}`)
	if err := s.Publish(context.Background(), payload); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if len(writer.messages) != 1 {
		t.Fatalf("messages = %d, want 1", len(writer.messages))
	}
	if string(writer.messages[0]) != string(payload) {
		t.Fatalf("message = %q, want %q", writer.messages[0], payload)
	}
}

func TestKafkaSinkPublishBatch(t *testing.T) {
	t.Parallel()

	writer := &mockKafkaWriter{}
	s := newTestKafkaSink(writer)

	payloads := [][]byte{
		[]byte(`{"line":1}`),
		[]byte(`{"line":2}`),
	}
	if err := s.PublishBatch(context.Background(), payloads); err != nil {
		t.Fatalf("PublishBatch() error = %v", err)
	}
	if len(writer.messages) != len(payloads) {
		t.Fatalf("messages = %d, want %d", len(writer.messages), len(payloads))
	}
	for i, want := range payloads {
		if string(writer.messages[i]) != string(want) {
			t.Fatalf("message[%d] = %q, want %q", i, writer.messages[i], want)
		}
	}
}

func TestKafkaSinkPublishBatchEmpty(t *testing.T) {
	t.Parallel()

	writer := &mockKafkaWriter{}
	s := newTestKafkaSink(writer)

	if err := s.PublishBatch(context.Background(), nil); err != nil {
		t.Fatalf("PublishBatch(nil) error = %v", err)
	}
	if len(writer.messages) != 0 {
		t.Fatalf("messages = %d, want 0", len(writer.messages))
	}
}

func TestKafkaSinkPublishBrokerError(t *testing.T) {
	t.Parallel()

	brokerErr := errors.New("broker unavailable")
	writer := &mockKafkaWriter{err: brokerErr}
	s := newTestKafkaSink(writer)

	err := s.Publish(context.Background(), []byte(`{}`))
	if err == nil {
		t.Fatal("expected error for broker failure")
	}
	if !errors.Is(err, brokerErr) {
		t.Fatalf("error = %v, want wrap of %v", err, brokerErr)
	}
	if !strings.Contains(err.Error(), "kafka publish:") {
		t.Fatalf("error = %q, want kafka publish prefix", err.Error())
	}
}

func TestKafkaSinkPublishContextTimeout(t *testing.T) {
	t.Parallel()

	writer := &mockKafkaWriter{
		writeFn: func(ctx context.Context, _ ...kafka.Message) error {
			return ctx.Err()
		},
	}
	s := newTestKafkaSink(writer)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := s.Publish(ctx, []byte(`{}`))
	if err == nil {
		t.Fatal("expected error for cancelled context")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want %v", err, context.Canceled)
	}
}
