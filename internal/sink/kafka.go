// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package sink

import (
	"context"
	"fmt"
	"time"

	"github.com/sanjuthomas/log-forwarder/internal/config"
	"github.com/segmentio/kafka-go"
)

type kafkaMessageWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

type kafkaSink struct {
	writer kafkaMessageWriter
	cfg    config.KafkaConfig
	dialer *kafka.Dialer
}

func newKafkaSink(cfg config.SinkConfig) (Sink, error) {
	if cfg.Kafka == nil {
		return nil, fmt.Errorf("sink.kafka is required")
	}
	kafkaCfg := *cfg.Kafka

	dialer, err := buildDialer(kafkaCfg)
	if err != nil {
		return nil, fmt.Errorf("kafka dialer: %w", err)
	}

	return &kafkaSink{
		cfg:    kafkaCfg,
		dialer: dialer,
		writer: newKafkaWriter(kafkaCfg, dialer),
	}, nil
}

func newKafkaWriter(kafkaCfg config.KafkaConfig, dialer *kafka.Dialer) kafkaMessageWriter {
	return kafka.NewWriter(kafka.WriterConfig{
		Brokers:      kafkaCfg.Brokers,
		Topic:        kafkaCfg.Topic,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond,
		RequiredAcks: int(kafka.RequireOne),
		Async:        false,
		Dialer:       dialer,
	})
}

func (k *kafkaSink) Check(ctx context.Context) error {
	var lastErr error
	for _, broker := range k.cfg.Brokers {
		conn, err := k.dialer.DialContext(ctx, "tcp", broker)
		if err != nil {
			lastErr = fmt.Errorf("broker %q: %w", broker, err)
			continue
		}

		partitions, err := conn.ReadPartitions(k.cfg.Topic)
		_ = conn.Close()
		if err != nil {
			return fmt.Errorf("kafka topic %q on broker %q: %w", k.cfg.Topic, broker, err)
		}
		if len(partitions) == 0 {
			return fmt.Errorf("kafka topic %q not found on broker %q", k.cfg.Topic, broker)
		}
		return nil
	}

	if lastErr != nil {
		return fmt.Errorf("unable to connect to kafka: %w", lastErr)
	}
	return fmt.Errorf("unable to connect to kafka: no brokers configured")
}

func (k *kafkaSink) Publish(ctx context.Context, payload []byte) error {
	return k.PublishBatch(ctx, [][]byte{payload})
}

func (k *kafkaSink) PublishBatch(ctx context.Context, payloads [][]byte) error {
	if len(payloads) == 0 {
		return nil
	}
	messages := make([]kafka.Message, len(payloads))
	for i, payload := range payloads {
		messages[i] = kafka.Message{Value: payload}
	}
	if err := k.writer.WriteMessages(ctx, messages...); err != nil {
		return fmt.Errorf("kafka publish: %w", err)
	}
	return nil
}

func (k *kafkaSink) Close() error {
	return k.writer.Close()
}

func init() {
	Register("kafka", newKafkaSink)
}
