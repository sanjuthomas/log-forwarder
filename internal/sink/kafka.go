package sink

import (
	"context"
	"fmt"
	"time"

	"github.com/sanjuthomas/log-forwarder/internal/config"
	"github.com/segmentio/kafka-go"
)

type KafkaSink struct {
	writer *kafka.Writer
	cfg    config.KafkaConfig
	dialer *kafka.Dialer
}

func NewKafka(cfg config.KafkaConfig) (*KafkaSink, error) {
	dialer, err := buildDialer(cfg)
	if err != nil {
		return nil, fmt.Errorf("kafka dialer: %w", err)
	}

	return &KafkaSink{
		cfg:    cfg,
		dialer: dialer,
		writer: kafka.NewWriter(kafka.WriterConfig{
			Brokers:      cfg.Brokers,
			Topic:        cfg.Topic,
			Balancer:     &kafka.LeastBytes{},
			BatchTimeout: 10 * time.Millisecond,
			RequiredAcks: int(kafka.RequireOne),
			Async:        false,
			Dialer:       dialer,
		}),
	}, nil
}

func CheckConnectivity(ctx context.Context, cfg config.KafkaConfig) error {
	dialer, err := buildDialer(cfg)
	if err != nil {
		return fmt.Errorf("kafka dialer: %w", err)
	}

	var lastErr error
	for _, broker := range cfg.Brokers {
		conn, err := dialer.DialContext(ctx, "tcp", broker)
		if err != nil {
			lastErr = fmt.Errorf("broker %q: %w", broker, err)
			continue
		}

		partitions, err := conn.ReadPartitions(cfg.Topic)
		_ = conn.Close()
		if err != nil {
			return fmt.Errorf("kafka topic %q on broker %q: %w", cfg.Topic, broker, err)
		}
		if len(partitions) == 0 {
			return fmt.Errorf("kafka topic %q not found on broker %q", cfg.Topic, broker)
		}
		return nil
	}

	if lastErr != nil {
		return fmt.Errorf("unable to connect to kafka: %w", lastErr)
	}
	return fmt.Errorf("unable to connect to kafka: no brokers configured")
}

func (k *KafkaSink) Publish(ctx context.Context, payload []byte) error {
	if err := k.writer.WriteMessages(ctx, kafka.Message{Value: payload}); err != nil {
		return fmt.Errorf("kafka publish: %w", err)
	}
	return nil
}

func (k *KafkaSink) Close() error {
	return k.writer.Close()
}
