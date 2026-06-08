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
}

func NewKafka(cfg config.KafkaConfig) *KafkaSink {
	return &KafkaSink{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(cfg.Brokers...),
			Topic:        cfg.Topic,
			Balancer:     &kafka.LeastBytes{},
			BatchTimeout: 10 * time.Millisecond,
			RequiredAcks: kafka.RequireOne,
			Async:        false,
		},
	}
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
