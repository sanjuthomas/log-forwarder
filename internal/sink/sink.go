// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package sink

import (
	"context"
	"fmt"

	"github.com/sanjuthomas/log-forwarder/internal/config"
)

// Sink publishes encoded log records to a destination.
type Sink interface {
	Publish(ctx context.Context, payload []byte) error
	Close() error
}

// BatchSink publishes multiple encoded records in one sink operation.
type BatchSink interface {
	PublishBatch(ctx context.Context, payloads [][]byte) error
}

// Checker optionally verifies destination connectivity at startup.
type Checker interface {
	Check(ctx context.Context) error
}

// Factory constructs a sink from configuration. Register custom sinks in init().
type Factory func(cfg config.SinkConfig) (Sink, error)

var registry = map[string]Factory{}

// Register adds a custom sink factory. Call from init() in user code.
func Register(name string, factory Factory) {
	registry[name] = factory
	config.RegisterSinkType(name)
}

// New creates the configured sink implementation (kafka, file, http-noauth, or custom).
func New(cfg config.SinkConfig) (Sink, error) {
	sinkType := cfg.Type
	if sinkType == "" {
		sinkType = "kafka"
	}

	factory, ok := registry[sinkType]
	if !ok {
		return nil, fmt.Errorf("unknown sink type %q (registered: %v)", sinkType, registeredNames())
	}
	return factory(cfg)
}

func registeredNames() []string {
	names := make([]string, 0, len(registry))
	for name := range registry {
		names = append(names, name)
	}
	return names
}
