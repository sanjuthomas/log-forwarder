// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package transform

import (
	"fmt"

	"github.com/sanjuthomas/log-forwarder/internal/config"
)

// Record is the in-memory representation of a log line after transformation.
type Record map[string]any

// Transformer converts a raw log line into a structured record.
type Transformer interface {
	Transform(line []byte) (Record, error)
}

// Factory constructs a transformer from configuration. Register custom transformers in init().
type Factory func(cfg config.TransformConfig) (Transformer, error)

var registry = map[string]Factory{}

// Register adds a custom transformer factory. Call from init() in user code.
func Register(name string, factory Factory) {
	registry[name] = factory
	config.RegisterTransformType(name)
}

// New selects and constructs the configured transformer implementation.
func New(cfg config.TransformConfig) (Transformer, error) {
	factory, ok := registry[cfg.Type]
	if !ok {
		return nil, fmt.Errorf("unknown transformer type %q (registered: %v)", cfg.Type, registeredNames())
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

func init() {
	Register("delimiter", newDelimiterTransformer)
	Register("tab", func(cfg config.TransformConfig) (Transformer, error) {
		if cfg.Delimiter == "" {
			cfg.Delimiter = "\t"
		}
		return newDelimiterTransformer(cfg)
	})
	Register("regex", newRegexTransformer)
}
