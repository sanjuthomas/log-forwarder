// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package enrich

import (
	"github.com/sanjuthomas/log-forwarder/internal/config"
	"github.com/sanjuthomas/log-forwarder/internal/transform"
)

type staticEnricher struct {
	fields map[string]string
}

func newStaticEnricher(cfg config.EnricherConfig) (Enricher, error) {
	return &staticEnricher{fields: cfg.Fields}, nil
}

func (e *staticEnricher) Enrich(record transform.Record) transform.Record {
	for k, v := range e.fields {
		record[k] = v
	}
	return record
}
