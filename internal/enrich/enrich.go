package enrich

import (
	"fmt"

	"github.com/sanjuthomas/log-forwarder/internal/config"
	"github.com/sanjuthomas/log-forwarder/internal/transform"
)

// Enricher adds metadata to a transformed record.
type Enricher interface {
	Enrich(record transform.Record) transform.Record
}

type Factory func(cfg config.EnricherConfig) (Enricher, error)

var registry = map[string]Factory{}

// Register adds a custom enricher factory. Call from init() in user code.
func Register(name string, factory Factory) {
	registry[name] = factory
	config.RegisterEnricherType(name)
}

func NewChain(cfgs []config.EnricherConfig) ([]Enricher, error) {
	chain := make([]Enricher, 0, len(cfgs))
	for _, cfg := range cfgs {
		factory, ok := registry[cfg.Type]
		if !ok {
			return nil, fmt.Errorf("unknown enricher type %q", cfg.Type)
		}
		e, err := factory(cfg)
		if err != nil {
			return nil, fmt.Errorf("enricher %q: %w", cfg.Type, err)
		}
		chain = append(chain, e)
	}
	return chain, nil
}

func Apply(chain []Enricher, record transform.Record) transform.Record {
	for _, e := range chain {
		record = e.Enrich(record)
	}
	return record
}

func init() {
	Register("static", newStaticEnricher)
	Register("host", newHostEnricher)
}
