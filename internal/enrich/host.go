package enrich

import (
	"os"

	"github.com/sanjuthomas/log-forwarder/internal/config"
	"github.com/sanjuthomas/log-forwarder/internal/transform"
)

type hostEnricher struct {
	hostname string
}

func newHostEnricher(_ config.EnricherConfig) (Enricher, error) {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}
	return &hostEnricher{hostname: hostname}, nil
}

func (e *hostEnricher) Enrich(record transform.Record) transform.Record {
	record["hostname"] = e.hostname
	return record
}
