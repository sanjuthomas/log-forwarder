package config

import "fmt"

func (c *Config) validateEnrichers() error {
	for i, enricher := range c.Enrichers {
		if enricher.Type == "" {
			return fmt.Errorf("enrichers[%d].type must not be empty", i)
		}
		if !knownEnricherType(enricher.Type) {
			return unknownTypeError(fmt.Sprintf("enrichers[%d].type", i), enricher.Type, enricherTypes)
		}
		switch enricher.Type {
		case "static":
			if len(enricher.Fields) == 0 {
				return fmt.Errorf("enrichers[%d].fields is required when type is static", i)
			}
		}
	}
	return nil
}
