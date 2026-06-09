package config

import (
	"strings"
	"testing"
)

func validConfig() *Config {
	cfg := Default()
	cfg.Watch.Paths = []string{"./logs"}
	cfg.Watch.Patterns = []string{"*.log"}
	cfg.Watch.Poll = "1s"
	cfg.Sink = SinkConfig{
		Type: "kafka",
		Kafka: &KafkaConfig{
			Brokers: []string{"localhost:9092"},
			Topic:   "logs",
		},
	}
	cfg.Transform = TransformConfig{
		Type:    "tab",
		OnError: "wrap",
	}
	cfg.Pipeline = PipelineConfig{
		BufferSize: 1024,
		OnFull:     "block",
	}
	return cfg
}

func TestValidateRejectsUnknownSinkType(t *testing.T) {
	t.Parallel()

	cfg := validConfig()
	cfg.Sink = SinkConfig{Type: "unknown-sink-typo"}

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error for unknown sink type")
	} else if !strings.Contains(err.Error(), "sink.type") {
		t.Fatalf("Validate() error = %v, want sink.type mention", err)
	}
}

func TestValidateAllowsRegisteredCustomSinkType(t *testing.T) {
	const sinkType = "custom-sink-registry-test"

	RegisterSinkType(sinkType)
	t.Cleanup(func() {
		delete(sinkTypes, sinkType)
	})

	cfg := validConfig()
	cfg.Sink = SinkConfig{Type: sinkType}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestValidateRejectsUnknownEnricherType(t *testing.T) {
	t.Parallel()

	cfg := validConfig()
	cfg.Enrichers = []EnricherConfig{{Type: "region"}}

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error for unknown enricher type")
	} else if !strings.Contains(err.Error(), "enrichers[0].type") {
		t.Fatalf("Validate() error = %v, want enrichers[0].type mention", err)
	}
}

func TestValidateStaticEnricherRequiresFields(t *testing.T) {
	t.Parallel()

	cfg := validConfig()
	cfg.Enrichers = []EnricherConfig{{Type: "static"}}

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error when static enricher has no fields")
	} else if !strings.Contains(err.Error(), "enrichers[0].fields") {
		t.Fatalf("Validate() error = %v, want enrichers[0].fields mention", err)
	}
}

func TestValidateStaticEnricherWithFields(t *testing.T) {
	t.Parallel()

	cfg := validConfig()
	cfg.Enrichers = []EnricherConfig{
		{Type: "static", Fields: map[string]string{"environment": "prod"}},
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestValidateRejectsUnknownTransformType(t *testing.T) {
	t.Parallel()

	cfg := validConfig()
	cfg.Transform.Type = "uppercase_tab"

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error for unknown transform type")
	} else if !strings.Contains(err.Error(), "transform.type") {
		t.Fatalf("Validate() error = %v, want transform.type mention", err)
	}
}

func TestValidateAllowsRegisteredCustomTransformType(t *testing.T) {
	const transformType = "custom-transform-registry-test"

	RegisterTransformType(transformType)
	t.Cleanup(func() {
		delete(transformerTypes, transformType)
	})

	cfg := validConfig()
	cfg.Transform.Type = transformType

	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestValidateRejectsUnknownParserType(t *testing.T) {
	t.Parallel()

	cfg := validConfig()
	cfg.Parser = ParserConfig{Type: "json"}

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error for unknown parser type")
	} else if !strings.Contains(err.Error(), "parser.type") {
		t.Fatalf("Validate() error = %v, want parser.type mention", err)
	}
}

func TestValidateAllowsRegisteredCustomParserType(t *testing.T) {
	const parserType = "custom-parser-registry-test"

	RegisterParserType(parserType)
	t.Cleanup(func() {
		delete(parserTypes, parserType)
	})

	cfg := validConfig()
	cfg.Parser = ParserConfig{Type: parserType}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestValidateAllowsRegisteredCustomEnricherType(t *testing.T) {
	const enricherType = "custom-enricher-registry-test"

	RegisterEnricherType(enricherType)
	t.Cleanup(func() {
		delete(enricherTypes, enricherType)
	})

	cfg := validConfig()
	cfg.Enrichers = []EnricherConfig{
		{Type: enricherType, Fields: map[string]string{"region": "us-east-1"}},
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}
