package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Watch      WatchConfig      `yaml:"watch"`
	Kafka      KafkaConfig      `yaml:"kafka"`
	Transform  TransformConfig  `yaml:"transform"`
	Enrichers  []EnricherConfig `yaml:"enrichers"`
	Pipeline   PipelineConfig   `yaml:"pipeline"`
}

type WatchSource struct {
	Path     string   `yaml:"path"`
	Patterns []string `yaml:"patterns"`
}

type WatchConfig struct {
	Paths    []string      `yaml:"paths"`
	Patterns []string      `yaml:"patterns"`
	Sources  []WatchSource `yaml:"sources"`
	Poll     string        `yaml:"poll"`
}

// Entries returns the effective watch sources. When sources is set, it is used
// as-is; otherwise each path is paired with the shared patterns list.
func (c WatchConfig) Entries() []WatchSource {
	if len(c.Sources) > 0 {
		return c.Sources
	}

	entries := make([]WatchSource, 0, len(c.Paths))
	for _, path := range c.Paths {
		entries = append(entries, WatchSource{
			Path:     path,
			Patterns: c.Patterns,
		})
	}
	return entries
}

type KafkaConfig struct {
	Brokers []string `yaml:"brokers"`
	Topic   string   `yaml:"topic"`
}

type TransformConfig struct {
	Type      string   `yaml:"type"`
	Delimiter string   `yaml:"delimiter"`
	Columns   []string `yaml:"columns"`
	Pattern   string   `yaml:"pattern"`
	OnError   string   `yaml:"on_error"`
}

type EnricherConfig struct {
	Type   string            `yaml:"type"`
	Fields map[string]string `yaml:"fields"`
}

type PipelineConfig struct {
	BufferSize int    `yaml:"buffer_size"`
	OnFull     string `yaml:"on_full"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	cfg := Default()
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

func Default() *Config {
	wd, _ := os.Getwd()
	if wd == "" {
		wd = "."
	}

	return &Config{
		Watch: WatchConfig{
			Paths:    []string{wd},
			Patterns: []string{"*.log*"},
			Poll:     "1s",
		},
		Kafka: KafkaConfig{
			Brokers: []string{"localhost:9092"},
			Topic:   "logs",
		},
		Transform: TransformConfig{
			Type:      "delimiter",
			Delimiter: "\t",
			OnError:   "wrap",
		},
		Enrichers: []EnricherConfig{
			{Type: "host"},
		},
		Pipeline: PipelineConfig{
			BufferSize: 1024,
			OnFull:     "block",
		},
	}
}

func (c *Config) Validate() error {
	if len(c.Watch.Sources) > 0 {
		for i, source := range c.Watch.Sources {
			if source.Path == "" {
				return fmt.Errorf("watch.sources[%d].path must not be empty", i)
			}
			if len(source.Patterns) == 0 {
				return fmt.Errorf("watch.sources[%d].patterns must not be empty", i)
			}
		}
	} else {
		if len(c.Watch.Paths) == 0 {
			return fmt.Errorf("watch.paths must not be empty")
		}
		if len(c.Watch.Patterns) == 0 {
			return fmt.Errorf("watch.patterns must not be empty")
		}
	}
	if _, err := time.ParseDuration(c.Watch.Poll); err != nil {
		return fmt.Errorf("watch.poll: %w", err)
	}
	if len(c.Kafka.Brokers) == 0 {
		return fmt.Errorf("kafka.brokers must not be empty")
	}
	if c.Kafka.Topic == "" {
		return fmt.Errorf("kafka.topic must not be empty")
	}
	if c.Transform.Type == "" {
		return fmt.Errorf("transform.type must not be empty")
	}
	if c.Transform.Type == "regex" && c.Transform.Pattern == "" {
		return fmt.Errorf("transform.pattern is required when transform.type is regex")
	}
	switch c.Transform.OnError {
	case "skip", "wrap":
	default:
		return fmt.Errorf("transform.on_error must be skip or wrap")
	}
	if c.Pipeline.BufferSize <= 0 {
		return fmt.Errorf("pipeline.buffer_size must be positive")
	}
	switch c.Pipeline.OnFull {
	case "block", "drop":
	default:
		return fmt.Errorf("pipeline.on_full must be block or drop")
	}
	return nil
}

func (c *Config) PollInterval() time.Duration {
	d, _ := time.ParseDuration(c.Watch.Poll)
	return d
}
