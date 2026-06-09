package config

import (
	"fmt"
	"os"
	"regexp"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Watch      WatchConfig      `yaml:"watch"`
	Sink       SinkConfig       `yaml:"sink"`
	Parser     ParserConfig     `yaml:"parser"`
	Transform  TransformConfig  `yaml:"transform"`
	Enrichers  []EnricherConfig `yaml:"enrichers"`
	Pipeline   PipelineConfig   `yaml:"pipeline"`
	Logging    LoggingConfig    `yaml:"logging"`
	Metrics    MetricsConfig    `yaml:"metrics"`
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
	State    StateConfig   `yaml:"state"`
}

type StateConfig struct {
	Path string `yaml:"path"`
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

type ParserConfig struct {
	Type          string `yaml:"type"`
	StartPattern  string `yaml:"start_pattern"`
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
		Sink: SinkConfig{
			Type: "kafka",
			Kafka: &KafkaConfig{
				Brokers: []string{"localhost:9092"},
				Topic:   "logs",
			},
		},
		Parser: ParserConfig{
			Type: "line",
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
		Logging: LoggingConfig{
			Level:          "info",
			Format:         "text",
			StatusInterval: "30s",
		},
	}
}

func (c *Config) StatePath() string {
	if c.Watch.State.Path != "" {
		return c.Watch.State.Path
	}
	return ".log-forwarder/watermarks.json"
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
	if err := c.validateSink(); err != nil {
		return err
	}
	if err := c.validateParser(); err != nil {
		return err
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
	if err := c.validateLogging(); err != nil {
		return err
	}
	if err := c.validateMetrics(); err != nil {
		return err
	}
	return c.validateState()
}

func (c *Config) PollInterval() time.Duration {
	d, _ := time.ParseDuration(c.Watch.Poll)
	return d
}

func (c *Config) validateParser() error {
	parserType := c.Parser.Type
	if parserType == "" {
		parserType = "line"
	}
	switch parserType {
	case "line", "multiline":
	default:
		return fmt.Errorf("parser.type must be line or multiline")
	}
	if parserType == "multiline" {
		if c.Parser.StartPattern == "" {
			return fmt.Errorf("parser.start_pattern is required when parser.type is multiline")
		}
		if _, err := regexp.Compile(c.Parser.StartPattern); err != nil {
			return fmt.Errorf("parser.start_pattern: %w", err)
		}
	}
	return nil
}
