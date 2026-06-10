package config

import (
	"testing"
)

func TestWatchConfigEntriesFromSources(t *testing.T) {
	t.Parallel()

	cfg := WatchConfig{
		Sources: []WatchSource{
			{Path: "./logs/nginx", Patterns: []string{"*.log"}},
			{Path: "./logs/app", Patterns: []string{"*.log", "*.out"}},
		},
	}

	entries := cfg.Entries()
	if len(entries) != 2 {
		t.Fatalf("len(entries) = %d, want 2", len(entries))
	}
	if entries[0].Path != "./logs/nginx" {
		t.Fatalf("entries[0].path = %q", entries[0].Path)
	}
	if len(entries[0].Patterns) != 1 || entries[0].Patterns[0] != "*.log" {
		t.Fatalf("entries[0].patterns = %v", entries[0].Patterns)
	}
}

func TestWatchConfigEntriesFromPathsAndPatterns(t *testing.T) {
	t.Parallel()

	cfg := WatchConfig{
		Paths:    []string{"./a", "./b"},
		Patterns: []string{"*.log", "*.out"},
	}

	entries := cfg.Entries()
	if len(entries) != 2 {
		t.Fatalf("len(entries) = %d, want 2", len(entries))
	}
	if len(entries[0].Patterns) != 2 || len(entries[1].Patterns) != 2 {
		t.Fatalf("expected shared patterns on each path, got %+v", entries)
	}
}

func TestValidateWatchSources(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		Watch: WatchConfig{
			Sources: []WatchSource{
				{Path: "./logs/app", Patterns: []string{"*.log"}},
			},
			Poll: "1s",
		},
		Sink: SinkConfig{
			Type: "kafka",
			Kafka: &KafkaConfig{
				Brokers: []string{"localhost:9092"},
				Topic:   "logs",
			},
		},
		Transform: TransformConfig{
			Type:    "tab",
			OnError: "wrap",
		},
		Pipeline: PipelineConfig{
			BufferSize: 1024,
			OnFull:     "block",
		},
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestValidateWatchSourcesRequirePatterns(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.Watch.Sources = []WatchSource{
		{Path: "./logs/app"},
	}

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error for empty source patterns")
	}
}

func TestValidateRegexRequiresPattern(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.Transform = TransformConfig{
		Type:    "regex",
		OnError: "wrap",
	}

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error when regex pattern is missing")
	}
}

func TestValidateMultilineParserRequiresStartPattern(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.Parser = ParserConfig{Type: "multiline"}

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error when multiline start_pattern is missing")
	}
}

func TestValidateFileSinkRequiresPath(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.Sink = SinkConfig{Type: "file", File: &FileSinkConfig{}}

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error when sink.file.path is missing")
	}
}

func TestValidateHTTPNoauthSinkRequiresURL(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.Sink = SinkConfig{Type: "http-noauth", HTTPNoauth: &HTTPNoauthSinkConfig{}}

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error when sink.http_noauth.url is missing")
	}
}

func TestValidatePipelineRejectsNegativeMaxPublishBytes(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.Pipeline.MaxPublishBytes = -1

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error for negative max_publish_bytes")
	}
}

func TestValidateMultilineParserRejectsInvalidStartPattern(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.Parser = ParserConfig{
		Type:         "multiline",
		StartPattern: `(?P<bad`,
	}

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error for invalid start_pattern")
	}
}
