package parser

import (
	"fmt"

	"github.com/sanjuthomas/log-forwarder/internal/config"
	"github.com/sanjuthomas/log-forwarder/internal/watcher"
)

// Event is a logical log record produced by a parser, ready for transformation.
type Event struct {
	Path   string
	Data   []byte
	Offset int64
	Inode  uint64
}

// Parser groups physical log lines into logical records.
type Parser interface {
	Feed(event watcher.LineEvent) ([]Event, error)
	Flush() ([]Event, error)
}

type Factory func(cfg config.ParserConfig) (Parser, error)

var registry = map[string]Factory{}

// Register adds a custom parser factory. Call from init() in user code.
func Register(name string, factory Factory) {
	registry[name] = factory
	config.RegisterParserType(name)
}

func New(cfg config.ParserConfig) (Parser, error) {
	parserType := cfg.Type
	if parserType == "" {
		parserType = "line"
	}

	factory, ok := registry[parserType]
	if !ok {
		return nil, fmt.Errorf("unknown parser type %q (registered: %v)", parserType, registeredNames())
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
	Register("line", newLineParser)
	Register("multiline", newMultilineParser)
}
