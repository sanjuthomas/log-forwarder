package parser

import (
	"github.com/sanjuthomas/log-forwarder/internal/config"
	"github.com/sanjuthomas/log-forwarder/internal/watcher"
)

type lineParser struct{}

func newLineParser(_ config.ParserConfig) (Parser, error) {
	return &lineParser{}, nil
}

func (p *lineParser) Feed(event watcher.LineEvent) ([]Event, error) {
	return []Event{{
		Path:   event.Path,
		Data:   event.Line,
		Offset: event.Offset,
		Inode:  event.Inode,
	}}, nil
}

func (p *lineParser) Flush() ([]Event, error) {
	return nil, nil
}
