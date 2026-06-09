package parser

import (
	"bytes"
	"fmt"
	"regexp"

	"github.com/sanjuthomas/log-forwarder/internal/config"
	"github.com/sanjuthomas/log-forwarder/internal/watcher"
)

type multilineBuffer struct {
	path   string
	inode  uint64
	lines  [][]byte
	offset int64
}

type multilineParser struct {
	startRe *regexp.Regexp
	buffers map[string]*multilineBuffer
}

func newMultilineParser(cfg config.ParserConfig) (Parser, error) {
	re, err := regexp.Compile(cfg.StartPattern)
	if err != nil {
		return nil, fmt.Errorf("compile parser.start_pattern: %w", err)
	}
	return &multilineParser{
		startRe: re,
		buffers: make(map[string]*multilineBuffer),
	}, nil
}

func (p *multilineParser) Feed(event watcher.LineEvent) ([]Event, error) {
	if p.startRe.Match(event.Line) {
		var out []Event
		if buf, ok := p.buffers[event.Path]; ok && len(buf.lines) > 0 {
			out = append(out, flushBuffer(buf))
		}
		p.buffers[event.Path] = &multilineBuffer{
			path:   event.Path,
			inode:  event.Inode,
			lines:  [][]byte{append([]byte(nil), event.Line...)},
			offset: event.Offset,
		}
		return out, nil
	}

	if buf, ok := p.buffers[event.Path]; ok {
		buf.lines = append(buf.lines, append([]byte(nil), event.Line...))
		buf.offset = event.Offset
		buf.inode = event.Inode
		return nil, nil
	}

	return []Event{{
		Path:   event.Path,
		Data:   event.Line,
		Offset: event.Offset,
		Inode:  event.Inode,
	}}, nil
}

func (p *multilineParser) Flush() ([]Event, error) {
	out := make([]Event, 0, len(p.buffers))
	for path, buf := range p.buffers {
		if len(buf.lines) > 0 {
			out = append(out, flushBuffer(buf))
		}
		delete(p.buffers, path)
	}
	return out, nil
}

func flushBuffer(buf *multilineBuffer) Event {
	return Event{
		Path:   buf.path,
		Data:   bytes.Join(buf.lines, []byte("\n")),
		Offset: buf.offset,
		Inode:  buf.inode,
	}
}
