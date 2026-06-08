package transform

import (
	"fmt"
	"regexp"

	"github.com/sanjuthomas/log-forwarder/internal/config"
)

type regexTransformer struct {
	re      *regexp.Regexp
	onError string
}

func newRegexTransformer(cfg config.TransformConfig) (Transformer, error) {
	if cfg.Pattern == "" {
		return nil, fmt.Errorf("transform.pattern is required for regex transformer")
	}
	re, err := regexp.Compile(cfg.Pattern)
	if err != nil {
		return nil, fmt.Errorf("compile transform.pattern: %w", err)
	}
	return &regexTransformer{re: re, onError: cfg.OnError}, nil
}

func (t *regexTransformer) Transform(line []byte) (Record, error) {
	matches := t.re.FindStringSubmatch(string(line))
	if matches == nil {
		return nil, fmt.Errorf("line does not match pattern")
	}

	record := make(Record)
	for i, name := range t.re.SubexpNames() {
		if i == 0 || name == "" {
			continue
		}
		record[name] = matches[i]
	}
	return record, nil
}
