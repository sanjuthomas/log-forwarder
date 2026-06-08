package transform

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/sanjuthomas/log-forwarder/internal/config"
)

type delimiterTransformer struct {
	delimiter string
	columns   []string
}

func newDelimiterTransformer(cfg config.TransformConfig) (Transformer, error) {
	delim := cfg.Delimiter
	if delim == "" {
		delim = "\t"
	}
	return &delimiterTransformer{
		delimiter: delim,
		columns:   cfg.Columns,
	}, nil
}

func (t *delimiterTransformer) Transform(line []byte) (Record, error) {
	line = bytes.TrimSpace(line)
	if len(line) == 0 {
		return nil, fmt.Errorf("empty line")
	}

	parts := strings.Split(string(line), t.delimiter)
	record := make(Record, len(parts))

	if len(t.columns) > 0 {
		for i, col := range t.columns {
			if i < len(parts) {
				record[col] = parts[i]
			}
		}
		for i := len(t.columns); i < len(parts); i++ {
			record[fmt.Sprintf("field_%d", i)] = parts[i]
		}
	} else {
		for i, part := range parts {
			record[fmt.Sprintf("field_%d", i)] = part
		}
	}

	return record, nil
}
