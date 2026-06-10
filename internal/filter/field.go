package filter

import (
	"fmt"
	"strings"

	"github.com/sanjuthomas/log-forwarder/internal/config"
	"github.com/sanjuthomas/log-forwarder/internal/transform"
)

type fieldPredicate struct {
	field      string
	op         string
	value      string
	values     []string
	ignoreCase bool
}

func newFieldPredicate(cfg config.FilterRuleConfig) (Predicate, error) {
	return fieldPredicate{
		field:      cfg.Field,
		op:         cfg.Op,
		value:      cfg.Value,
		values:     append([]string(nil), cfg.Values...),
		ignoreCase: cfg.IgnoreCase,
	}, nil
}

func (f fieldPredicate) Match(record transform.Record) bool {
	actual, ok := record[f.field]
	if !ok {
		return f.op == "neq" || f.op == "not_in"
	}

	actualStr := stringify(actual)
	switch f.op {
	case "eq":
		return compareEqual(actualStr, f.value, f.ignoreCase)
	case "neq":
		return !compareEqual(actualStr, f.value, f.ignoreCase)
	case "in":
		return compareIn(actualStr, f.values, f.ignoreCase)
	case "not_in":
		return !compareIn(actualStr, f.values, f.ignoreCase)
	default:
		return false
	}
}

func stringify(value any) string {
	switch v := value.(type) {
	case string:
		return v
	case fmt.Stringer:
		return v.String()
	default:
		return fmt.Sprint(v)
	}
}

func compareEqual(actual, expected string, ignoreCase bool) bool {
	if ignoreCase {
		return strings.EqualFold(actual, expected)
	}
	return actual == expected
}

func compareIn(actual string, values []string, ignoreCase bool) bool {
	for _, candidate := range values {
		if compareEqual(actual, candidate, ignoreCase) {
			return true
		}
	}
	return false
}
