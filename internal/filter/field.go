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
	onMissing  string
}

func newFieldPredicate(cfg config.FilterRuleConfig, onMissingDefault string) (Predicate, error) {
	onMissing := cfg.OnMissing
	if onMissing == "" {
		onMissing = onMissingDefault
	}
	if onMissing == "" {
		onMissing = "drop"
	}
	if onMissing != "drop" && onMissing != "pass" {
		return nil, fmt.Errorf("field filter on_missing must be drop or pass")
	}

	return fieldPredicate{
		field:      cfg.Field,
		op:         cfg.Op,
		value:      cfg.Value,
		values:     append([]string(nil), cfg.Values...),
		ignoreCase: cfg.IgnoreCase,
		onMissing:  onMissing,
	}, nil
}

func (f fieldPredicate) Match(record transform.Record) bool {
	actual, ok := record[f.field]
	if !ok {
		return f.onMissing == "pass"
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
