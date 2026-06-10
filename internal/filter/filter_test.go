package filter

import (
	"testing"

	"github.com/sanjuthomas/log-forwarder/internal/config"
	"github.com/sanjuthomas/log-forwarder/internal/transform"
)

func TestFieldPredicateCaseInsensitiveIn(t *testing.T) {
	t.Parallel()

	predicate, err := newFieldPredicate(config.FilterRuleConfig{
		Field:      "level",
		Op:         "in",
		Values:     []string{"ERROR"},
		IgnoreCase: true,
	})
	if err != nil {
		t.Fatalf("newFieldPredicate() error = %v", err)
	}

	if !predicate.Match(transform.Record{"level": "error"}) {
		t.Fatal("expected error to match ERROR")
	}
	if predicate.Match(transform.Record{"level": "INFO"}) {
		t.Fatal("expected INFO to be filtered out")
	}
}

func TestCompoundPredicateMatchAny(t *testing.T) {
	t.Parallel()

	predicate, err := New(config.FilterConfig{
		Match: "any",
		Rules: []config.FilterRuleConfig{
			{Type: "field", Field: "level", Op: "eq", Value: "INFO", IgnoreCase: true},
			{Type: "field", Field: "level", Op: "eq", Value: "WARN", IgnoreCase: true},
			{Type: "field", Field: "level", Op: "eq", Value: "ERROR", IgnoreCase: true},
		},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	for _, level := range []string{"info", "WARN", "error"} {
		if !predicate.Match(transform.Record{"level": level}) {
			t.Fatalf("expected level %q to pass filter", level)
		}
	}
	if predicate.Match(transform.Record{"level": "DEBUG"}) {
		t.Fatal("expected DEBUG to be filtered out")
	}
}

func TestCompoundPredicateMatchAll(t *testing.T) {
	t.Parallel()

	predicate, err := New(config.FilterConfig{
		Match: "all",
		Rules: []config.FilterRuleConfig{
			{Type: "field", Field: "level", Op: "eq", Value: "ERROR", IgnoreCase: true},
			{Type: "field", Field: "service", Op: "eq", Value: "billing"},
		},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if !predicate.Match(transform.Record{"level": "ERROR", "service": "billing"}) {
		t.Fatal("expected matching record to pass")
	}
	if predicate.Match(transform.Record{"level": "ERROR", "service": "auth"}) {
		t.Fatal("expected non-matching service to be filtered out")
	}
}

func TestCustomFilterRegister(t *testing.T) {
	const filterType = "always_false_test"

	Register(filterType, func(_ config.FilterRuleConfig) (Predicate, error) {
		return predicateFunc(func(transform.Record) bool { return false }), nil
	})

	predicate, err := New(config.FilterConfig{
		Match: "all",
		Rules: []config.FilterRuleConfig{{Type: filterType}},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if predicate.Match(transform.Record{"level": "ERROR"}) {
		t.Fatal("expected custom filter to reject record")
	}
}

type predicateFunc func(transform.Record) bool

func (f predicateFunc) Match(record transform.Record) bool { return f(record) }
