// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

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
	}, "drop")
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

func TestFieldPredicateMissingFieldDropsByDefault(t *testing.T) {
	t.Parallel()

	predicate, err := newFieldPredicate(config.FilterRuleConfig{
		Field:  "level",
		Op:     "in",
		Values: []string{"ERROR"},
	}, "drop")
	if err != nil {
		t.Fatalf("newFieldPredicate() error = %v", err)
	}

	if predicate.Match(transform.Record{"message": "no level here"}) {
		t.Fatal("expected missing level field to fail filter")
	}
}

func TestFieldPredicateMissingFieldPassWhenConfigured(t *testing.T) {
	t.Parallel()

	predicate, err := newFieldPredicate(config.FilterRuleConfig{
		Field:     "level",
		Op:        "in",
		Values:    []string{"ERROR"},
		OnMissing: "pass",
	}, "drop")
	if err != nil {
		t.Fatalf("newFieldPredicate() error = %v", err)
	}

	if !predicate.Match(transform.Record{"message": "no level here"}) {
		t.Fatal("expected missing level field to pass rule when on_missing is pass")
	}
}

func TestFieldPredicateMissingFieldInheritsFilterDefault(t *testing.T) {
	t.Parallel()

	predicate, err := New(config.FilterConfig{
		OnMissing: "pass",
		Match:     "all",
		Rules: []config.FilterRuleConfig{
			{Type: "field", Field: "level", Op: "in", Values: []string{"ERROR"}},
		},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if !predicate.Match(transform.Record{"message": "no level here"}) {
		t.Fatal("expected missing field to inherit filter.on_missing pass")
	}
}

func TestFieldPredicateMissingFieldNeqDropsByDefault(t *testing.T) {
	t.Parallel()

	predicate, err := newFieldPredicate(config.FilterRuleConfig{
		Field: "level",
		Op:    "neq",
		Value: "ERROR",
	}, "drop")
	if err != nil {
		t.Fatalf("newFieldPredicate() error = %v", err)
	}

	if predicate.Match(transform.Record{"message": "no level here"}) {
		t.Fatal("expected missing field with neq to fail filter by default")
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
