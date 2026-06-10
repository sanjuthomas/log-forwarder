package filter

import (
	"testing"

	"github.com/sanjuthomas/log-forwarder/internal/config"
	"github.com/sanjuthomas/log-forwarder/internal/transform"
)

func TestFieldPredicateOps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		rule   config.FilterRuleConfig
		record transform.Record
		want   bool
	}{
		{
			name:   "eq match",
			rule:   config.FilterRuleConfig{Field: "level", Op: "eq", Value: "ERROR"},
			record: transform.Record{"level": "ERROR"},
			want:   true,
		},
		{
			name:   "eq case sensitive mismatch",
			rule:   config.FilterRuleConfig{Field: "level", Op: "eq", Value: "ERROR"},
			record: transform.Record{"level": "error"},
			want:   false,
		},
		{
			name:   "eq case insensitive match",
			rule:   config.FilterRuleConfig{Field: "level", Op: "eq", Value: "ERROR", IgnoreCase: true},
			record: transform.Record{"level": "error"},
			want:   true,
		},
		{
			name:   "neq match",
			rule:   config.FilterRuleConfig{Field: "level", Op: "neq", Value: "ERROR"},
			record: transform.Record{"level": "INFO"},
			want:   true,
		},
		{
			name:   "neq no match",
			rule:   config.FilterRuleConfig{Field: "level", Op: "neq", Value: "ERROR"},
			record: transform.Record{"level": "ERROR"},
			want:   false,
		},
		{
			name:   "in match",
			rule:   config.FilterRuleConfig{Field: "level", Op: "in", Values: []string{"WARN", "ERROR"}},
			record: transform.Record{"level": "WARN"},
			want:   true,
		},
		{
			name:   "in no match",
			rule:   config.FilterRuleConfig{Field: "level", Op: "in", Values: []string{"WARN", "ERROR"}},
			record: transform.Record{"level": "INFO"},
			want:   false,
		},
		{
			name:   "not_in match",
			rule:   config.FilterRuleConfig{Field: "level", Op: "not_in", Values: []string{"DEBUG", "TRACE"}},
			record: transform.Record{"level": "INFO"},
			want:   true,
		},
		{
			name:   "not_in no match",
			rule:   config.FilterRuleConfig{Field: "level", Op: "not_in", Values: []string{"DEBUG", "TRACE"}},
			record: transform.Record{"level": "DEBUG"},
			want:   false,
		},
		{
			name:   "non string field value",
			rule:   config.FilterRuleConfig{Field: "pid", Op: "eq", Value: "18432"},
			record: transform.Record{"pid": 18432},
			want:   true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			predicate, err := newFieldPredicate(tt.rule, "drop")
			if err != nil {
				t.Fatalf("newFieldPredicate() error = %v", err)
			}
			if got := predicate.Match(tt.record); got != tt.want {
				t.Fatalf("Match() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFieldPredicateRuleOnMissingOverridesFilterDefault(t *testing.T) {
	t.Parallel()

	predicate, err := New(config.FilterConfig{
		OnMissing: "pass",
		Match:     "all",
		Rules: []config.FilterRuleConfig{
			{
				Type:      "field",
				Field:     "level",
				Op:        "in",
				Values:    []string{"ERROR"},
				OnMissing: "drop",
			},
		},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if predicate.Match(transform.Record{"message": "missing level"}) {
		t.Fatal("expected rule on_missing drop to override filter on_missing pass")
	}
}

func TestFieldPredicateMissingFieldPassStillRequiresLevelMatch(t *testing.T) {
	t.Parallel()

	predicate, err := New(config.FilterConfig{
		Match: "all",
		Rules: []config.FilterRuleConfig{
			{
				Type:      "field",
				Field:     "level",
				Op:        "in",
				Values:    []string{"ERROR"},
				OnMissing: "pass",
			},
		},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if predicate.Match(transform.Record{"level": "INFO"}) {
		t.Fatal("expected INFO to be filtered out even when on_missing is pass")
	}
}

func TestNestedCompoundFilter(t *testing.T) {
	t.Parallel()

	predicate, err := New(config.FilterConfig{
		Match: "all",
		Rules: []config.FilterRuleConfig{
			{
				Type:  "compound",
				Match: "any",
				Rules: []config.FilterRuleConfig{
					{Type: "field", Field: "level", Op: "eq", Value: "WARN", IgnoreCase: true},
					{Type: "field", Field: "level", Op: "eq", Value: "ERROR", IgnoreCase: true},
				},
			},
			{Type: "field", Field: "service", Op: "eq", Value: "billing"},
		},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if !predicate.Match(transform.Record{"level": "error", "service": "billing"}) {
		t.Fatal("expected nested compound to pass matching record")
	}
	if predicate.Match(transform.Record{"level": "error", "service": "auth"}) {
		t.Fatal("expected non-matching service to fail nested compound")
	}
	if predicate.Match(transform.Record{"level": "INFO", "service": "billing"}) {
		t.Fatal("expected INFO to fail level branch of nested compound")
	}
}

func TestEmptyFilterConfigPassesAll(t *testing.T) {
	t.Parallel()

	predicate, err := New(config.FilterConfig{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if !predicate.Match(transform.Record{"level": "DEBUG"}) {
		t.Fatal("expected empty filter config to pass all records")
	}
}
