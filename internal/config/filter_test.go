package config

import (
	"strings"
	"testing"
)

func TestValidateFilterFieldRequiresOp(t *testing.T) {
	t.Parallel()

	cfg := validConfig()
	cfg.Filter = FilterConfig{
		Match: "all",
		Rules: []FilterRuleConfig{
			{Type: "field", Field: "level"},
		},
	}

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error for missing field op")
	} else if !strings.Contains(err.Error(), "filter.rules[0].op") {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestValidateFilterFieldInRequiresValues(t *testing.T) {
	t.Parallel()

	cfg := validConfig()
	cfg.Filter = FilterConfig{
		Match: "all",
		Rules: []FilterRuleConfig{
			{Type: "field", Field: "level", Op: "in"},
		},
	}

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error for missing values")
	} else if !strings.Contains(err.Error(), "filter.rules[0].values") {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestValidateFilterRejectsUnknownType(t *testing.T) {
	t.Parallel()

	cfg := validConfig()
	cfg.Filter = FilterConfig{
		Match: "all",
		Rules: []FilterRuleConfig{
			{Type: "unknown-filter-type"},
		},
	}

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error for unknown filter type")
	} else if !strings.Contains(err.Error(), "filter.rules[0].type") {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestValidateFilterRejectsInvalidOnMissing(t *testing.T) {
	t.Parallel()

	cfg := validConfig()
	cfg.Filter = FilterConfig{
		OnMissing: "ignore",
		Match:     "all",
		Rules: []FilterRuleConfig{
			{Type: "field", Field: "level", Op: "in", Values: []string{"ERROR"}},
		},
	}

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error for invalid on_missing")
	} else if !strings.Contains(err.Error(), "filter.on_missing") {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestValidateFilterAllowsErrorLevelRule(t *testing.T) {
	t.Parallel()

	cfg := validConfig()
	cfg.Filter = FilterConfig{
		Match: "all",
		Rules: []FilterRuleConfig{
			{
				Type:       "field",
				Field:      "level",
				Op:         "in",
				Values:     []string{"ERROR"},
				IgnoreCase: true,
			},
		},
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestValidateFilterAllowsRegisteredCustomType(t *testing.T) {
	const filterType = "custom-filter-validation-test"

	RegisterFilterType(filterType)
	t.Cleanup(func() {
		delete(filterTypes, filterType)
	})

	cfg := validConfig()
	cfg.Filter = FilterConfig{
		Match: "all",
		Rules: []FilterRuleConfig{{Type: filterType}},
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}
