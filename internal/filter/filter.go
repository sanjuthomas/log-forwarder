package filter

import (
	"fmt"

	"github.com/sanjuthomas/log-forwarder/internal/config"
	"github.com/sanjuthomas/log-forwarder/internal/transform"
)

// Predicate decides whether a transformed record should continue downstream.
type Predicate interface {
	Match(record transform.Record) bool
}

type Factory func(cfg config.FilterRuleConfig) (Predicate, error)

var registry = map[string]Factory{}

// Register adds a custom filter factory. Call from init() in user code.
func Register(name string, factory Factory) {
	registry[name] = factory
	config.RegisterFilterType(name)
}

// New builds the configured filter predicate. An empty config passes all records.
func New(cfg config.FilterConfig) (Predicate, error) {
	if !cfg.Enabled() {
		return passAll{}, nil
	}
	onMissingDefault := cfg.OnMissing
	if onMissingDefault == "" {
		onMissingDefault = "drop"
	}
	return newCompound(cfg.Match, onMissingDefault, cfg.Rules)
}

func newRule(cfg config.FilterRuleConfig, onMissingDefault string) (Predicate, error) {
	switch cfg.Type {
	case "field":
		return newFieldPredicate(cfg, onMissingDefault)
	case "compound":
		return newCompound(cfg.Match, onMissingDefault, cfg.Rules)
	default:
		factory, ok := registry[cfg.Type]
		if !ok {
			return nil, fmt.Errorf("unknown filter type %q (registered: %v)", cfg.Type, registeredNames())
		}
		return factory(cfg)
	}
}

func newCompound(match, onMissingDefault string, rules []config.FilterRuleConfig) (Predicate, error) {
	if len(rules) == 0 {
		return passAll{}, nil
	}
	if match == "" {
		match = "all"
	}
	matchAll, err := parseMatchMode(match)
	if err != nil {
		return nil, err
	}

	predicates := make([]Predicate, 0, len(rules))
	for i, rule := range rules {
		predicate, err := newRule(rule, onMissingDefault)
		if err != nil {
			return nil, fmt.Errorf("filter rule[%d]: %w", i, err)
		}
		predicates = append(predicates, predicate)
	}
	return compoundPredicate{matchAll: matchAll, rules: predicates}, nil
}

func parseMatchMode(match string) (bool, error) {
	switch match {
	case "all", "":
		return true, nil
	case "any":
		return false, nil
	default:
		return false, fmt.Errorf("filter match must be all or any")
	}
}

func registeredNames() []string {
	names := make([]string, 0, len(registry))
	for name := range registry {
		names = append(names, name)
	}
	return names
}

type passAll struct{}

func (passAll) Match(transform.Record) bool { return true }

type compoundPredicate struct {
	matchAll bool
	rules    []Predicate
}

func (c compoundPredicate) Match(record transform.Record) bool {
	if len(c.rules) == 0 {
		return true
	}
	if c.matchAll {
		for _, rule := range c.rules {
			if !rule.Match(record) {
				return false
			}
		}
		return true
	}
	for _, rule := range c.rules {
		if rule.Match(record) {
			return true
		}
	}
	return false
}
