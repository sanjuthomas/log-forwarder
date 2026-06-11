// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package config

type FilterConfig struct {
	Match     string             `yaml:"match"`
	OnMissing string             `yaml:"on_missing,omitempty"`
	Rules     []FilterRuleConfig `yaml:"rules"`
}

type FilterRuleConfig struct {
	Type       string             `yaml:"type"`
	Field      string             `yaml:"field,omitempty"`
	Op         string             `yaml:"op,omitempty"`
	Value      string             `yaml:"value,omitempty"`
	Values     []string           `yaml:"values,omitempty"`
	IgnoreCase bool               `yaml:"ignore_case,omitempty"`
	OnMissing  string             `yaml:"on_missing,omitempty"`
	Match      string             `yaml:"match,omitempty"`
	Rules      []FilterRuleConfig `yaml:"rules,omitempty"`
}

func (c FilterConfig) Enabled() bool {
	return len(c.Rules) > 0
}
