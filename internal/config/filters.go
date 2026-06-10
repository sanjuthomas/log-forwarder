package config

import "fmt"

func (c *Config) validateFilter() error {
	if !c.Filter.Enabled() {
		return nil
	}
	if err := validateOnMissing("filter", c.Filter.OnMissing); err != nil {
		return err
	}
	onMissingDefault := c.Filter.OnMissing
	if onMissingDefault == "" {
		onMissingDefault = "drop"
	}
	return validateFilterRules("filter", c.Filter.Match, onMissingDefault, c.Filter.Rules)
}

func validateFilterRules(prefix, match, onMissingDefault string, rules []FilterRuleConfig) error {
	if len(rules) == 0 {
		return fmt.Errorf("%s.rules must not be empty when filter is configured", prefix)
	}
	if match == "" {
		match = "all"
	}
	switch match {
	case "all", "any":
	default:
		return fmt.Errorf("%s.match must be all or any", prefix)
	}

	for i, rule := range rules {
		if err := validateFilterRule(fmt.Sprintf("%s.rules[%d]", prefix, i), onMissingDefault, rule); err != nil {
			return err
		}
	}
	return nil
}

func validateFilterRule(prefix, onMissingDefault string, rule FilterRuleConfig) error {
	if rule.Type == "" {
		return fmt.Errorf("%s.type must not be empty", prefix)
	}
	if !knownFilterType(rule.Type) {
		return unknownTypeError(prefix+".type", rule.Type, filterTypes)
	}

	switch rule.Type {
	case "field":
		if rule.Field == "" {
			return fmt.Errorf("%s.field is required when type is field", prefix)
		}
		if rule.Op == "" {
			return fmt.Errorf("%s.op is required when type is field", prefix)
		}
		if err := validateOnMissing(prefix, rule.OnMissing); err != nil {
			return err
		}
		switch rule.Op {
		case "eq", "neq":
			if rule.Value == "" {
				return fmt.Errorf("%s.value is required when op is %s", prefix, rule.Op)
			}
		case "in", "not_in":
			if len(rule.Values) == 0 {
				return fmt.Errorf("%s.values is required when op is %s", prefix, rule.Op)
			}
		default:
			return fmt.Errorf("%s.op must be eq, neq, in, or not_in", prefix)
		}
	case "compound":
		return validateFilterRules(prefix, rule.Match, onMissingDefault, rule.Rules)
	default:
		// Custom filter registered via filter.Register; field validation deferred to factory.
		return nil
	}
	return nil
}

func validateOnMissing(prefix, value string) error {
	if value == "" {
		return nil
	}
	switch value {
	case "drop", "pass":
		return nil
	default:
		return fmt.Errorf("%s.on_missing must be drop or pass", prefix)
	}
}
