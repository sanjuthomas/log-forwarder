package config

import (
	"fmt"
	"path/filepath"
	"time"
)

func (c StateConfig) FlushIntervalDuration() time.Duration {
	if c.FlushInterval == "0" {
		return 0
	}
	if c.FlushInterval == "" {
		return time.Second
	}
	d, err := time.ParseDuration(c.FlushInterval)
	if err != nil {
		return time.Second
	}
	return d
}

func (c StateConfig) PersistOptions() (flushInterval time.Duration, flushEvery int) {
	return c.FlushIntervalDuration(), c.FlushEvery
}

func (c *Config) validateState() error {
	statePath, err := filepath.Abs(c.StatePath())
	if err != nil {
		return fmt.Errorf("watch.state.path: %w", err)
	}

	if c.Watch.State.FlushEvery < 0 {
		return fmt.Errorf("watch.state.flush_every must not be negative")
	}
	if c.Watch.State.FlushInterval != "" && c.Watch.State.FlushInterval != "0" {
		if _, err := time.ParseDuration(c.Watch.State.FlushInterval); err != nil {
			return fmt.Errorf("watch.state.flush_interval: %w", err)
		}
	}

	for _, source := range c.Watch.Entries() {
		watchPath, err := filepath.Abs(source.Path)
		if err != nil {
			return fmt.Errorf("watch path %q: %w", source.Path, err)
		}
		if logFileMatchesWatchPattern(statePath, watchPath, source.Patterns) {
			return fmt.Errorf(
				"watch.state.path %q matches a watch pattern under %q; choose a state file that will not be tailed",
				statePath, watchPath,
			)
		}
	}
	return nil
}
