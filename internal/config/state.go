package config

import (
	"fmt"
	"path/filepath"
)

func (c *Config) validateState() error {
	statePath, err := filepath.Abs(c.StatePath())
	if err != nil {
		return fmt.Errorf("watch.state.path: %w", err)
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
