package config

import (
	"fmt"
	"path/filepath"

	"github.com/sanjuthomas/log-forwarder/internal/deadletter"
)

func validateDeadLetterPath(c *Config, path string) error {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return fmt.Errorf("pipeline.publish_batch.dead_letter.path: %w", err)
	}

	for _, source := range c.Watch.Entries() {
		watchPath, err := filepath.Abs(source.Path)
		if err != nil {
			return fmt.Errorf("watch path %q: %w", source.Path, err)
		}
		if pathInside(watchPath, absPath) {
			return fmt.Errorf(
				"pipeline.publish_batch.dead_letter.path %q must not be inside a watched directory (%q)",
				absPath, watchPath,
			)
		}
	}

	if err := deadletter.ValidateWritable(absPath); err != nil {
		return fmt.Errorf("pipeline.publish_batch.dead_letter.path: %w", err)
	}
	return nil
}
