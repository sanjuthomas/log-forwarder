package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type LoggingConfig struct {
	Level          string `yaml:"level"`
	Format         string `yaml:"format"`
	File           string `yaml:"file"`
	StatusInterval string `yaml:"status_interval"`
}

func (c LoggingConfig) StatusIntervalDuration() time.Duration {
	if c.StatusInterval == "" || c.StatusInterval == "0" {
		return 0
	}
	d, err := time.ParseDuration(c.StatusInterval)
	if err != nil {
		return 30 * time.Second
	}
	return d
}

func (c *Config) validateLogging() error {
	level := strings.ToLower(c.Logging.Level)
	if level == "" {
		level = "info"
	}
	switch level {
	case "debug", "info", "warn", "error":
	default:
		return fmt.Errorf("logging.level must be debug, info, warn, or error")
	}

	format := strings.ToLower(c.Logging.Format)
	if format == "" {
		format = "text"
	}
	switch format {
	case "text", "json":
	default:
		return fmt.Errorf("logging.format must be text or json")
	}

	if c.Logging.StatusInterval != "" && c.Logging.StatusInterval != "0" {
		if _, err := time.ParseDuration(c.Logging.StatusInterval); err != nil {
			return fmt.Errorf("logging.status_interval: %w", err)
		}
	}

	if c.Logging.File == "" {
		return nil
	}

	logPath, err := filepath.Abs(c.Logging.File)
	if err != nil {
		return fmt.Errorf("logging.file: %w", err)
	}

	for _, source := range c.Watch.Entries() {
		watchPath, err := filepath.Abs(source.Path)
		if err != nil {
			return fmt.Errorf("watch path %q: %w", source.Path, err)
		}

		if pathInside(watchPath, logPath) {
			return fmt.Errorf(
				"logging.file %q must not be inside a watched directory (%q); that would cause the forwarder to read its own logs",
				logPath, watchPath,
			)
		}

		if logFileMatchesWatchPattern(logPath, watchPath, source.Patterns) {
			return fmt.Errorf(
				"logging.file %q matches a watch pattern under %q; choose a log path outside watched directories",
				logPath, watchPath,
			)
		}
	}

	return nil
}

func pathInside(dir, path string) bool {
	dir = filepath.Clean(dir)
	path = filepath.Clean(path)
	if dir == path {
		return true
	}
	rel, err := filepath.Rel(dir, path)
	if err != nil {
		return false
	}
	return rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator))
}

func logFileMatchesWatchPattern(logPath, watchPath string, patterns []string) bool {
	if !pathInside(watchPath, logPath) {
		return false
	}
	base := filepath.Base(logPath)
	for _, pattern := range patterns {
		matched, err := filepath.Match(pattern, base)
		if err == nil && matched {
			return true
		}
	}
	return false
}
