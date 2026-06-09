package integration_test

import (
	"path/filepath"
	"testing"

	"github.com/sanjuthomas/log-forwarder/internal/config"
)

func TestIntegration_ExampleConfigsValidate(t *testing.T) {
	t.Parallel()

	roots := []string{
		"../../configs",
		"../../examples/kafka",
	}

	var paths []string
	for _, root := range roots {
		matches, err := filepath.Glob(filepath.Join(root, "*.yaml"))
		if err != nil {
			t.Fatalf("Glob(%q) error = %v", root, err)
		}
		paths = append(paths, matches...)
	}
	if len(paths) == 0 {
		t.Fatal("no example configs found")
	}

	for _, path := range paths {
		path := path
		t.Run(filepath.Base(path), func(t *testing.T) {
			t.Parallel()
			if _, err := config.Load(path); err != nil {
				t.Fatalf("Load(%q) error = %v", path, err)
			}
		})
	}
}
