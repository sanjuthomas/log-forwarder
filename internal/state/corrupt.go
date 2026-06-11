// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package state

import (
	"errors"
	"fmt"
	"os"
	"time"
)

// CorruptWatermarkError indicates the watermark file exists but cannot be parsed.
type CorruptWatermarkError struct {
	Path  string
	Cause error
}

func (e *CorruptWatermarkError) Error() string {
	return fmt.Sprintf(
		"watermark file %q is corrupt or unreadable: %v; rename or remove the file, or restart with --reset-watermarks or watch.state.reset_on_corrupt: true to archive it and start fresh",
		e.Path,
		e.Cause,
	)
}

func (e *CorruptWatermarkError) Unwrap() error {
	return e.Cause
}

func archiveCorruptWatermark(path string) (string, error) {
	ts := time.Now().UTC().Format("2006-01-02T15-04-05Z")
	backup := fmt.Sprintf("%s.corrupt.%s", path, ts)
	if err := os.Rename(path, backup); err != nil {
		return "", fmt.Errorf("archive corrupt watermark file: %w", err)
	}
	return backup, nil
}

func isCorruptWatermarkError(err error) bool {
	var corrupt *CorruptWatermarkError
	return errors.As(err, &corrupt)
}
