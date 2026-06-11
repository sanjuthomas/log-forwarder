// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package runner

import (
	"context"
	"errors"
)

// Wait receives errors from two goroutines (typically watcher and pipeline).
// When the first returns a non-cancel error, cancel is invoked so the other
// goroutine can unblock and exit.
func Wait(errCh <-chan error, cancel context.CancelFunc) error {
	var first error
	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil && !errors.Is(err, context.Canceled) {
			if first == nil {
				first = err
				if cancel != nil {
					cancel()
				}
			}
		}
	}
	return first
}
