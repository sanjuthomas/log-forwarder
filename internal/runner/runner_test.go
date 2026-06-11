package runner_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sanjuthomas/log-forwarder/internal/runner"
)

func TestWaitCancelsOnFirstError(t *testing.T) {
	t.Parallel()

	errCh := make(chan error, 2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- runner.Wait(errCh, cancel)
	}()

	errCh <- errors.New("pipeline failed")

	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("context was not canceled after first runner error")
	}

	errCh <- context.Canceled

	select {
	case err := <-done:
		if err == nil || err.Error() != "pipeline failed" {
			t.Fatalf("Wait() error = %v, want pipeline failed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Wait() did not return after both runners exited")
	}
}

func TestWaitReturnsNilOnGracefulCancel(t *testing.T) {
	t.Parallel()

	errCh := make(chan error, 2)
	_, cancel := context.WithCancel(context.Background())

	errCh <- context.Canceled
	errCh <- context.Canceled

	if err := runner.Wait(errCh, cancel); err != nil {
		t.Fatalf("Wait() error = %v, want nil", err)
	}
}
