package watcher

import (
	"io"
	"log/slog"
	"testing"

	"github.com/sanjuthomas/log-forwarder/internal/config"
	"github.com/sanjuthomas/log-forwarder/internal/metrics"
)

func TestSendLineEventBlock(t *testing.T) {
	t.Parallel()

	lines := make(chan LineEvent, 1)
	lines <- LineEvent{Path: "/tmp/app.log", Line: []byte("filled")}

	w := &Watcher{
		onFull:  "block",
		lines:   lines,
		metrics: &metrics.Collector{},
		logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	done := make(chan struct{})
	go func() {
		w.sendLineEvent(LineEvent{Path: "/tmp/app.log", Line: []byte("blocked")})
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("expected block mode to wait on full buffer")
	case event := <-lines:
		if string(event.Line) != "filled" {
			t.Fatalf("first event = %q", event.Line)
		}
	}

	<-done
	if got := len(lines); got != 1 {
		t.Fatalf("buffer len = %d, want 1", got)
	}
}

func TestSendLineEventDrop(t *testing.T) {
	t.Parallel()

	lines := make(chan LineEvent, 1)
	lines <- LineEvent{Path: "/tmp/app.log", Line: []byte("filled")}

	w := &Watcher{
		onFull:  "drop",
		lines:   lines,
		metrics: &metrics.Collector{},
		logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	w.sendLineEvent(LineEvent{Path: "/tmp/app.log", Line: []byte("dropped")})

	if got := len(lines); got != 1 {
		t.Fatalf("buffer len = %d, want 1", got)
	}
}

func TestNewDefaultsOnFullToBlock(t *testing.T) {
	t.Parallel()

	cfg := config.Default()
	cfg.Pipeline.OnFull = ""
	w := New(cfg, make(chan LineEvent), nil, &metrics.Collector{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if w.onFull != "block" {
		t.Fatalf("onFull = %q, want block", w.onFull)
	}
}
