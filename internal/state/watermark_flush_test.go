package state

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestStoreDebouncedPersist(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "watermarks.json")

	store, err := NewStore(path, Options{FlushInterval: time.Hour})
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}

	for i := range 100 {
		if err := store.Set("/tmp/app.log", int64(i+1), 42); err != nil {
			t.Fatalf("Set() error = %v", err)
		}
	}

	if _, err := os.Stat(path); err == nil {
		t.Fatal("expected no watermark file before flush")
	} else if !os.IsNotExist(err) {
		t.Fatalf("Stat() error = %v", err)
	}

	entry, ok := store.Get("/tmp/app.log")
	if !ok || entry.Offset != 100 {
		t.Fatalf("in-memory entry = %+v, want offset 100", entry)
	}

	if err := store.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	reloaded, err := NewStore(path)
	if err != nil {
		t.Fatalf("NewStore() reload error = %v", err)
	}
	entry, ok = reloaded.Get("/tmp/app.log")
	if !ok || entry.Offset != 100 || entry.Inode != 42 {
		t.Fatalf("reloaded entry = %+v, want offset 100 inode 42", entry)
	}
}

func TestStoreFlushEvery(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "watermarks.json")

	store, err := NewStore(path, Options{FlushEvery: 3})
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}

	for i := range 5 {
		if err := store.Set("/tmp/app.log", int64(i+1), 1); err != nil {
			t.Fatalf("Set() error = %v", err)
		}
	}

	reloaded, err := NewStore(path)
	if err != nil {
		t.Fatalf("NewStore() reload error = %v", err)
	}
	entry, ok := reloaded.Get("/tmp/app.log")
	if !ok || entry.Offset != 3 {
		t.Fatalf("reloaded entry = %+v, want offset 3 after first count flush", entry)
	}

	if err := store.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	reloaded, err = NewStore(path)
	if err != nil {
		t.Fatalf("NewStore() reload error = %v", err)
	}
	entry, ok = reloaded.Get("/tmp/app.log")
	if !ok || entry.Offset != 5 {
		t.Fatalf("reloaded entry = %+v, want offset 5 after final flush", entry)
	}
}

func TestStorePeriodicFlush(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "watermarks.json")

	store, err := NewStore(path, Options{FlushInterval: 20 * time.Millisecond})
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go store.RunPeriodicFlush(ctx)

	if err := store.Set("/tmp/app.log", 64, 7); err != nil {
		t.Fatalf("Set() error = %v", err)
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(path)
		if err == nil {
			var persisted fileState
			if err := json.Unmarshal(data, &persisted); err != nil {
				t.Fatalf("Unmarshal() error = %v", err)
			}
			if persisted.Files["/tmp/app.log"].Offset == 64 {
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timeout waiting for periodic watermark flush")
}

func TestStoreFlushNoOpWhenClean(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "watermarks.json")

	store, err := NewStore(path, Options{FlushInterval: time.Hour})
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}

	if err := store.Flush(); err != nil {
		t.Fatalf("Flush() on clean store error = %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("expected no watermark file after flush on clean store")
	}
}
