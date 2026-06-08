package state

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestStoreSetAndLoad(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "watermarks.json")

	store, err := NewStore(path)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}

	if err := store.Set("/tmp/app.log", 128, 42); err != nil {
		t.Fatalf("Set() error = %v", err)
	}

	reloaded, err := NewStore(path)
	if err != nil {
		t.Fatalf("NewStore() reload error = %v", err)
	}

	entry, ok := reloaded.Get("/tmp/app.log")
	if !ok {
		t.Fatal("expected watermark entry")
	}
	if entry.Offset != 128 || entry.Inode != 42 {
		t.Fatalf("entry = %+v, want offset=128 inode=42", entry)
	}
}

func TestStoreLoadMissingFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "missing.json")

	store, err := NewStore(path)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}

	if _, ok := store.Get("/tmp/app.log"); ok {
		t.Fatal("expected no watermark for new store")
	}
}

func TestStorePersistsJSON(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "watermarks.json")

	store, err := NewStore(path)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	if err := store.Set("/tmp/a.log", 10, 1); err != nil {
		t.Fatalf("Set() error = %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}

	var persisted fileState
	if err := json.Unmarshal(data, &persisted); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if persisted.Files["/tmp/a.log"].Offset != 10 {
		t.Fatalf("persisted offset = %d", persisted.Files["/tmp/a.log"].Offset)
	}
}
