package state

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNewStoreCorruptWatermarkFailsWithActionableError(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "watermarks.json")
	if err := os.WriteFile(path, []byte("{not-json"), 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := NewStore(path)
	if err == nil {
		t.Fatal("expected error for corrupt watermark file")
	}
	var corrupt *CorruptWatermarkError
	if !errors.As(err, &corrupt) {
		t.Fatalf("error = %v, want CorruptWatermarkError", err)
	}
	if !strings.Contains(err.Error(), "--reset-watermarks") {
		t.Fatalf("error = %q, want reset guidance", err.Error())
	}
}

func TestNewStoreCorruptWatermarkResetOnCorrupt(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "watermarks.json")
	if err := os.WriteFile(path, []byte("{not-json"), 0o644); err != nil {
		t.Fatal(err)
	}

	store, err := NewStore(path, Options{ResetOnCorrupt: true})
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	if backup := store.CorruptBackupPath(); backup == "" {
		t.Fatal("expected corrupt backup path")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("original watermark file should be archived, stat err = %v", err)
	}
	if _, err := os.Stat(store.CorruptBackupPath()); err != nil {
		t.Fatalf("backup file missing: %v", err)
	}
	if err := store.Set("/tmp/app.log", 42, 7); err != nil {
		t.Fatalf("Set() error = %v", err)
	}
}
