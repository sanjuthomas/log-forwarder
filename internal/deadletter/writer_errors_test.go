// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package deadletter

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWriteBatchFailsWhenDirectoryPathIsFile(t *testing.T) {
	t.Parallel()

	base := t.TempDir()
	notDir := filepath.Join(base, "blocked")
	if err := os.WriteFile(notDir, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	_, _, err := WriteBatch(notDir, [][]byte{[]byte(`{"x":1}`)}, WriteInfo{})
	if err == nil {
		t.Fatal("expected error when dead letter path is a file")
	}
}

func TestListEntriesInvalidMetadataSidecar(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	filename := "2026-01-01T00-00-00Z_abcd.jsonl"
	if err := os.WriteFile(filepath.Join(dir, filename), []byte(`{"line":1}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(metaPath(dir, strings.TrimSuffix(filename, ".jsonl")), []byte("{"), 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := ListEntries(dir); err == nil {
		t.Fatal("expected error for invalid metadata sidecar")
	}
}

func TestCountJSONLLinesMissingFile(t *testing.T) {
	t.Parallel()

	if _, err := countJSONLLines(filepath.Join(t.TempDir(), "missing.jsonl")); err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestWriteBatchWritesMetadataSidecar(t *testing.T) {
	dir := t.TempDir()
	filename, _, err := WriteBatch(dir, [][]byte{[]byte(`{"a":1}`)}, WriteInfo{
		FailureReason: "timeout",
		SinkType:      "file",
		BatchAttempts: 1,
	})
	if err != nil {
		t.Fatalf("WriteBatch() error = %v", err)
	}
	meta := metaPath(dir, strings.TrimSuffix(filename, ".jsonl"))
	if _, err := os.Stat(meta); err != nil {
		t.Fatalf("Stat(metadata) error = %v", err)
	}
}

func TestListEntriesReadDirFailure(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	if err := os.Chmod(dir, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })

	if _, err := ListEntries(dir); err == nil {
		t.Fatal("expected error when directory is not readable")
	}
}

func TestWriteBatchFailsOnReadOnlyDirectory(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	if err := os.Chmod(dir, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })

	_, _, err := WriteBatch(dir, [][]byte{[]byte(`{"x":1}`)}, WriteInfo{})
	if err == nil {
		t.Fatal("expected error when directory is not writable")
	}
}

func TestWriteMetaFailsOnReadOnlyDirectory(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	if err := os.Chmod(dir, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })

	if err := writeMeta(dir, "batch.jsonl", Entry{Filename: "batch.jsonl"}); err == nil {
		t.Fatal("expected error writing metadata to read-only directory")
	}
}

func TestValidateWritableFailsWhenProbePathIsDirectory(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	probe := filepath.Join(dir, ".write-probe")
	if err := os.Mkdir(probe, 0o755); err != nil {
		t.Fatal(err)
	}

	if err := ValidateWritable(dir); err == nil {
		t.Fatal("expected error when probe path is a directory")
	}
}
