// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package deadletter

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidateWritableCreatesAndProbesDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "dlq")
	if err := ValidateWritable(dir); err != nil {
		t.Fatalf("ValidateWritable() error = %v", err)
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("Stat() error = %v", err)
	}
}

func TestWriteBatchRejectsEmptyBatch(t *testing.T) {
	t.Parallel()

	_, _, err := WriteBatch(t.TempDir(), nil, WriteInfo{})
	if err == nil {
		t.Fatal("expected error for empty batch")
	}
	if !strings.Contains(err.Error(), "empty batch") {
		t.Fatalf("error = %q", err.Error())
	}
}

func TestValidateWritableRejectsEmptyPath(t *testing.T) {
	t.Parallel()

	if err := ValidateWritable(""); err == nil {
		t.Fatal("expected error for empty path")
	}
}

func TestValidateWritableRejectsReadOnlyParent(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	readOnly := filepath.Join(dir, "readonly")
	if err := os.Mkdir(readOnly, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(readOnly, 0o755) })

	if err := ValidateWritable(filepath.Join(readOnly, "dlq")); err == nil {
		t.Fatal("expected error when parent directory is not writable")
	}
}

func TestWriteBatchCreatesJSONLFile(t *testing.T) {
	dir := t.TempDir()
	payloads := [][]byte{
		[]byte(`{"message":"one"}`),
		[]byte(`{"message":"two"}`),
	}

	filename, bytes, err := WriteBatch(dir, payloads, WriteInfo{
		FailureReason: "publish failed",
		SinkType:      "file",
		BatchAttempts: 2,
	})
	if err != nil {
		t.Fatalf("WriteBatch() error = %v", err)
	}
	if !strings.HasSuffix(filename, ".jsonl") {
		t.Fatalf("filename = %q, want .jsonl suffix", filename)
	}
	if bytes == 0 {
		t.Fatal("expected bytes written > 0")
	}

	content, err := os.ReadFile(filepath.Join(dir, filename))
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	lines := strings.Split(strings.TrimSuffix(string(content), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("line count = %d, want 2", len(lines))
	}
	if lines[0] != `{"message":"one"}` || lines[1] != `{"message":"two"}` {
		t.Fatalf("content = %q", content)
	}
}
