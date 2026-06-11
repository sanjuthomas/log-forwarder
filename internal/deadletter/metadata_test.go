// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package deadletter

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestListEntriesEmptyDir(t *testing.T) {
	t.Parallel()

	entries, err := ListEntries("")
	if err != nil {
		t.Fatalf("ListEntries(\"\") error = %v", err)
	}
	if entries != nil {
		t.Fatalf("entries = %v, want nil", entries)
	}
}

func TestListEntriesMissingDir(t *testing.T) {
	t.Parallel()

	entries, err := ListEntries(filepath.Join(t.TempDir(), "missing"))
	if err != nil {
		t.Fatalf("ListEntries() error = %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("len(entries) = %d, want 0", len(entries))
	}
}

func TestListEntriesInfersMetadataWithoutSidecar(t *testing.T) {
	dir := t.TempDir()
	content := `{"line":1}` + "\n" + `{"line":2}` + "\n"
	filename := "2026-01-01T00-00-00Z_abcd.jsonl"
	if err := os.WriteFile(filepath.Join(dir, filename), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	entries, err := ListEntries(dir)
	if err != nil {
		t.Fatalf("ListEntries() error = %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("len(entries) = %d, want 1", len(entries))
	}
	if entries[0].Filename != filename {
		t.Fatalf("filename = %q, want %q", entries[0].Filename, filename)
	}
	if entries[0].EventCount != 2 {
		t.Fatalf("event_count = %d, want 2", entries[0].EventCount)
	}
	if entries[0].FailureReason != "" {
		t.Fatalf("failure_reason = %q, want empty when sidecar missing", entries[0].FailureReason)
	}
}

func TestCountJSONLLinesEmptyFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "empty.jsonl")
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatal(err)
	}

	count, err := countJSONLLines(path)
	if err != nil {
		t.Fatalf("countJSONLLines() error = %v", err)
	}
	if count != 0 {
		t.Fatalf("count = %d, want 0", count)
	}
}

func TestListEntriesReturnsMetadataFromSidecar(t *testing.T) {
	dir := t.TempDir()
	payloads := [][]byte{[]byte(`{"message":"secret-line"}`)}
	filename, _, err := WriteBatch(dir, payloads, WriteInfo{
		FailureReason: "kafka publish: broker unavailable",
		SinkType:      "kafka",
		BatchAttempts: 2,
	})
	if err != nil {
		t.Fatalf("WriteBatch() error = %v", err)
	}

	entries, err := ListEntries(dir)
	if err != nil {
		t.Fatalf("ListEntries() error = %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("len(entries) = %d, want 1", len(entries))
	}
	entry := entries[0]
	if entry.Filename != filename {
		t.Fatalf("filename = %q, want %q", entry.Filename, filename)
	}
	if entry.EventCount != 1 {
		t.Fatalf("event_count = %d, want 1", entry.EventCount)
	}
	if entry.SinkType != "kafka" {
		t.Fatalf("sink_type = %q, want kafka", entry.SinkType)
	}
	if entry.FailureReason == "" {
		t.Fatal("expected failure_reason in metadata")
	}
	if entry.BatchAttempts != 2 {
		t.Fatalf("batch_attempts = %d, want 2", entry.BatchAttempts)
	}

	data, err := json.Marshal(entries)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "secret-line") {
		t.Fatalf("metadata response must not include record bodies: %s", data)
	}
}

func TestListEntriesSkipsTempFiles(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "batch.tmp.jsonl"), []byte("x\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	entries, err := ListEntries(dir)
	if err != nil {
		t.Fatalf("ListEntries() error = %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("len(entries) = %d, want 0", len(entries))
	}
}
