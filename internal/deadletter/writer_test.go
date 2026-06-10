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
