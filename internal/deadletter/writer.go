// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package deadletter

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// ValidateWritable ensures path exists and the forwarder can create files there.
func ValidateWritable(path string) error {
	if path == "" {
		return fmt.Errorf("path is required")
	}
	absPath, err := filepath.Abs(path)
	if err != nil {
		return fmt.Errorf("resolve path: %w", err)
	}
	if err := os.MkdirAll(absPath, 0o755); err != nil {
		return fmt.Errorf("create directory: %w", err)
	}

	probe := filepath.Join(absPath, ".write-probe")
	if err := os.WriteFile(probe, []byte("ok"), 0o644); err != nil {
		return fmt.Errorf("write probe file: %w", err)
	}
	if err := os.Remove(probe); err != nil {
		return fmt.Errorf("remove probe file: %w", err)
	}
	return nil
}

// WriteBatch writes one JSONL file per failed batch and returns the filename and total bytes written.
func WriteBatch(dir string, payloads [][]byte, info WriteInfo) (filename string, bytes int, err error) {
	if len(payloads) == 0 {
		return "", 0, fmt.Errorf("empty batch")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", 0, fmt.Errorf("create dead letter directory: %w", err)
	}

	filename = batchFilename()
	tmpPath := filepath.Join(dir, filename+".tmp")
	finalPath := filepath.Join(dir, filename)

	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return "", 0, fmt.Errorf("create dead letter file: %w", err)
	}

	for _, payload := range payloads {
		if _, err := f.Write(payload); err != nil {
			_ = f.Close()
			_ = os.Remove(tmpPath)
			return "", 0, fmt.Errorf("write dead letter record: %w", err)
		}
		if _, err := f.Write([]byte("\n")); err != nil {
			_ = f.Close()
			_ = os.Remove(tmpPath)
			return "", 0, fmt.Errorf("write dead letter newline: %w", err)
		}
		bytes += len(payload) + 1
	}

	if err := f.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return "", 0, fmt.Errorf("close dead letter file: %w", err)
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		_ = os.Remove(tmpPath)
		return "", 0, fmt.Errorf("commit dead letter file: %w", err)
	}

	createdAt := time.Now().UTC()
	if err := writeMeta(dir, filename, Entry{
		Filename:      filename,
		CreatedAt:     createdAt.Format(time.RFC3339),
		EventCount:    len(payloads),
		Bytes:         int64(bytes),
		FailureReason: info.FailureReason,
		SinkType:      info.SinkType,
		BatchAttempts: info.BatchAttempts,
	}); err != nil {
		_ = os.Remove(finalPath)
		return "", 0, err
	}
	return filename, bytes, nil
}

func batchFilename() string {
	ts := time.Now().UTC().Format("2006-01-02T15-04-05Z")
	return fmt.Sprintf("%s_%s.jsonl", ts, batchID())
}

func batchID() string {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}
