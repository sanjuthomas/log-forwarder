package deadletter

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Entry is metadata for one dead-letter batch file (no log record bodies).
type Entry struct {
	Filename      string `json:"filename"`
	CreatedAt     string `json:"created_at"`
	EventCount    int    `json:"event_count"`
	Bytes         int64  `json:"bytes"`
	FailureReason string `json:"failure_reason"`
	SinkType      string `json:"sink_type"`
	BatchAttempts int    `json:"batch_attempts"`
}

// WriteInfo is stored alongside each JSONL batch at write time.
type WriteInfo struct {
	FailureReason string
	SinkType      string
	BatchAttempts int
}

func metaPath(dir, filename string) string {
	return filepath.Join(dir, filename+".meta.json")
}

func writeMeta(dir, filename string, entry Entry) error {
	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshal dead letter metadata: %w", err)
	}
	metaFile := metaPath(dir, strings.TrimSuffix(filename, ".jsonl"))
	if err := os.WriteFile(metaFile, data, 0o644); err != nil {
		return fmt.Errorf("write dead letter metadata: %w", err)
	}
	return nil
}

// ListEntries scans dir for committed JSONL batches and returns metadata only.
func ListEntries(dir string) ([]Entry, error) {
	if dir == "" {
		return nil, nil
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read dead letter directory: %w", err)
	}

	var out []Entry
	for _, ent := range entries {
		if ent.IsDir() {
			continue
		}
		name := ent.Name()
		if !strings.HasSuffix(name, ".jsonl") || strings.Contains(name, ".tmp") {
			continue
		}
		if filepath.Base(name) != name {
			continue
		}

		entry, err := entryForFile(dir, name, ent)
		if err != nil {
			return nil, err
		}
		out = append(out, entry)
	}
	return out, nil
}

func entryForFile(dir, filename string, ent os.DirEntry) (Entry, error) {
	metaFile := metaPath(dir, strings.TrimSuffix(filename, ".jsonl"))
	if data, err := os.ReadFile(metaFile); err == nil {
		var entry Entry
		if err := json.Unmarshal(data, &entry); err != nil {
			return Entry{}, fmt.Errorf("parse metadata for %q: %w", filename, err)
		}
		if entry.Filename == "" {
			entry.Filename = filename
		}
		return entry, nil
	}

	info, err := ent.Info()
	if err != nil {
		return Entry{}, fmt.Errorf("stat %q: %w", filename, err)
	}

	eventCount, err := countJSONLLines(filepath.Join(dir, filename))
	if err != nil {
		return Entry{}, err
	}

	return Entry{
		Filename:   filename,
		CreatedAt:  info.ModTime().UTC().Format(time.RFC3339),
		EventCount: eventCount,
		Bytes:      info.Size(),
	}, nil
}

func countJSONLLines(path string) (int, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, fmt.Errorf("read %q: %w", filepath.Base(path), err)
	}
	if len(data) == 0 {
		return 0, nil
	}
	lines := strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
	return len(lines), nil
}
