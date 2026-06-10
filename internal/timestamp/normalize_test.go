package timestamp

import (
	"testing"
	"time"

	"github.com/sanjuthomas/log-forwarder/internal/config"
	"github.com/sanjuthomas/log-forwarder/internal/transform"
)

func TestNormalizeRFC3339(t *testing.T) {
	t.Parallel()

	n := mustNormalizer(t, config.TimestampConfig{Field: "timestamp"})
	record := transform.Record{"timestamp": "2024-01-01T12:00:00Z"}

	if failed := n.Normalize(record); failed {
		t.Fatal("expected parse success")
	}
	if record["timestamp"] != "2024-01-01T12:00:00Z" {
		t.Fatalf("timestamp = %q, want RFC3339Nano UTC", record["timestamp"])
	}
	if _, ok := record["timestamp_source"]; ok {
		t.Fatal("did not expect timestamp_source on success")
	}
}

func TestNormalizeSpringBootLayout(t *testing.T) {
	t.Parallel()

	n := mustNormalizer(t, config.TimestampConfig{
		Field:  "timestamp",
		Format: "2006-01-02 15:04:05.000",
	})
	record := transform.Record{"timestamp": "2026-06-08 10:16:22.901"}

	if failed := n.Normalize(record); failed {
		t.Fatal("expected parse success")
	}
	want := time.Date(2026, 6, 8, 10, 16, 22, 901000000, time.UTC).Format(time.RFC3339Nano)
	if record["timestamp"] != want {
		t.Fatalf("timestamp = %q, want %q", record["timestamp"], want)
	}
}

func TestNormalizeNoZoneUsesDefaultTimezone(t *testing.T) {
	t.Parallel()

	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	n := mustNormalizer(t, config.TimestampConfig{
		Field:           "timestamp",
		Format:          "2006-01-02 15:04:05",
		DefaultTimezone: "America/New_York",
	})
	record := transform.Record{"timestamp": "2026-06-08 10:16:22"}

	if failed := n.Normalize(record); failed {
		t.Fatal("expected parse success")
	}
	want := time.Date(2026, 6, 8, 10, 16, 22, 0, loc).UTC().Format(time.RFC3339Nano)
	if record["timestamp"] != want {
		t.Fatalf("timestamp = %q, want %q", record["timestamp"], want)
	}
}

func TestNormalizeMissingFieldUsesProcessingTime(t *testing.T) {
	t.Parallel()

	fixed := time.Date(2026, 6, 10, 2, 30, 0, 123456789, time.UTC)
	n := mustNormalizer(t, config.TimestampConfig{Field: "timestamp"})
	n.now = func() time.Time { return fixed }

	record := transform.Record{"level": "INFO"}
	if failed := n.Normalize(record); !failed {
		t.Fatal("expected parse failure")
	}
	if record["timestamp"] != fixed.Format(time.RFC3339Nano) {
		t.Fatalf("timestamp = %q", record["timestamp"])
	}
	if record["timestamp_source"] != config.TimestampSourceProcessing {
		t.Fatalf("timestamp_source = %v", record["timestamp_source"])
	}
	if _, ok := record["timestamp_raw"]; ok {
		t.Fatal("did not expect timestamp_raw when field missing")
	}
}

func TestNormalizeInvalidValueUsesProcessingTime(t *testing.T) {
	t.Parallel()

	fixed := time.Date(2026, 6, 10, 2, 30, 0, 0, time.UTC)
	n := mustNormalizer(t, config.TimestampConfig{Field: "timestamp"})
	n.now = func() time.Time { return fixed }

	record := transform.Record{"timestamp": "not-a-valid-time"}
	if failed := n.Normalize(record); !failed {
		t.Fatal("expected parse failure")
	}
	if record["timestamp_raw"] != "not-a-valid-time" {
		t.Fatalf("timestamp_raw = %v", record["timestamp_raw"])
	}
	if record["timestamp_source"] != config.TimestampSourceProcessing {
		t.Fatalf("timestamp_source = %v", record["timestamp_source"])
	}
}

func TestNormalizeEmptyValueUsesProcessingTime(t *testing.T) {
	t.Parallel()

	n := mustNormalizer(t, config.TimestampConfig{Field: "timestamp"})
	record := transform.Record{"timestamp": ""}

	if failed := n.Normalize(record); !failed {
		t.Fatal("expected parse failure for empty timestamp")
	}
	if record["timestamp_source"] != config.TimestampSourceProcessing {
		t.Fatalf("timestamp_source = %v", record["timestamp_source"])
	}
}

func TestNewDisabledReturnsNil(t *testing.T) {
	t.Parallel()

	n, err := New(config.TimestampConfig{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if n != nil {
		t.Fatal("expected nil normalizer when disabled")
	}
}

func mustNormalizer(t *testing.T, cfg config.TimestampConfig) *Normalizer {
	t.Helper()

	n, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return n
}
