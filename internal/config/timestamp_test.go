package config

import "testing"

func TestValidateTimestampRejectsInvalidFormat(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.Timestamp = TimestampConfig{
		Field:  "timestamp",
		Format: "not-a-layout",
	}

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error for invalid timestamp.format")
	}
}

func TestValidateTimestampRejectsInvalidTimezone(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.Timestamp = TimestampConfig{
		Field:           "timestamp",
		DefaultTimezone: "Not/A/Zone",
	}

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error for invalid default_timezone")
	}
}

func TestValidateTimestampRejectsInvalidOutput(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.Timestamp = TimestampConfig{
		Field:  "timestamp",
		Output: "unix",
	}

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error for invalid timestamp.output")
	}
}

func TestValidateTimestampAcceptsSpringBootLayout(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.Timestamp = TimestampConfig{
		Field:  "timestamp",
		Format: "2006-01-02 15:04:05.000",
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}
