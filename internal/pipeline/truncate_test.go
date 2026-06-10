package pipeline

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/sanjuthomas/log-forwarder/internal/config"
	"github.com/sanjuthomas/log-forwarder/internal/transform"
	"github.com/sanjuthomas/log-forwarder/internal/watcher"
)

func TestMarshalPublishPayloadUnderLimit(t *testing.T) {
	t.Parallel()

	record := transform.Record{
		"timestamp": "2026-06-08T10:00:00Z",
		"level":     "INFO",
		"message":   "hello",
		"_path":     "/var/log/app.log",
	}

	payload, truncated, err := marshalPublishPayload(record, 1024, config.DefaultTruncateField, config.DefaultTruncateSuffix)
	if err != nil {
		t.Fatalf("marshalPublishPayload() error = %v", err)
	}
	if truncated {
		t.Fatal("expected no truncation")
	}
	if len(payload) > 1024 {
		t.Fatalf("payload len = %d, want <= 1024", len(payload))
	}
}

func TestMarshalPublishPayloadTruncatesMessage(t *testing.T) {
	t.Parallel()

	huge := strings.Repeat("x", 4096)
	record := transform.Record{
		"timestamp": "2026-06-08T10:00:00Z",
		"level":     "INFO",
		"message":   huge,
		"_path":     "/var/log/app.log",
	}

	maxBytes := 512
	payload, truncated, err := marshalPublishPayload(record, maxBytes, config.DefaultTruncateField, config.DefaultTruncateSuffix)
	if err != nil {
		t.Fatalf("marshalPublishPayload() error = %v", err)
	}
	if !truncated {
		t.Fatal("expected truncation")
	}
	if len(payload) > maxBytes {
		t.Fatalf("payload len = %d, want <= %d", len(payload), maxBytes)
	}

	var out map[string]any
	if err := json.Unmarshal(payload, &out); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if out["message_truncated"] != true {
		t.Fatalf("message_truncated = %v, want true", out["message_truncated"])
	}
	if out["publish_truncated"] != true {
		t.Fatalf("publish_truncated = %v, want true", out["publish_truncated"])
	}
	origBytes, ok := out["message_original_bytes"].(float64)
	if !ok || int(origBytes) != len(huge) {
		t.Fatalf("message_original_bytes = %v, want %d", out["message_original_bytes"], len(huge))
	}
	msg, ok := out["message"].(string)
	if !ok || !strings.HasSuffix(msg, config.DefaultTruncateSuffix) {
		t.Fatalf("message = %q, want suffix %q", msg, config.DefaultTruncateSuffix)
	}
}

func TestMarshalPublishPayloadUTF8Safe(t *testing.T) {
	t.Parallel()

	huge := strings.Repeat("你好", 2048)
	record := transform.Record{
		"timestamp": "2026-06-08T10:00:00Z",
		"level":     "INFO",
		"message":   huge,
		"_path":     "/var/log/app.log",
	}

	maxBytes := 400
	payload, truncated, err := marshalPublishPayload(record, maxBytes, config.DefaultTruncateField, config.DefaultTruncateSuffix)
	if err != nil {
		t.Fatalf("marshalPublishPayload() error = %v", err)
	}
	if !truncated {
		t.Fatal("expected truncation")
	}
	if len(payload) > maxBytes {
		t.Fatalf("payload len = %d, want <= %d", len(payload), maxBytes)
	}

	var out map[string]any
	if err := json.Unmarshal(payload, &out); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	msg, ok := out["message"].(string)
	if !ok {
		t.Fatalf("message type = %T", out["message"])
	}
	if !utf8.ValidString(msg) {
		t.Fatalf("message is not valid UTF-8: %q", msg)
	}
}

func TestMarshalPublishPayloadTruncatesRaw(t *testing.T) {
	t.Parallel()

	huge := strings.Repeat("y", 4096)
	record := transform.Record{
		"_raw":   huge,
		"_path":  "/var/log/app.log",
		"_error": "transform failed",
	}

	maxBytes := 512
	payload, truncated, err := marshalPublishPayload(record, maxBytes, config.DefaultTruncateField, config.DefaultTruncateSuffix)
	if err != nil {
		t.Fatalf("marshalPublishPayload() error = %v", err)
	}
	if !truncated {
		t.Fatal("expected truncation")
	}
	if len(payload) > maxBytes {
		t.Fatalf("payload len = %d, want <= %d", len(payload), maxBytes)
	}

	var out map[string]any
	if err := json.Unmarshal(payload, &out); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if out["_raw_truncated"] != true {
		t.Fatalf("_raw_truncated = %v, want true", out["_raw_truncated"])
	}
}

func TestMarshalPublishPayloadDisabled(t *testing.T) {
	t.Parallel()

	huge := strings.Repeat("z", 4096)
	record := transform.Record{
		"message": huge,
		"_path":   "/var/log/app.log",
	}

	payload, truncated, err := marshalPublishPayload(record, 0, config.DefaultTruncateField, config.DefaultTruncateSuffix)
	if err != nil {
		t.Fatalf("marshalPublishPayload() error = %v", err)
	}
	if truncated {
		t.Fatal("expected no truncation when disabled")
	}
	if len(payload) <= 4096 {
		t.Fatalf("payload len = %d, want full record marshaled", len(payload))
	}
}

func TestMarshalPublishPayloadStillTooLargeFails(t *testing.T) {
	t.Parallel()

	record := transform.Record{
		"timestamp": "2026-06-08T10:00:00Z",
		"level":     "INFO",
		"message":   strings.Repeat("a", 4096),
		"_path":     "/var/log/app.log",
	}

	_, _, err := marshalPublishPayload(record, 20, config.DefaultTruncateField, config.DefaultTruncateSuffix)
	if err == nil {
		t.Fatal("expected error when record still exceeds max publish size")
	}
}

func TestPipelineTruncatesOversizedMessage(t *testing.T) {
	huge := strings.Repeat("m", 8192)
	cfg := config.Default()
	cfg.Transform = config.TransformConfig{
		Type:    "delimiter",
		Columns: []string{"timestamp", "level", "message"},
		OnError: "wrap",
	}
	cfg.Pipeline.MaxPublishBytes = 600

	sink := &capturingSink{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	pipe, err := New(cfg, sink, logger, Options{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	lines := make(chan watcher.LineEvent, 1)
	lines <- watcher.LineEvent{
		Path:   "/tmp/test.log",
		Line:   []byte("2026-06-08T10:00:00Z\tINFO\t" + huge),
		Offset: 42,
	}
	close(lines)

	if err := pipe.Run(context.Background(), lines); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if len(sink.payloads) != 1 {
		t.Fatalf("publish count = %d, want 1", len(sink.payloads))
	}
	if len(sink.payloads[0]) > cfg.Pipeline.MaxPublishBytes {
		t.Fatalf("payload len = %d, want <= %d", len(sink.payloads[0]), cfg.Pipeline.MaxPublishBytes)
	}

	var out map[string]any
	if err := json.Unmarshal(sink.payloads[0], &out); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if out["message_truncated"] != true {
		t.Fatalf("message_truncated = %v, want true", out["message_truncated"])
	}
}

type capturingSink struct {
	payloads [][]byte
}

func (c *capturingSink) Publish(_ context.Context, payload []byte) error {
	c.payloads = append(c.payloads, append([]byte(nil), payload...))
	return nil
}

func (c *capturingSink) Close() error { return nil }
