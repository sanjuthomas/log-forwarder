// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package sink

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/sanjuthomas/log-forwarder/internal/config"
)

func TestSchemaFilterDropsUnknownField(t *testing.T) {
	t.Parallel()

	filter := newSchemaFilter(bigquery.Schema{
		{Name: "level", Type: bigquery.StringFieldType},
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	out, err := filter.filterPayload([]byte(`{"level":"INFO","extra":"drop-me"}`))
	if err != nil {
		t.Fatalf("filterPayload() error = %v", err)
	}
	if strings.Contains(string(out), "extra") {
		t.Fatalf("filtered payload = %s, want extra field removed", out)
	}
	if !strings.Contains(string(out), `"level":"INFO"`) {
		t.Fatalf("filtered payload = %s, want level preserved", out)
	}
}

func TestSchemaFilterDropsTypeMismatch(t *testing.T) {
	t.Parallel()

	filter := newSchemaFilter(bigquery.Schema{
		{Name: "level", Type: bigquery.StringFieldType},
		{Name: "count", Type: bigquery.IntegerFieldType},
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	out, err := filter.filterPayload([]byte(`{"level":123,"count":5,"message":"ok"}`))
	if err != nil {
		t.Fatalf("filterPayload() error = %v", err)
	}
	if strings.Contains(string(out), `"level":123`) {
		t.Fatalf("filtered payload = %s, want invalid level removed", out)
	}
	if !strings.Contains(string(out), `"count":5`) {
		t.Fatalf("filtered payload = %s, want count preserved", out)
	}
}

func TestSchemaFilterWarnsOncePerField(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	filter := newSchemaFilter(bigquery.Schema{
		{Name: "level", Type: bigquery.StringFieldType},
	}, logger)

	for range 3 {
		if _, err := filter.filterPayload([]byte(`{"level":"INFO","extra":"one"}`)); err != nil {
			t.Fatalf("filterPayload() error = %v", err)
		}
	}

	warnCount := strings.Count(buf.String(), "extra")
	if warnCount != 1 {
		t.Fatalf("warn log count for field = %d, want 1", warnCount)
	}
}

func TestSchemaFilterNestedRecord(t *testing.T) {
	t.Parallel()

	filter := newSchemaFilter(bigquery.Schema{
		{
			Name: "context",
			Type: bigquery.RecordFieldType,
			Schema: bigquery.Schema{
				{Name: "service", Type: bigquery.StringFieldType},
			},
		},
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	out, err := filter.filterPayload([]byte(`{"context":{"service":"api","host":"drop-me"}}`))
	if err != nil {
		t.Fatalf("filterPayload() error = %v", err)
	}
	if strings.Contains(string(out), "host") {
		t.Fatalf("filtered payload = %s, want nested host removed", out)
	}
	if !strings.Contains(string(out), `"service":"api"`) {
		t.Fatalf("filtered payload = %s, want nested service preserved", out)
	}
}

func TestSchemaFilterFillsRequiredScalarDefaults(t *testing.T) {
	t.Parallel()

	filter := newSchemaFilter(bigquery.Schema{
		{Name: "level", Type: bigquery.StringFieldType, Required: true},
		{Name: "count", Type: bigquery.IntegerFieldType, Required: true},
		{Name: "active", Type: bigquery.BooleanFieldType, Required: true},
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	out, err := filter.filterPayload([]byte(`{"message":"extra"}`))
	if err != nil {
		t.Fatalf("filterPayload() error = %v", err)
	}
	for _, want := range []string{`"level":""`, `"count":0`, `"active":false`} {
		if !strings.Contains(string(out), want) {
			t.Fatalf("filtered payload = %s, want substring %q", out, want)
		}
	}
	if strings.Contains(string(out), "message") {
		t.Fatalf("filtered payload = %s, want unknown field removed", out)
	}
}

func TestSchemaFilterFillsRequiredDefaultsAfterTypeMismatch(t *testing.T) {
	t.Parallel()

	filter := newSchemaFilter(bigquery.Schema{
		{Name: "level", Type: bigquery.StringFieldType, Required: true},
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	out, err := filter.filterPayload([]byte(`{"level":123}`))
	if err != nil {
		t.Fatalf("filterPayload() error = %v", err)
	}
	if string(out) != `{"level":""}` {
		t.Fatalf("filtered payload = %s, want required string default", out)
	}
}

func TestSchemaFilterFillsRequiredNestedRecordDefaults(t *testing.T) {
	t.Parallel()

	filter := newSchemaFilter(bigquery.Schema{
		{
			Name:     "context",
			Type:     bigquery.RecordFieldType,
			Required: true,
			Schema: bigquery.Schema{
				{Name: "service", Type: bigquery.StringFieldType, Required: true},
			},
		},
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	out, err := filter.filterPayload([]byte(`{}`))
	if err != nil {
		t.Fatalf("filterPayload() error = %v", err)
	}
	if !strings.Contains(string(out), `"context":{"service":""}`) {
		t.Fatalf("filtered payload = %s, want nested required default", out)
	}
}

func TestSchemaFilterFillsRequiredRepeatedDefault(t *testing.T) {
	t.Parallel()

	filter := newSchemaFilter(bigquery.Schema{
		{Name: "tags", Type: bigquery.StringFieldType, Required: true, Repeated: true},
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	out, err := filter.filterPayload([]byte(`{}`))
	if err != nil {
		t.Fatalf("filterPayload() error = %v", err)
	}
	if string(out) != `{"tags":[]}` {
		t.Fatalf("filtered payload = %s, want empty repeated default", out)
	}
}

func TestEncodeJSONRowsAfterFillingRequiredDefaults(t *testing.T) {
	t.Parallel()

	schema := bigquery.Schema{
		{Name: "level", Type: bigquery.StringFieldType, Required: true},
	}
	msgDesc, _, err := descriptorsFromSchema(schema)
	if err != nil {
		t.Fatal(err)
	}

	filter := newSchemaFilter(schema, slog.New(slog.NewTextHandler(io.Discard, nil)))
	filtered, err := filter.filterPayload([]byte(`{"message":"extra only"}`))
	if err != nil {
		t.Fatal(err)
	}

	encoded, err := encodeJSONRows(msgDesc, [][]byte{filtered})
	if err != nil {
		t.Fatalf("encodeJSONRows() error = %v", err)
	}
	if len(encoded) != 1 || len(encoded[0]) == 0 {
		t.Fatal("expected encoded row")
	}
}

func TestSchemaFilterAllowsPublishAfterStrippingFields(t *testing.T) {
	t.Parallel()

	schema := bigquery.Schema{
		{Name: "level", Type: bigquery.StringFieldType},
	}
	msgDesc, _, err := descriptorsFromSchema(schema)
	if err != nil {
		t.Fatal(err)
	}

	filter := newSchemaFilter(schema, slog.New(slog.NewTextHandler(io.Discard, nil)))
	filtered, err := filter.filterPayload([]byte(`{"level":"INFO","message":"extra"}`))
	if err != nil {
		t.Fatal(err)
	}

	encoded, err := encodeJSONRows(msgDesc, [][]byte{filtered})
	if err != nil {
		t.Fatalf("encodeJSONRows() error = %v", err)
	}
	if len(encoded) != 1 || len(encoded[0]) == 0 {
		t.Fatal("expected encoded row")
	}
}

func TestBigQuerySinkPublishBatchStripsUnknownFields(t *testing.T) {
	t.Parallel()

	schema := bigquery.Schema{
		{Name: "level", Type: bigquery.StringFieldType},
		{Name: "message", Type: bigquery.StringFieldType},
	}
	msgDesc, _, err := descriptorsFromSchema(schema)
	if err != nil {
		t.Fatal(err)
	}

	stream := &recordingManagedStream{}
	s := &bigQuerySink{
		cfg:          config.BigQueryConfig{ProjectID: "proj", Dataset: "logs", Table: "events"},
		msgDesc:      msgDesc,
		schemaFilter: newSchemaFilter(schema, slog.New(slog.NewTextHandler(io.Discard, nil))),
		stream:       stream,
	}

	if err := s.PublishBatch(context.Background(), [][]byte{
		[]byte(`{"level":"INFO","message":"one","_path":"/tmp/app.log"}`),
	}); err != nil {
		t.Fatalf("PublishBatch() error = %v", err)
	}
	if len(stream.lastRows) != 1 {
		t.Fatalf("AppendRows rows = %d, want 1", len(stream.lastRows))
	}
}

func TestSchemaFilterCoversScalarTypes(t *testing.T) {
	t.Parallel()

	filter := newSchemaFilter(bigquery.Schema{
		{Name: "active", Type: bigquery.BooleanFieldType},
		{Name: "ratio", Type: bigquery.FloatFieldType},
		{Name: "seen_at", Type: bigquery.TimestampFieldType},
		{Name: "payload", Type: bigquery.JSONFieldType},
		{Name: "tags", Type: bigquery.StringFieldType, Repeated: true},
	}, nil)

	out, err := filter.filterPayload([]byte(`{
		"active": true,
		"ratio": 1.5,
		"seen_at": "2024-01-01T00:00:00Z",
		"payload": {"k":"v"},
		"tags": ["a","b"],
		"extra": "drop"
	}`))
	if err != nil {
		t.Fatalf("filterPayload() error = %v", err)
	}
	for _, want := range []string{`"active":true`, `"ratio":1.5`, `"seen_at"`, `"payload"`, `"tags":["a","b"]`} {
		if !strings.Contains(string(out), want) {
			t.Fatalf("filtered payload = %s, want substring %q", out, want)
		}
	}
}

func TestSchemaFilterDropsInvalidScalarTypes(t *testing.T) {
	t.Parallel()

	filter := newSchemaFilter(bigquery.Schema{
		{Name: "active", Type: bigquery.BooleanFieldType},
		{Name: "count", Type: bigquery.IntegerFieldType},
		{Name: "seen_at", Type: bigquery.TimestampFieldType},
		{Name: "amount", Type: bigquery.NumericFieldType},
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	out, err := filter.filterPayload([]byte(`{
		"active": "yes",
		"count": 1.5,
		"seen_at": 123,
		"amount": 99
	}`))
	if err != nil {
		t.Fatalf("filterPayload() error = %v", err)
	}
	if string(out) != "{}" {
		t.Fatalf("filtered payload = %s, want {}", out)
	}
}

func TestSchemaFilterDropsInvalidRepeatedElement(t *testing.T) {
	t.Parallel()

	filter := newSchemaFilter(bigquery.Schema{
		{Name: "tags", Type: bigquery.StringFieldType, Repeated: true},
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	out, err := filter.filterPayload([]byte(`{"tags":["ok",123]}`))
	if err != nil {
		t.Fatalf("filterPayload() error = %v", err)
	}
	if !strings.Contains(string(out), `"tags":["ok"]`) {
		t.Fatalf("filtered payload = %s, want valid tag preserved", out)
	}
}

func TestSchemaFilterRejectsInvalidRepeatedContainer(t *testing.T) {
	t.Parallel()

	filter := newSchemaFilter(bigquery.Schema{
		{Name: "tags", Type: bigquery.StringFieldType, Repeated: true},
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	out, err := filter.filterPayload([]byte(`{"tags":"not-an-array"}`))
	if err != nil {
		t.Fatalf("filterPayload() error = %v", err)
	}
	if string(out) != "{}" {
		t.Fatalf("filtered payload = %s, want {}", out)
	}
}

func TestSchemaFilterEmptyPayload(t *testing.T) {
	t.Parallel()

	filter := newSchemaFilter(bigquery.Schema{
		{Name: "level", Type: bigquery.StringFieldType},
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	out, err := filter.filterPayload(nil)
	if err != nil {
		t.Fatalf("filterPayload(nil) error = %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("filterPayload(nil) = %q, want empty", out)
	}
}

func TestSchemaFilterUnsupportedFieldType(t *testing.T) {
	t.Parallel()

	filter := newSchemaFilter(bigquery.Schema{
		{Name: "window", Type: bigquery.RangeFieldType},
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	out, err := filter.filterPayload([]byte(`{"window":{"start":"2024-01-01","end":"2024-01-02"}}`))
	if err != nil {
		t.Fatalf("filterPayload() error = %v", err)
	}
	if string(out) != "{}" {
		t.Fatalf("filtered payload = %s, want {}", out)
	}
}
