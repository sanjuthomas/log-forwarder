// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package sink

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"github.com/sanjuthomas/log-forwarder/internal/config"
)

func TestBigQueryClientOptionsUsesCredentialsFile(t *testing.T) {
	t.Parallel()

	opts, err := bigQueryClientOptions(config.BigQueryConfig{})
	if err != nil {
		t.Fatalf("bigQueryClientOptions() error = %v", err)
	}
	if len(opts) != 0 {
		t.Fatalf("expected no options for ADC, got %d", len(opts))
	}

	path := filepath.Join(t.TempDir(), "wif.json")
	if err := os.WriteFile(path, []byte(`{
		"type": "external_account",
		"audience": "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/pool/providers/provider",
		"subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
		"token_url": "https://sts.googleapis.com/v1/token",
		"credential_source": {
			"file": "/var/run/secrets/token"
		},
		"service_account_impersonation_url": "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/sa@proj.iam.gserviceaccount.com:generateAccessToken"
	}`), 0o600); err != nil {
		t.Fatal(err)
	}

	opts, err = bigQueryClientOptions(config.BigQueryConfig{CredentialsFile: path})
	if err != nil {
		t.Fatalf("bigQueryClientOptions() error = %v", err)
	}
	if len(opts) != 1 {
		t.Fatalf("expected one option, got %d", len(opts))
	}
}

func TestValidateCredentialsFileAllowsEmptyPath(t *testing.T) {
	t.Parallel()

	if err := validateCredentialsFile(""); err != nil {
		t.Fatalf("validateCredentialsFile(\"\") error = %v", err)
	}
}

func TestBigQuerySinkCloseSucceeds(t *testing.T) {
	t.Parallel()

	s := &bigQuerySink{
		stream: &fakeManagedStream{},
		mw:     &fakeManagedWriterClient{},
		bq:     fakeMetadataClient{},
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestBigQuerySinkPublishBatchPropagatesEncodeError(t *testing.T) {
	t.Parallel()

	schema := bigquery.Schema{{Name: "level", Type: bigquery.StringFieldType, Required: true}}
	msgDesc, _, err := descriptorsFromSchema(schema)
	if err != nil {
		t.Fatal(err)
	}

	s := &bigQuerySink{
		msgDesc: msgDesc,
		stream:  &recordingManagedStream{},
	}
	err = s.PublishBatch(context.Background(), [][]byte{[]byte(`not-json`)})
	if err == nil {
		t.Fatal("expected error for invalid JSON payload")
	}
}

func TestValidateCredentialsFileRejectsMissingPath(t *testing.T) {
	t.Parallel()

	if err := validateCredentialsFile(filepath.Join(t.TempDir(), "missing.json")); err == nil {
		t.Fatal("expected error for missing credentials file")
	}
}

func TestValidateCredentialsFileRejectsDirectory(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	if err := validateCredentialsFile(dir); err == nil {
		t.Fatal("expected error when credentials_file is a directory")
	}
}

func TestBigQueryClientOptionsRejectsInvalidCredentialsFile(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "bad.json")
	if err := os.WriteFile(path, []byte(`not-json`), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := bigQueryClientOptions(config.BigQueryConfig{CredentialsFile: path})
	if err == nil {
		t.Fatal("expected error for invalid credentials file")
	}
}

func TestEncodeJSONRowsMatchesTableSchema(t *testing.T) {
	t.Parallel()

	schema := bigquery.Schema{
		{Name: "level", Type: bigquery.StringFieldType},
		{Name: "message", Type: bigquery.StringFieldType},
	}
	msgDesc, _, err := descriptorsFromSchema(schema)
	if err != nil {
		t.Fatalf("descriptorsFromSchema() error = %v", err)
	}

	encoded, err := encodeJSONRows(msgDesc, [][]byte{
		[]byte(`{"level":"INFO","message":"started"}`),
		[]byte(`{"level":"ERROR","message":"failed"}`),
	})
	if err != nil {
		t.Fatalf("encodeJSONRows() error = %v", err)
	}
	if len(encoded) != 2 {
		t.Fatalf("encoded rows = %d, want 2", len(encoded))
	}
	for i, row := range encoded {
		if len(row) == 0 {
			t.Fatalf("encoded row %d is empty", i)
		}
	}
}

func TestEncodeJSONRowsEmptyPayload(t *testing.T) {
	t.Parallel()

	schema := bigquery.Schema{{Name: "level", Type: bigquery.StringFieldType}}
	msgDesc, _, err := descriptorsFromSchema(schema)
	if err != nil {
		t.Fatal(err)
	}

	encoded, err := encodeJSONRows(msgDesc, nil)
	if err != nil {
		t.Fatalf("encodeJSONRows(nil) error = %v", err)
	}
	if encoded != nil {
		t.Fatalf("encodeJSONRows(nil) = %v, want nil", encoded)
	}
}

func TestBigQuerySinkInitStreamSuccess(t *testing.T) {
	t.Parallel()

	schema := bigquery.Schema{{Name: "level", Type: bigquery.StringFieldType}}
	s := &bigQuerySink{
		cfg: config.BigQueryConfig{ProjectID: "proj", Dataset: "logs", Table: "events"},
		bq: fakeMetadataClient{table: fakeMetadataTable{
			schema: schema,
		}},
		mw: &fakeManagedWriterClient{},
	}
	if err := s.initStream(context.Background()); err != nil {
		t.Fatalf("initStream() error = %v", err)
	}
	if s.msgDesc == nil || s.stream == nil {
		t.Fatal("expected message descriptor and stream to be initialized")
	}
}

func TestNewBigQuerySinkFailsWithoutGCPAccess(t *testing.T) {
	t.Parallel()

	_, err := New(config.SinkConfig{
		Type: "bigquery",
		BigQuery: &config.BigQueryConfig{
			ProjectID: "log-forwarder-unit-test-project",
			Dataset:   "logs",
			Table:     "events",
		},
	})
	if err == nil {
		t.Fatal("expected error when BigQuery client cannot reach GCP")
	}
}

func TestEncodeJSONRowsAfterDroppingUnknownField(t *testing.T) {
	t.Parallel()

	schema := bigquery.Schema{
		{Name: "level", Type: bigquery.StringFieldType, Required: true},
	}
	msgDesc, _, err := descriptorsFromSchema(schema)
	if err != nil {
		t.Fatalf("descriptorsFromSchema() error = %v", err)
	}

	filter := newSchemaFilter(schema, slog.New(slog.NewTextHandler(io.Discard, nil)))
	filtered, err := filter.filterPayload([]byte(`{"level":"INFO","message":"extra"}`))
	if err != nil {
		t.Fatalf("filterPayload() error = %v", err)
	}

	encoded, err := encodeJSONRows(msgDesc, [][]byte{filtered})
	if err != nil {
		t.Fatalf("encodeJSONRows() error = %v", err)
	}
	if len(encoded) != 1 {
		t.Fatalf("encoded rows = %d, want 1", len(encoded))
	}
}

func TestNewBigQuerySinkRequiresConfigBlock(t *testing.T) {
	t.Parallel()

	_, err := New(config.SinkConfig{Type: "bigquery"})
	if err == nil {
		t.Fatal("expected error when sink.bigquery block missing")
	}
}

func TestNewBigQuerySinkRejectsMissingCredentialsFile(t *testing.T) {
	t.Parallel()

	_, err := New(config.SinkConfig{
		Type: "bigquery",
		BigQuery: &config.BigQueryConfig{
			ProjectID:       "proj",
			Dataset:         "logs",
			Table:           "events",
			CredentialsFile: filepath.Join(t.TempDir(), "missing.json"),
		},
	})
	if err == nil {
		t.Fatal("expected error when credentials file is missing")
	}
}

type fakeAppendResult struct{}

func (fakeAppendResult) GetResult(context.Context) (int64, error) {
	return 0, nil
}

type errorAppendResult struct {
	err error
}

func (e errorAppendResult) GetResult(context.Context) (int64, error) {
	return 0, e.err
}

type fakeManagedStream struct {
	appendErr error
	resultErr error
	lastRows  [][]byte
}

func (f *fakeManagedStream) AppendRows(_ context.Context, data [][]byte, _ ...managedwriter.AppendOption) (bigQueryAppendResult, error) {
	f.lastRows = data
	if f.appendErr != nil {
		return nil, f.appendErr
	}
	if f.resultErr != nil {
		return errorAppendResult{err: f.resultErr}, nil
	}
	return fakeAppendResult{}, nil
}

func (f *fakeManagedStream) Close() error { return nil }

type recordingManagedStream struct {
	lastRows [][]byte
}

func (r *recordingManagedStream) AppendRows(_ context.Context, data [][]byte, _ ...managedwriter.AppendOption) (bigQueryAppendResult, error) {
	r.lastRows = data
	return fakeAppendResult{}, nil
}

func (r *recordingManagedStream) Close() error { return nil }

type fakeMetadataTable struct {
	schema bigquery.Schema
	err    error
}

func (f fakeMetadataTable) Metadata(ctx context.Context, _ ...bigquery.TableMetadataOption) (*bigquery.TableMetadata, error) {
	if f.err != nil {
		return nil, f.err
	}
	return &bigquery.TableMetadata{Schema: f.schema}, nil
}

type fakeMetadataClient struct {
	table fakeMetadataTable
	err   error
}

func (f fakeMetadataClient) Table(_, _, _ string) bigQueryTableMetadataReader {
	return f.table
}

func (f fakeMetadataClient) Close() error {
	return f.err
}

type fakeManagedWriterClient struct {
	stream   *fakeManagedStream
	err      error
	closeErr error
}

func (f *fakeManagedWriterClient) NewManagedStream(context.Context, ...managedwriter.WriterOption) (bigQueryManagedStream, error) {
	if f.err != nil {
		return nil, f.err
	}
	if f.stream == nil {
		f.stream = &fakeManagedStream{}
	}
	return f.stream, nil
}

func (f *fakeManagedWriterClient) Close() error {
	return f.closeErr
}

type closeErrStream struct {
	fakeManagedStream
}

func (closeErrStream) Close() error {
	return errors.New("stream close failed")
}

func TestBigQuerySinkPublishBatch(t *testing.T) {
	t.Parallel()

	schema := bigquery.Schema{
		{Name: "level", Type: bigquery.StringFieldType},
		{Name: "message", Type: bigquery.StringFieldType},
	}
	msgDesc, _, err := descriptorsFromSchema(schema)
	if err != nil {
		t.Fatalf("descriptorsFromSchema() error = %v", err)
	}

	stream := &recordingManagedStream{}
	s := &bigQuerySink{
		cfg: config.BigQueryConfig{
			ProjectID: "proj",
			Dataset:   "logs",
			Table:     "events",
		},
		msgDesc: msgDesc,
		stream:  stream,
		bq: fakeMetadataClient{table: fakeMetadataTable{
			schema: schema,
		}},
	}

	if err := s.PublishBatch(context.Background(), [][]byte{
		[]byte(`{"level":"INFO","message":"one"}`),
	}); err != nil {
		t.Fatalf("PublishBatch() error = %v", err)
	}
	if len(stream.lastRows) != 1 {
		t.Fatalf("AppendRows rows = %d, want 1", len(stream.lastRows))
	}
}

func TestBigQuerySinkPublishBatchPropagatesAppendError(t *testing.T) {
	t.Parallel()

	schema := bigquery.Schema{{Name: "level", Type: bigquery.StringFieldType}}
	msgDesc, _, err := descriptorsFromSchema(schema)
	if err != nil {
		t.Fatal(err)
	}

	wantErr := errors.New("append failed")
	s := &bigQuerySink{
		cfg:     config.BigQueryConfig{ProjectID: "proj", Dataset: "logs", Table: "events"},
		msgDesc: msgDesc,
		stream:  &fakeManagedStream{appendErr: wantErr},
	}

	err = s.PublishBatch(context.Background(), [][]byte{[]byte(`{"level":"INFO"}`)})
	if err == nil {
		t.Fatal("expected append error")
	}
}

func TestBigQuerySinkPublishBatchPropagatesResultError(t *testing.T) {
	t.Parallel()

	schema := bigquery.Schema{{Name: "level", Type: bigquery.StringFieldType}}
	msgDesc, _, err := descriptorsFromSchema(schema)
	if err != nil {
		t.Fatal(err)
	}

	wantErr := errors.New("result failed")
	s := &bigQuerySink{
		cfg:     config.BigQueryConfig{ProjectID: "proj", Dataset: "logs", Table: "events"},
		msgDesc: msgDesc,
		stream:  &fakeManagedStream{resultErr: wantErr},
	}

	err = s.PublishBatch(context.Background(), [][]byte{[]byte(`{"level":"INFO"}`)})
	if err == nil {
		t.Fatal("expected result error")
	}
}

func TestBigQuerySinkPublishDelegatesToBatch(t *testing.T) {
	t.Parallel()

	schema := bigquery.Schema{{Name: "level", Type: bigquery.StringFieldType}}
	msgDesc, _, err := descriptorsFromSchema(schema)
	if err != nil {
		t.Fatal(err)
	}

	stream := &recordingManagedStream{}
	s := &bigQuerySink{
		cfg:     config.BigQueryConfig{ProjectID: "proj", Dataset: "logs", Table: "events"},
		msgDesc: msgDesc,
		stream:  stream,
	}

	if err := s.Publish(context.Background(), []byte(`{"level":"INFO"}`)); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if len(stream.lastRows) != 1 {
		t.Fatalf("AppendRows rows = %d, want 1", len(stream.lastRows))
	}
}

func TestBigQuerySinkInitStreamRejectsEmptySchema(t *testing.T) {
	t.Parallel()

	s := &bigQuerySink{
		cfg: config.BigQueryConfig{ProjectID: "proj", Dataset: "logs", Table: "events"},
		bq: fakeMetadataClient{table: fakeMetadataTable{
			schema: bigquery.Schema{},
		}},
		mw: &fakeManagedWriterClient{},
	}
	if err := s.initStream(context.Background()); err == nil {
		t.Fatal("expected error for empty table schema")
	}
}

func TestBigQuerySinkInitStreamPropagatesManagedStreamError(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("stream setup failed")
	s := &bigQuerySink{
		cfg: config.BigQueryConfig{ProjectID: "proj", Dataset: "logs", Table: "events"},
		bq: fakeMetadataClient{table: fakeMetadataTable{
			schema: bigquery.Schema{{Name: "level", Type: bigquery.StringFieldType}},
		}},
		mw: &fakeManagedWriterClient{err: wantErr},
	}
	if err := s.initStream(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("initStream() error = %v, want %v", err, wantErr)
	}
}

func TestBigQuerySinkPublishBatchEmptyPayload(t *testing.T) {
	t.Parallel()

	s := &bigQuerySink{}
	if err := s.PublishBatch(context.Background(), nil); err != nil {
		t.Fatalf("PublishBatch(nil) error = %v", err)
	}
}

func TestBigQuerySinkCheckUsesMetadata(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("table missing")
	s := &bigQuerySink{
		cfg: config.BigQueryConfig{ProjectID: "proj", Dataset: "logs", Table: "events"},
		bq: fakeMetadataClient{table: fakeMetadataTable{
			err: wantErr,
		}},
	}
	if err := s.Check(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("Check() error = %v, want %v", err, wantErr)
	}
}

func TestBigQuerySinkCloseReturnsFirstError(t *testing.T) {
	t.Parallel()

	s := &bigQuerySink{
		stream: &closeErrStream{},
		mw:     &fakeManagedWriterClient{closeErr: errors.New("mw close failed")},
		bq:     fakeMetadataClient{err: errors.New("bq close failed")},
	}
	if err := s.Close(); err == nil {
		t.Fatal("expected close error")
	}
}

func TestBigQuerySinkInitStreamPropagatesMetadataError(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("metadata unavailable")
	s := &bigQuerySink{
		cfg: config.BigQueryConfig{ProjectID: "proj", Dataset: "logs", Table: "events"},
		bq: fakeMetadataClient{table: fakeMetadataTable{
			err: wantErr,
		}},
	}
	if err := s.initStream(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("initStream() error = %v, want %v", err, wantErr)
	}
}

func TestDescriptorsFromSchemaSupportsNestedRecords(t *testing.T) {
	t.Parallel()

	schema := bigquery.Schema{
		{Name: "level", Type: bigquery.StringFieldType},
		{
			Name: "context",
			Type: bigquery.RecordFieldType,
			Schema: bigquery.Schema{
				{Name: "service", Type: bigquery.StringFieldType},
			},
		},
	}
	msgDesc, descProto, err := descriptorsFromSchema(schema)
	if err != nil {
		t.Fatalf("descriptorsFromSchema() error = %v", err)
	}
	if msgDesc == nil || descProto == nil {
		t.Fatal("expected non-nil descriptors")
	}

	encoded, err := encodeJSONRows(msgDesc, [][]byte{
		[]byte(`{"level":"INFO","context":{"service":"api"}}`),
	})
	if err != nil {
		t.Fatalf("encodeJSONRows() error = %v", err)
	}
	if len(encoded) != 1 {
		t.Fatalf("encoded rows = %d, want 1", len(encoded))
	}
}

func TestDescriptorsFromSchemaSupportsJSONColumn(t *testing.T) {
	t.Parallel()

	schema := bigquery.Schema{{Name: "payload", Type: bigquery.JSONFieldType}}
	msgDesc, descProto, err := descriptorsFromSchema(schema)
	if err != nil {
		t.Fatalf("descriptorsFromSchema() error = %v", err)
	}
	if msgDesc == nil || descProto == nil {
		t.Fatal("expected non-nil descriptors")
	}
}
