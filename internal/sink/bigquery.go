// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package sink

import (
	"context"
	"fmt"
	"log/slog"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"github.com/sanjuthomas/log-forwarder/internal/config"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type bigQueryTableMetadataReader interface {
	Metadata(ctx context.Context, opts ...bigquery.TableMetadataOption) (*bigquery.TableMetadata, error)
}

type bigQueryMetadataClient interface {
	Table(projectID, datasetID, tableID string) bigQueryTableMetadataReader
	Close() error
}

type bigQueryAppendResult interface {
	GetResult(ctx context.Context) (int64, error)
}

type bigQueryManagedStream interface {
	AppendRows(ctx context.Context, data [][]byte, opts ...managedwriter.AppendOption) (bigQueryAppendResult, error)
	Close() error
}

type bigQueryManagedWriterClient interface {
	NewManagedStream(ctx context.Context, opts ...managedwriter.WriterOption) (bigQueryManagedStream, error)
	Close() error
}

type bigQuerySink struct {
	cfg          config.BigQueryConfig
	msgDesc      protoreflect.MessageDescriptor
	schemaFilter *schemaFilter
	stream       bigQueryManagedStream
	bq           bigQueryMetadataClient
	mw           bigQueryManagedWriterClient
}

func newBigQuerySink(cfg config.SinkConfig) (Sink, error) {
	if cfg.BigQuery == nil {
		return nil, fmt.Errorf("sink.bigquery is required")
	}
	bqCfg := *cfg.BigQuery
	if err := validateCredentialsFile(bqCfg.CredentialsFile); err != nil {
		return nil, err
	}

	ctx := context.Background()
	clientOpts, err := bigQueryClientOptions(bqCfg)
	if err != nil {
		return nil, err
	}

	bqClient, err := bigquery.NewClient(ctx, bqCfg.ProjectID, clientOpts...)
	if err != nil {
		return nil, fmt.Errorf("bigquery client: %w", err)
	}

	mwClient, err := managedwriter.NewClient(ctx, bqCfg.ProjectID, clientOpts...)
	if err != nil {
		_ = bqClient.Close()
		return nil, fmt.Errorf("bigquery storage write client: %w", err)
	}

	s := &bigQuerySink{
		cfg: bqCfg,
		bq:  &bigQueryClientAdapter{client: bqClient},
		mw:  &managedWriterClientAdapter{client: mwClient},
	}
	if err := s.initStream(ctx); err != nil {
		_ = s.Close()
		return nil, err
	}
	return s, nil
}

func (b *bigQuerySink) initStream(ctx context.Context) error {
	md, err := b.tableMetadata(ctx)
	if err != nil {
		return err
	}
	if len(md.Schema) == 0 {
		return fmt.Errorf("bigquery table %s.%s.%s has no schema", b.cfg.ProjectID, b.cfg.Dataset, b.cfg.Table)
	}

	msgDesc, descriptorProto, err := descriptorsFromSchema(md.Schema)
	if err != nil {
		return fmt.Errorf("bigquery table schema: %w", err)
	}
	b.msgDesc = msgDesc
	b.schemaFilter = newSchemaFilter(md.Schema, slog.Default())

	streamOpts := []managedwriter.WriterOption{
		managedwriter.WithDestinationTable(
			managedwriter.TableParentFromParts(b.cfg.ProjectID, b.cfg.Dataset, b.cfg.Table),
		),
		managedwriter.WithType(managedwriter.DefaultStream),
		managedwriter.WithSchemaDescriptor(descriptorProto),
	}
	if b.cfg.WriteRetriesEnabled() {
		streamOpts = append(streamOpts, managedwriter.EnableWriteRetries(true))
	}

	stream, err := b.mw.NewManagedStream(ctx, streamOpts...)
	if err != nil {
		return fmt.Errorf("bigquery managed stream: %w", err)
	}
	b.stream = stream
	return nil
}

func (b *bigQuerySink) tableMetadata(ctx context.Context) (*bigquery.TableMetadata, error) {
	table := b.bq.Table(b.cfg.ProjectID, b.cfg.Dataset, b.cfg.Table)
	md, err := table.Metadata(ctx)
	if err != nil {
		return nil, fmt.Errorf("bigquery table %s.%s.%s metadata: %w", b.cfg.ProjectID, b.cfg.Dataset, b.cfg.Table, err)
	}
	return md, nil
}

func (b *bigQuerySink) Check(ctx context.Context) error {
	_, err := b.tableMetadata(ctx)
	return err
}

func (b *bigQuerySink) Publish(ctx context.Context, payload []byte) error {
	return b.PublishBatch(ctx, [][]byte{payload})
}

func (b *bigQuerySink) PublishBatch(ctx context.Context, payloads [][]byte) error {
	if len(payloads) == 0 {
		return nil
	}

	filtered := make([][]byte, len(payloads))
	for i, payload := range payloads {
		if b.schemaFilter != nil {
			sanitized, err := b.schemaFilter.filterPayload(payload)
			if err != nil {
				return fmt.Errorf("bigquery publish row %d: %w", i, err)
			}
			payload = sanitized
		}
		filtered[i] = payload
	}

	encoded, err := encodeJSONRows(b.msgDesc, filtered)
	if err != nil {
		return fmt.Errorf("bigquery publish: %w", err)
	}

	result, err := b.stream.AppendRows(ctx, encoded)
	if err != nil {
		return fmt.Errorf("bigquery append rows: %w", err)
	}
	if _, err := result.GetResult(ctx); err != nil {
		return fmt.Errorf("bigquery publish: %w", err)
	}
	return nil
}

func (b *bigQuerySink) Close() error {
	var firstErr error
	if b.stream != nil {
		if err := b.stream.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if b.mw != nil {
		if err := b.mw.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if b.bq != nil {
		if err := b.bq.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

type bigQueryClientAdapter struct {
	client *bigquery.Client
}

func (a *bigQueryClientAdapter) Table(projectID, datasetID, tableID string) bigQueryTableMetadataReader {
	return a.client.DatasetInProject(projectID, datasetID).Table(tableID)
}

func (a *bigQueryClientAdapter) Close() error {
	return a.client.Close()
}

type managedWriterClientAdapter struct {
	client *managedwriter.Client
}

func (a *managedWriterClientAdapter) NewManagedStream(
	ctx context.Context,
	opts ...managedwriter.WriterOption,
) (bigQueryManagedStream, error) {
	stream, err := a.client.NewManagedStream(ctx, opts...)
	if err != nil {
		return nil, err
	}
	return &managedStreamAdapter{stream: stream}, nil
}

func (a *managedWriterClientAdapter) Close() error {
	return a.client.Close()
}

type managedStreamAdapter struct {
	stream *managedwriter.ManagedStream
}

func (a *managedStreamAdapter) AppendRows(
	ctx context.Context,
	data [][]byte,
	opts ...managedwriter.AppendOption,
) (bigQueryAppendResult, error) {
	return a.stream.AppendRows(ctx, data, opts...)
}

func (a *managedStreamAdapter) Close() error {
	return a.stream.Close()
}

func init() {
	Register("bigquery", newBigQuerySink)
}
