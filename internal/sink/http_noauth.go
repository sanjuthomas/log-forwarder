// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package sink

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/sanjuthomas/log-forwarder/internal/config"
)

// httpNoauthSink posts JSON records to an open HTTP endpoint with no authentication.
// Use a separate sink (for example http-oauth2) when the destination requires credentials.
type httpNoauthSink struct {
	client *http.Client
	url    string
	method string
}

func newHTTPNoauthSink(cfg config.SinkConfig) (Sink, error) {
	if cfg.HTTPNoauth == nil {
		return nil, fmt.Errorf("sink.http_noauth is required")
	}
	httpCfg := cfg.HTTPNoauth
	return &httpNoauthSink{
		client: &http.Client{
			Timeout: httpCfg.TimeoutDuration(),
		},
		url:    httpCfg.URL,
		method: httpCfg.MethodOrDefault(),
	}, nil
}

func (h *httpNoauthSink) Check(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, h.method, h.url, nil)
	if err != nil {
		return fmt.Errorf("http check request: %w", err)
	}

	resp, err := h.client.Do(req)
	if err != nil {
		return fmt.Errorf("http check: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("http check: status %d: %s", resp.StatusCode, bytes.TrimSpace(body))
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

func (h *httpNoauthSink) Publish(ctx context.Context, payload []byte) error {
	return h.PublishBatch(ctx, [][]byte{payload})
}

func (h *httpNoauthSink) PublishBatch(ctx context.Context, payloads [][]byte) error {
	if len(payloads) == 0 {
		return nil
	}

	body, contentType := encodeHTTPBatchPayloads(payloads)
	return h.post(ctx, body, contentType)
}

func encodeHTTPBatchPayloads(payloads [][]byte) ([]byte, string) {
	if len(payloads) == 1 {
		return payloads[0], "application/json"
	}

	var body bytes.Buffer
	for i, payload := range payloads {
		if i > 0 {
			body.WriteByte('\n')
		}
		body.Write(payload)
	}
	return body.Bytes(), "application/x-ndjson"
}

func (h *httpNoauthSink) post(ctx context.Context, body []byte, contentType string) error {
	req, err := http.NewRequestWithContext(ctx, h.method, h.url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("http publish request: %w", err)
	}
	req.Header.Set("Content-Type", contentType)

	resp, err := h.client.Do(req)
	if err != nil {
		return fmt.Errorf("http publish: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("http publish: status %d: %s", resp.StatusCode, bytes.TrimSpace(respBody))
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

func (h *httpNoauthSink) Close() error {
	return nil
}

func init() {
	Register("http-noauth", newHTTPNoauthSink)
}
