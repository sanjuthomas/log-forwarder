package sink

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sanjuthomas/log-forwarder/internal/config"
)

func TestHTTPNoauthSinkPublish(t *testing.T) {
	t.Parallel()

	var gotMethod string
	var gotBody []byte
	var gotAuth string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotBody, _ = io.ReadAll(r.Body)
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusAccepted)
	}))
	t.Cleanup(server.Close)

	s, err := New(config.SinkConfig{
		Type: "http-noauth",
		HTTPNoauth: &config.HTTPNoauthSinkConfig{
			URL:    server.URL + "/ingest",
			Method: "POST",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	payload := []byte(`{"message":"hello"}`)
	if err := s.Publish(context.Background(), payload); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if gotMethod != http.MethodPost {
		t.Fatalf("method = %q, want POST", gotMethod)
	}
	if string(gotBody) != string(payload) {
		t.Fatalf("body = %q, want %q", gotBody, payload)
	}
	if gotAuth != "" {
		t.Fatalf("Authorization header = %q, want empty", gotAuth)
	}
}

func TestHTTPNoauthSinkPublishErrorStatus(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("boom"))
	}))
	t.Cleanup(server.Close)

	s, err := New(config.SinkConfig{
		Type:       "http-noauth",
		HTTPNoauth: &config.HTTPNoauthSinkConfig{URL: server.URL},
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := s.Publish(context.Background(), []byte(`{}`)); err == nil {
		t.Fatal("expected error for non-2xx response")
	}
}

func TestHTTPNoauthSinkCheck(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)

	s, err := New(config.SinkConfig{
		Type:       "http-noauth",
		HTTPNoauth: &config.HTTPNoauthSinkConfig{URL: server.URL},
	})
	if err != nil {
		t.Fatal(err)
	}

	checker, ok := s.(Checker)
	if !ok {
		t.Fatal("expected sink to implement Checker")
	}
	if err := checker.Check(context.Background()); err != nil {
		t.Fatalf("Check() error = %v", err)
	}
}

func TestHTTPNoauthSinkPublishBatch(t *testing.T) {
	t.Parallel()

	var requestCount int
	var gotContentType string
	var gotBody []byte

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		gotContentType = r.Header.Get("Content-Type")
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusAccepted)
	}))
	t.Cleanup(server.Close)

	s, err := New(config.SinkConfig{
		Type:       "http-noauth",
		HTTPNoauth: &config.HTTPNoauthSinkConfig{URL: server.URL},
	})
	if err != nil {
		t.Fatal(err)
	}

	batchSink, ok := s.(BatchSink)
	if !ok {
		t.Fatal("expected sink to implement BatchSink")
	}

	payloads := [][]byte{
		[]byte(`{"line":1}`),
		[]byte(`{"line":2}`),
		[]byte(`{"line":3}`),
	}
	if err := batchSink.PublishBatch(context.Background(), payloads); err != nil {
		t.Fatalf("PublishBatch() error = %v", err)
	}
	if requestCount != 1 {
		t.Fatalf("request count = %d, want 1", requestCount)
	}
	if gotContentType != "application/x-ndjson" {
		t.Fatalf("Content-Type = %q, want application/x-ndjson", gotContentType)
	}
	wantBody := "{\"line\":1}\n{\"line\":2}\n{\"line\":3}"
	if string(gotBody) != wantBody {
		t.Fatalf("body = %q, want %q", gotBody, wantBody)
	}
}

func TestHTTPNoauthSinkPublishBatchSingleRecordUsesJSONContentType(t *testing.T) {
	t.Parallel()

	var gotContentType string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotContentType = r.Header.Get("Content-Type")
		w.WriteHeader(http.StatusAccepted)
	}))
	t.Cleanup(server.Close)

	s, err := New(config.SinkConfig{
		Type:       "http-noauth",
		HTTPNoauth: &config.HTTPNoauthSinkConfig{URL: server.URL},
	})
	if err != nil {
		t.Fatal(err)
	}

	batchSink := s.(BatchSink)
	if err := batchSink.PublishBatch(context.Background(), [][]byte{[]byte(`{"line":1}`)}); err != nil {
		t.Fatalf("PublishBatch() error = %v", err)
	}
	if gotContentType != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", gotContentType)
	}
}

func TestHTTPNoauthSinkPublishBatchRetryDoesNotDuplicateWithinBatch(t *testing.T) {
	t.Parallel()

	var attempts int
	var bodies [][]byte

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		body, _ := io.ReadAll(r.Body)
		bodies = append(bodies, append([]byte(nil), body...))
		if attempts == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	t.Cleanup(server.Close)

	s, err := New(config.SinkConfig{
		Type:       "http-noauth",
		HTTPNoauth: &config.HTTPNoauthSinkConfig{URL: server.URL},
	})
	if err != nil {
		t.Fatal(err)
	}

	batchSink := s.(BatchSink)
	payloads := [][]byte{
		[]byte(`{"line":1}`),
		[]byte(`{"line":2}`),
		[]byte(`{"line":3}`),
	}
	wantBody := []byte("{\"line\":1}\n{\"line\":2}\n{\"line\":3}")

	if err := batchSink.PublishBatch(context.Background(), payloads); err == nil {
		t.Fatal("expected first PublishBatch attempt to fail")
	}
	if err := batchSink.PublishBatch(context.Background(), payloads); err != nil {
		t.Fatalf("PublishBatch() retry error = %v", err)
	}
	if attempts != 2 {
		t.Fatalf("attempts = %d, want 2", attempts)
	}
	for i, body := range bodies {
		if string(body) != string(wantBody) {
			t.Fatalf("attempt %d body = %q, want %q", i+1, body, wantBody)
		}
	}
}

func TestHTTPNoauthSinkCheckErrorStatus(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("boom"))
	}))
	t.Cleanup(server.Close)

	s, err := New(config.SinkConfig{
		Type:       "http-noauth",
		HTTPNoauth: &config.HTTPNoauthSinkConfig{URL: server.URL},
	})
	if err != nil {
		t.Fatal(err)
	}

	checker, ok := s.(Checker)
	if !ok {
		t.Fatal("expected sink to implement Checker")
	}
	if err := checker.Check(context.Background()); err == nil {
		t.Fatal("expected error for non-2xx response")
	}
}
