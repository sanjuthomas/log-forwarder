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
