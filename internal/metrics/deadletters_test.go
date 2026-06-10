package metrics

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sanjuthomas/log-forwarder/internal/deadletter"
)

func TestDeadLettersHandlerReturnsMetadataOnly(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	_, _, err := deadletter.WriteBatch(dir, [][]byte{[]byte(`{"message":"do-not-expose"}`)}, deadletter.WriteInfo{
		FailureReason: "sink error",
		SinkType:      "kafka",
		BatchAttempts: 3,
	})
	if err != nil {
		t.Fatalf("WriteBatch() error = %v", err)
	}

	collector, shutdown, err := New(configFromTest(), Snapshot{}, nil, &DeadLetters{Dir: dir})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() { _ = shutdown(context.Background()) })

	server := httptest.NewServer(collector.server.Handler)
	t.Cleanup(server.Close)

	resp, err := http.Get(server.URL + "/deadletters")
	if err != nil {
		t.Fatalf("GET /deadletters error = %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(body), "do-not-expose") {
		t.Fatalf("response must not include record bodies: %s", body)
	}

	var entries []deadletter.Entry
	if err := json.Unmarshal(body, &entries); err != nil {
		t.Fatalf("Unmarshal() error = %v, body = %s", err, body)
	}
	if len(entries) != 1 {
		t.Fatalf("len(entries) = %d, want 1", len(entries))
	}
	if entries[0].SinkType != "kafka" || entries[0].EventCount != 1 {
		t.Fatalf("entry = %+v", entries[0])
	}
}

func TestDeadLettersHandlerNotRegisteredWhenDisabled(t *testing.T) {
	t.Parallel()

	collector, shutdown, err := New(configFromTest(), Snapshot{}, nil, nil)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() { _ = shutdown(context.Background()) })

	server := httptest.NewServer(collector.server.Handler)
	t.Cleanup(server.Close)

	resp, err := http.Get(server.URL + "/deadletters")
	if err != nil {
		t.Fatalf("GET /deadletters error = %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusNotFound)
	}
}
