// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package atc_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/sanjuthomas/log-forwarder/internal/atc"
	"github.com/sanjuthomas/log-forwarder/internal/config"
)

func TestNewClientDisabled(t *testing.T) {
	t.Parallel()

	if got := atc.NewClient(config.ATCConfig{}); got != nil {
		t.Fatal("expected nil client when ATC is disabled")
	}
}

func TestRegisterAndDeregister(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	var putBody, deleteBody atc.Instance

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/instances" {
			http.NotFound(w, r)
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		var inst atc.Instance
		if err := json.Unmarshal(body, &inst); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		mu.Lock()
		defer mu.Unlock()
		switch r.Method {
		case http.MethodPut:
			putBody = inst
		case http.MethodDelete:
			deleteBody = inst
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(server.Close)

	client := atc.NewClient(config.ATCConfig{
		Enabled: true,
		URL:     server.URL + "/api/instances",
		Timeout: "2s",
	})
	if client == nil {
		t.Fatal("expected non-nil client")
	}

	inst := atc.Instance{
		Hostname:  "host-a",
		Port:      8080,
		ProcessID: 4242,
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := client.Register(ctx, inst); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if err := client.Deregister(ctx, inst); err != nil {
		t.Fatalf("Deregister() error = %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if putBody != inst {
		t.Fatalf("PUT body = %+v, want %+v", putBody, inst)
	}
	if deleteBody != inst {
		t.Fatalf("DELETE body = %+v, want %+v", deleteBody, inst)
	}
}

func TestNewInstanceUsesMetricsPortDefault(t *testing.T) {
	t.Parallel()

	cfg := config.Default()
	cfg.Metrics.Port = 0

	inst := atc.NewInstance(cfg)
	if inst.Port != 8080 {
		t.Fatalf("Port = %d, want 8080", inst.Port)
	}
	if inst.ProcessID != os.Getpid() {
		t.Fatalf("ProcessID = %d, want %d", inst.ProcessID, os.Getpid())
	}
	if inst.Hostname == "" {
		t.Fatal("expected non-empty hostname")
	}
	if _, err := time.Parse(time.RFC3339Nano, inst.Timestamp); err != nil {
		t.Fatalf("Timestamp = %q, parse error = %v", inst.Timestamp, err)
	}
}

func TestDeregisterNon2xx(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "gone", http.StatusGone)
	}))
	t.Cleanup(server.Close)

	client := atc.NewClient(config.ATCConfig{
		Enabled: true,
		URL:     server.URL + "/api/instances",
	})

	inst := atc.Instance{Hostname: "host-a", Port: 8080, ProcessID: 1, Timestamp: time.Now().UTC().Format(time.RFC3339Nano)}
	err := client.Deregister(context.Background(), inst)
	if err == nil {
		t.Fatal("expected error for non-2xx DELETE response")
	}
}

func TestRegisterNon2xx(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusServiceUnavailable)
	}))
	t.Cleanup(server.Close)

	client := atc.NewClient(config.ATCConfig{
		Enabled: true,
		URL:     server.URL + "/api/instances",
	})

	err := client.Register(context.Background(), atc.Instance{Hostname: "host-a", Port: 8080, ProcessID: 1, Timestamp: time.Now().UTC().Format(time.RFC3339Nano)})
	if err == nil {
		t.Fatal("expected error for non-2xx response")
	}
}
