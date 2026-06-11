// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package atc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/sanjuthomas/log-forwarder/internal/config"
)

// Instance identifies a running log-forwarder process to the ATC.
type Instance struct {
	Hostname  string `json:"hostname"`
	Port      int    `json:"port"`
	ProcessID int    `json:"process_id"`
	Timestamp string `json:"timestamp"`
}

// Client registers and deregisters forwarder instances with log-forwarder-atc.
type Client struct {
	instancesURL string
	httpClient   *http.Client
}

// NewClient constructs an ATC client when registration is enabled.
func NewClient(cfg config.ATCConfig) *Client {
	if !cfg.Enabled {
		return nil
	}
	return &Client{
		instancesURL: cfg.InstancesURL(),
		httpClient: &http.Client{
			Timeout: cfg.TimeoutDuration(),
		},
	}
}

// NewInstance builds the registration payload for the current process.
func NewInstance(cfg *config.Config) Instance {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = ""
	}

	port := cfg.Metrics.Port
	if port == 0 {
		port = 8080
	}

	return Instance{
		Hostname:  hostname,
		Port:      port,
		ProcessID: os.Getpid(),
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
	}
}

// Register notifies the ATC that this forwarder instance is ready.
func (c *Client) Register(ctx context.Context, inst Instance) error {
	return c.do(ctx, http.MethodPut, inst)
}

// Deregister notifies the ATC that this forwarder instance is shutting down.
func (c *Client) Deregister(ctx context.Context, inst Instance) error {
	return c.do(ctx, http.MethodDelete, inst)
}

func (c *Client) do(ctx context.Context, method string, inst Instance) error {
	body, err := json.Marshal(inst)
	if err != nil {
		return fmt.Errorf("marshal instance: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.instancesURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("atc request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("atc %s: %w", method, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("atc %s: status %d: %s", method, resp.StatusCode, bytes.TrimSpace(respBody))
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}
