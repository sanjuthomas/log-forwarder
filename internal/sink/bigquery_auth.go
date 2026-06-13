// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

package sink

import (
	"fmt"
	"os"

	"cloud.google.com/go/auth/credentials"
	"github.com/sanjuthomas/log-forwarder/internal/config"
	"google.golang.org/api/option"
)

func bigQueryClientOptions(cfg config.BigQueryConfig) ([]option.ClientOption, error) {
	if cfg.CredentialsFile == "" {
		return nil, nil
	}
	creds, err := credentials.DetectDefault(&credentials.DetectOptions{
		CredentialsFile: cfg.CredentialsFile,
	})
	if err != nil {
		return nil, fmt.Errorf("load google credentials from %q: %w", cfg.CredentialsFile, err)
	}
	return []option.ClientOption{option.WithAuthCredentials(creds)}, nil
}

func validateCredentialsFile(path string) error {
	if path == "" {
		return nil
	}
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("bigquery credentials_file: %w", err)
	}
	if info.IsDir() {
		return fmt.Errorf("bigquery credentials_file %q is a directory", path)
	}
	return nil
}
