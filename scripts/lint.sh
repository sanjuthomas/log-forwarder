#!/usr/bin/env bash
# Run the same format and lint checks as CI (golangci-lint).
# Usage: ./scripts/lint.sh [--fix]
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

if ! command -v golangci-lint >/dev/null 2>&1; then
  gopath_bin="$(go env GOPATH)/bin"
  if [[ -x "${gopath_bin}/golangci-lint" ]]; then
    export PATH="${gopath_bin}:$PATH"
  fi
fi

if ! command -v golangci-lint >/dev/null 2>&1; then
  echo "golangci-lint not found; install with:"
  echo "  go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest"
  exit 1
fi

if [[ "${1:-}" == "--fix" ]]; then
  golangci-lint fmt ./...
  golangci-lint run --fix ./...
else
  golangci-lint fmt --diff ./...
  golangci-lint run ./...
fi

echo "lint: ok"
