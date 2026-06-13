#!/usr/bin/env bash
# Fail if any internal/* package with statements drops below the coverage minimum.
# Excludes internal/integration (E2E harness only). See AGENTS.md and CONTRIBUTING.md.
#
# Usage: ./scripts/check-coverage.sh [min_percent]
#   Default minimum: 80
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

min="${1:-80}"
profile="$(mktemp -t log-forwarder-coverage.XXXXXX.out)"
test_output="$(mktemp -t log-forwarder-coverage-test.XXXXXX.txt)"
trap 'rm -f "$profile" "$test_output"' EXIT

echo "check-coverage: running tests (minimum ${min}% per internal/* package)..."

if ! go test ./internal/... -coverprofile="$profile" -covermode=atomic >"$test_output" 2>&1; then
  cat "$test_output"
  exit 1
fi

failed=0
checked=0
while IFS= read -r line; do
  if [[ "$line" != ok*internal/* ]]; then
    continue
  fi
  if [[ "$line" == *"/internal/integration"* ]]; then
    continue
  fi
  if [[ "$line" == *"[no statements]"* ]]; then
    continue
  fi
  if [[ "$line" =~ coverage:\ ([0-9.]+)% ]]; then
    pct="${BASH_REMATCH[1]}"
    pkg="$(echo "$line" | awk '{print $2}')"
    checked=$((checked + 1))
    if awk -v p="$pct" -v m="$min" 'BEGIN { exit !(p+0 < m+0) }'; then
      echo "check-coverage: FAIL ${pkg} ${pct}% (minimum ${min}%)"
      failed=1
    else
      echo "check-coverage: ok   ${pkg} ${pct}%"
    fi
  fi
done < "$test_output"

if [[ "$checked" -eq 0 ]]; then
  echo "check-coverage: no internal/* packages with coverage output found"
  exit 1
fi

if [[ "$failed" -ne 0 ]]; then
  echo "check-coverage: one or more packages are below ${min}%"
  exit 1
fi

echo "check-coverage: all ${checked} internal/* packages meet the ${min}% minimum"
