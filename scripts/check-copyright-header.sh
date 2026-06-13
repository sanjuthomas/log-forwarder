#!/usr/bin/env bash
# Verify every Go file has the standard MIT copyright header (see CONTRIBUTING.md).
# Usage: ./scripts/check-copyright-header.sh [path...]
#   With no paths, checks all *.go files under the repository root.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

check_file() {
  local file=$1
  local issues=()

  if ! head -n 10 "$file" | grep -qE '^// Copyright \(c\)'; then
    issues+=("missing // Copyright (c) in first 10 lines")
  fi
  if ! head -n 10 "$file" | grep -q 'SPDX-License-Identifier: MIT'; then
    issues+=("missing SPDX-License-Identifier: MIT in first 10 lines")
  fi

  local first_line
  first_line=$(grep -m1 '.' "$file" || true)
  if [[ "$first_line" != //* ]]; then
    issues+=("header must start at line 1")
  fi

  if [[ ${#issues[@]} -gt 0 ]]; then
    printf '%s: %s\n' "$file" "${issues[*]}"
    return 1
  fi
  return 0
}

failed=0
checked=0
while IFS= read -r file; do
  checked=$((checked + 1))
  if ! check_file "$file"; then
    failed=$((failed + 1))
  fi
done < <(
  if [[ $# -gt 0 ]]; then
    printf '%s\n' "$@"
  else
    find . -name '*.go' -not -path './.git/*' | sort
  fi
)

if [[ $failed -gt 0 ]]; then
  echo ""
  echo "copyright check failed: $failed of $checked file(s)"
  echo "Fix with ./scripts/add-copyright-header.sh <file> or add headers manually (see CONTRIBUTING.md)."
  exit 1
fi

echo "copyright check: ok ($checked files)"
