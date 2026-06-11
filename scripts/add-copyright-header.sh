#!/usr/bin/env bash
# Prepend the standard MIT copyright header to Go files that do not have one yet.
# Usage: ./scripts/add-copyright-header.sh [path...]
#   With no paths, processes all *.go files under the repository root.
set -euo pipefail

readonly HEADER='// Copyright (c) 2026 Sanju Thomas
// SPDX-License-Identifier: MIT

'

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

updated=0
skipped=0
while IFS= read -r file; do
  if grep -q 'SPDX-License-Identifier: MIT' "$file"; then
    skipped=$((skipped + 1))
    continue
  fi
  tmp="$(mktemp)"
  printf '%s' "$HEADER" > "$tmp"
  cat "$file" >> "$tmp"
  mv "$tmp" "$file"
  updated=$((updated + 1))
done < <(
  if [[ $# -gt 0 ]]; then
    printf '%s\n' "$@"
  else
    find . -name '*.go' -not -path './.git/*' | sort
  fi
)

echo "copyright header: updated=$updated skipped=$skipped"
