#!/usr/bin/env bash
# Fail if schema_service_core grows process-global mutable state.
#
# Clippy denies OnceLock, LazyLock, and once_cell. This gate covers the two
# spellings clippy does not: `static mut` and the `lazy_static` macro.

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
scope="$repo_root/schema_service/crates/core"

matches="$(rg -n --glob '*.rs' -e 'static[[:space:]]+mut' -e 'lazy_static' "$scope" || true)"
if [[ -n "$matches" ]]; then
  printf '%s\n' "$matches" >&2
  echo "FAIL: schema_service/crates/core must not use static mut or lazy_static" >&2
  exit 1
fi

echo "OK: schema_service/crates/core has no static mut or lazy_static"
