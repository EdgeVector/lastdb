#!/usr/bin/env bash
# lint-no-write-path-syncing-store.sh
#
# Ban reintroduction of write-path CDC store decorators
# (SyncingKvStore / SyncingNamespacedStore). Backup capture stays outside
# ordinary writes. Retired cloud upload producers are not required here.
#
# Scope: fold_db/crates/core/src/**/*.rs.
# Docs under docs/ may still name the historical types.
#
# Exit 0 on clean tree; 1 if banned patterns appear.

set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
src="$root/fold_db/crates/core/src"

if ! command -v rg >/dev/null 2>&1; then
  echo "lint-no-write-path-syncing-store: ripgrep (rg) not found in PATH" >&2
  exit 1
fi

if [[ ! -d "$src" ]]; then
  echo "lint-no-write-path-syncing-store: missing $src" >&2
  exit 1
fi

# Type / module reintroduction — not historical prose in docs/.
patterns=(
  'struct SyncingKvStore'
  'struct SyncingNamespacedStore'
  'SyncingKvStore::'
  'SyncingNamespacedStore::'
  'mod syncing_store'
  'mod syncing_namespaced_store'
  'use .*syncing_store'
  'use .*syncing_namespaced_store'
)

failed=0
for pat in "${patterns[@]}"; do
  if matches="$(rg -n --glob '*.rs' -e "$pat" "$src" 2>/dev/null)"; then
    echo "lint-no-write-path-syncing-store: banned pattern /$pat/:" >&2
    echo "$matches" >&2
    failed=1
  fi
done

if [[ -f "$src/storage/syncing_store.rs" ]] || [[ -f "$src/storage/syncing_namespaced_store.rs" ]]; then
  echo "lint-no-write-path-syncing-store: deleted module files must stay gone" >&2
  failed=1
fi

if [[ "$failed" -ne 0 ]]; then
  echo "lint-no-write-path-syncing-store: FAIL" >&2
  echo "Keep backup capture outside writes; never re-wrap KvStore for CDC." >&2
  exit 1
fi

echo "lint-no-write-path-syncing-store: ok"
