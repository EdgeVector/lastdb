#!/usr/bin/env bash
# Model purity lint: `lastdb_model` holds data and pure functions only.
#
# The model crate (fold_db/crates/model) is the bottom of the LastDB core:
# records, field keys, schemas, keys and mutations. Core (`fold_db`) owns the
# behavior over them. This lint fails when the model source names a behavior
# marker, or when its manifest lists a dependency outside the pure allowlist.
#
# Markers: an async runtime, file / network / environment / process I/O, a
# clock read, a random id, process-wide or task-local state, and logging.
# Comment lines are ignored, so a doc may name what the model does not do.
#
# This is a source check. The compiler enforces the dependency list (a crate
# can only name a crate it declares). This lint enforces that list and the std
# and chrono markers that a dependency list cannot see.
#
# usage: scripts/ci/model-purity-lint.sh [REPO_ROOT]
set -euo pipefail

root="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
src="$root/fold_db/crates/model/src"
manifest="$root/fold_db/crates/model/Cargo.toml"

if [[ ! -d "$src" || ! -f "$manifest" ]]; then
  echo "FAIL: model crate not found under $root/fold_db/crates/model" >&2
  exit 1
fi

markers=(
  # async runtime and blocking I/O
  '\btokio\b'
  '\bfutures\b'
  'std::fs\b'
  'std::net\b'
  'std::env\b'
  'std::process\b'
  'std::thread\b'
  '\benv::var'
  # clock reads
  'SystemTime::now'
  'Instant::now'
  'Utc::now'
  'Local::now'
  '\bunix_nanos\b'
  # random ids
  'new_v4'
  '\brand::'
  'thread_rng'
  'OsRng'
  'getrandom'
  # process-wide or task-local state
  'OnceLock'
  'LazyLock'
  'OnceCell'
  'lazy_static'
  'static mut'
  'task_local'
  'thread_local'
  '^\s*(pub(\([^)]*\))?\s+)?static\s+[A-Za-z_]'
  # logging and console output
  '\btracing::'
  '\blog::'
  '\bprintln!'
  '\beprintln!'
  '\bdbg!'
)

args=()
for marker in "${markers[@]}"; do
  args+=(-e "$marker")
done

fail=0
hits="$(rg -n --no-heading --glob '*.rs' "${args[@]}" "$src" | rg -v '^[^:]+:[0-9]+:\s*//' || true)"
if [[ -n "$hits" ]]; then
  echo "FAIL: behavior markers in the model crate source:" >&2
  printf '%s\n' "$hits" >&2
  echo "Move the behavior to fold_db (core): pass the time, id or signer value as an argument." >&2
  fail=1
fi

allowed=(base64 chrono hmac schema_types serde serde_json sha2)
deps="$(awk '
  /^\[/ {
    section = $0
    if (section ~ /^\[(target\..*\.)?(dev-|build-)?dependencies\.[A-Za-z0-9_-]+\]$/) {
      name = section
      sub(/^.*dependencies\./, "", name)
      sub(/\]$/, "", name)
      print section_kind(section) " " name
    }
    next
  }
  section ~ /^\[(target\..*\.)?(dev-|build-)?dependencies\]$/ && /^[A-Za-z0-9_-]+[[:space:]]*[=.]/ {
    name = $0
    sub(/[[:space:]]*[=.].*/, "", name)
    print section_kind(section) " " name
  }
  function section_kind(s) {
    if (s ~ /dev-dependencies/) return "dev"
    if (s ~ /build-dependencies/) return "build"
    return "normal"
  }
' "$manifest")"

while read -r kind name; do
  [[ -z "${name:-}" ]] && continue
  if [[ "$kind" != "normal" ]]; then
    echo "FAIL: model manifest lists a $kind dependency '$name'; the model has no tests or build scripts" >&2
    fail=1
    continue
  fi
  ok=0
  for a in "${allowed[@]}"; do
    [[ "$name" == "$a" ]] && ok=1
  done
  if [[ "$ok" -ne 1 ]]; then
    echo "FAIL: model dependency '$name' is outside the pure allowlist (${allowed[*]})" >&2
    fail=1
  fi
done <<<"$deps"

chrono_line="$(rg -n '^chrono\b' "$manifest" || true)"
if [[ -n "$chrono_line" ]]; then
  if ! printf '%s\n' "$chrono_line" | rg -q 'default-features\s*=\s*false'; then
    echo "FAIL: model chrono must set default-features = false (no clock feature): $chrono_line" >&2
    fail=1
  fi
  if printf '%s\n' "$chrono_line" | rg -q '"(clock|now)"'; then
    echo "FAIL: model chrono must not enable the clock or now feature: $chrono_line" >&2
    fail=1
  fi
fi

if [[ "$fail" -ne 0 ]]; then
  exit 1
fi
echo "OK: lastdb_model has no behavior markers and only pure dependencies (${allowed[*]})"
