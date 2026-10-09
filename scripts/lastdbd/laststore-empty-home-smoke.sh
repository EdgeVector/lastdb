#!/usr/bin/env bash
# Phase 1 Mini cutover: boot FoldDB against an empty Last Store home (throwaway).
# Never targets primary ~/.lastdb. Exit 0 on put/get + re-open proof.
set -euo pipefail

cd "$(dirname "$0")/../.."

export LASTDB_ENGINE=laststore
export FOLD_DISABLE_NATIVE_INDEX=1

cleanup_home=""
home="${1:-}"
if [ -z "$home" ]; then
  home="$(mktemp -d "${TMPDIR:-/tmp}/lastdb-laststore-empty-home-smoke.XXXXXX")"
  cleanup_home="$home"
  trap 'if [ -n "${cleanup_home}" ]; then rm -rf "${cleanup_home}"; fi' EXIT
fi

# Refuse primary homes even if the caller passed an explicit path.
resolved="$(cd "$home" 2>/dev/null && pwd -P || printf '%s' "$home")"
case "$resolved" in
  "$HOME/.lastdb"|"$HOME/.lastdb"/*|"$HOME/.folddb"|"$HOME/.folddb"/*)
    echo "laststore-empty-home-smoke: refusing primary/legacy home: $resolved" >&2
    exit 64
    ;;
esac

mkdir -p "$home"

cargo run -p lastdb_node --bin lastdb_laststore_empty_home_smoke -- "$home"
