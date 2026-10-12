#!/usr/bin/env bash
# Schema catalog diff: proves a change left schema loading, hashing and
# serializing unchanged.
#
# Boots an OLD and a NEW lastdbd, each on its own throwaway CoW copy of the same
# real LastDB home, dumps the full schema catalog from each over the owner
# socket, and diffs the two. NEVER touches the live home.
#
# Exit: 0 = catalogs identical, 2 = they differ, 1 = could not measure.
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: schema-catalog-diff.sh --old-bin PATH --new-bin PATH
                              [--source PATH] [--root PATH] [--out DIR]
                              [--old-label TEXT] [--new-label TEXT] [--keep]

Boots --old-bin and --new-bin on two separate copies of --source (default
~/.lastdb), reads GET /api/schemas?include_full=true&include_system=true from
each, and diffs the sorted JSON.

  --source  home to copy (default $LASTDB_HOME or ~/.lastdb). Only ever read.
  --root    where copies are made (default $LASTDB_PROBE_ROOT or
            ~/.lastdb-test-copies). Must not be inside --source.
  --out     where the two catalogs, the diff and a summary are written
            (default <root>/schema-catalog-diff-<timestamp>). Kept after the run.
  --keep    keep the copies and boot logs (default: delete them on exit).

Exit: 0 = IDENTICAL, 2 = DIFFERENT, 1 = could not measure.
EOF
}

SOURCE_HOME="${LASTDB_HOME:-$HOME/.lastdb}"
ROOT="${LASTDB_PROBE_ROOT:-$HOME/.lastdb-test-copies}"
OLD_BIN=""
NEW_BIN=""
OLD_LABEL="old"
NEW_LABEL="new"
OUT=""
KEEP=0
BOOT_TIMEOUT_SECS="${LASTDB_BAR_BOOT_TIMEOUT_SECS:-300}"
OP_TIMEOUT_SECS="${LASTDB_BAR_OP_TIMEOUT_SECS:-60}"

while [ "$#" -gt 0 ]; do
  case "$1" in
    --old-bin) OLD_BIN="$2"; shift 2 ;;
    --new-bin) NEW_BIN="$2"; shift 2 ;;
    --old-label) OLD_LABEL="$2"; shift 2 ;;
    --new-label) NEW_LABEL="$2"; shift 2 ;;
    --source) SOURCE_HOME="$2"; shift 2 ;;
    --root) ROOT="$2"; shift 2 ;;
    --out) OUT="$2"; shift 2 ;;
    --keep) KEEP=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 64 ;;
  esac
done

log() { printf 'schema-catalog-diff: %s\n' "$*" >&2; }
fail() { log "$*"; exit 1; }

command -v jq >/dev/null 2>&1 || fail "jq is required"
command -v curl >/dev/null 2>&1 || fail "curl is required"
command -v python3 >/dev/null 2>&1 || fail "python3 is required"

[ -n "$OLD_BIN" ] && [ -x "$OLD_BIN" ] || fail "--old-bin must be an executable lastdbd"
[ -n "$NEW_BIN" ] && [ -x "$NEW_BIN" ] || fail "--new-bin must be an executable lastdbd"
[ -d "$SOURCE_HOME" ] || fail "source home not found: $SOURCE_HOME"

realpath_of() { python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "$1"; }

SRC_REAL="$(realpath_of "$SOURCE_HOME")"
mkdir -p "$ROOT"
ROOT_REAL="$(realpath_of "$ROOT")"
case "$ROOT_REAL/" in
  "$SRC_REAL"/*) fail "--root ($ROOT_REAL) is inside --source ($SRC_REAL); refusing" ;;
esac
[ "$ROOT_REAL" != "$SRC_REAL" ] || fail "--root equals --source; refusing"

STAMP="$(date +%Y%m%dT%H%M%S)"
OUT="${OUT:-$ROOT_REAL/schema-catalog-diff-$STAMP}"
mkdir -p "$OUT"

PIDS=()
COPIES=()

cleanup() {
  local pid copy
  for pid in ${PIDS[@]+"${PIDS[@]}"}; do
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
      sleep 1
      kill -9 "$pid" 2>/dev/null || true
    fi
  done
  if [ "$KEEP" = "1" ]; then
    for copy in ${COPIES[@]+"${COPIES[@]}"}; do log "kept copy at $copy"; done
    return
  fi
  for copy in ${COPIES[@]+"${COPIES[@]}"}; do
    case "$(realpath_of "$copy")" in
      "$ROOT_REAL"/csd-*) rm -rf "$copy" "$copy.boot.log" 2>/dev/null || true ;;
    esac
  done
}
trap cleanup EXIT INT TERM

# dump_catalog <label> <bin> <outfile>
dump_catalog() {
  local label="$1" bin="$2" outfile="$3"
  local copy="$ROOT_REAL/csd-$label-$$"
  local blog="$copy.boot.log"
  local sock="$copy/data/folddb.sock"
  COPIES+=("$copy")

  log "[$label] CoW clone of $SRC_REAL -> $copy"
  if ! cp -cR "$SRC_REAL" "$copy" 2>/dev/null; then
    log "[$label] clonefile copy failed; using an ordinary recursive copy"
    rm -rf "$copy"
    cp -R "$SRC_REAL" "$copy"
  fi
  [ -d "$copy/data" ] || fail "[$label] copy is incomplete (no data dir)"
  case "$(realpath_of "$copy")" in
    "$SRC_REAL"|"$SRC_REAL"/*) fail "[$label] copy resolved inside the source; refusing" ;;
  esac
  printf 'schema-catalog-diff %s pid=%s - do not reap while present\n' "$STAMP" "$$" \
    >"$copy/.hold-schema-catalog-diff"
  # A copy must never sync to the cloud or reuse the live node's socket.
  rm -f "$copy/cloud_sync.json" "$copy/data/"*.sock 2>/dev/null || true

  log "[$label] boot: $bin --data-dir <copy>"
  (
    exec env -u SENTRY_DSN -u FOLD_SENTRY_DSN "$bin" --data-dir "$copy"
  ) >"$blog" 2>&1 &
  local pid=$!
  PIDS+=("$pid")

  local i=0 ready=""
  while [ "$i" -lt "$BOOT_TIMEOUT_SECS" ]; do
    i=$((i + 1))
    if ! kill -0 "$pid" 2>/dev/null; then
      fail "[$label] node exited during boot: $(tail -3 "$blog" 2>/dev/null | tr '\n' ' ')"
    fi
    if [ -S "$sock" ]; then
      ready="$(curl -sS --max-time 3 --unix-socket "$sock" -H 'Host: localhost' \
        'http://x/api/schemas?include_full=true&include_system=true' 2>/dev/null \
        | jq -r 'select(.ready == true) | "yes"' 2>/dev/null || true)"
      [ "$ready" = "yes" ] && break
    fi
    sleep 1
  done
  [ "$ready" = "yes" ] || fail "[$label] schema catalog not ready in ${BOOT_TIMEOUT_SECS}s"
  log "[$label] catalog ready after ${i}s"

  curl -sS --max-time "$OP_TIMEOUT_SECS" --unix-socket "$sock" -H 'Host: localhost' \
    'http://x/api/schemas?include_full=true&include_system=true' \
    | jq -S '.schemas |= sort_by(.name)' >"$outfile" \
    || fail "[$label] could not read the catalog"
  [ -s "$outfile" ] || fail "[$label] catalog dump is empty"

  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
}

OLD_JSON="$OUT/catalog-$OLD_LABEL.json"
NEW_JSON="$OUT/catalog-$NEW_LABEL.json"

dump_catalog "$OLD_LABEL" "$OLD_BIN" "$OLD_JSON"
dump_catalog "$NEW_LABEL" "$NEW_BIN" "$NEW_JSON"

OLD_COUNT="$(jq '.schemas | length' "$OLD_JSON")"
NEW_COUNT="$(jq '.schemas | length' "$NEW_JSON")"
# owner_app_id set but no identity hash: the one case the wire conversion
# recomputes. Real data that never has it leaves that branch unexercised.
EDGE_QUERY='[.schemas[] | select(.owner_app_id != null and .identity_hash == null) | .name]'
EDGE_OLD="$(jq -c "$EDGE_QUERY" "$OLD_JSON")"
EDGE_NEW="$(jq -c "$EDGE_QUERY" "$NEW_JSON")"

DIFF_FILE="$OUT/catalog.diff"
VERDICT="IDENTICAL"
if ! diff -u "$OLD_JSON" "$NEW_JSON" >"$DIFF_FILE"; then
  VERDICT="DIFFERENT"
fi

SUMMARY="$OUT/summary.txt"
{
  echo "verdict=$VERDICT"
  echo "old_label=$OLD_LABEL"
  echo "new_label=$NEW_LABEL"
  echo "old_bin=$OLD_BIN"
  echo "new_bin=$NEW_BIN"
  echo "source=$SRC_REAL"
  echo "schemas_old=$OLD_COUNT"
  echo "schemas_new=$NEW_COUNT"
  echo "owner_without_identity_old=$EDGE_OLD"
  echo "owner_without_identity_new=$EDGE_NEW"
  echo "catalog_old=$OLD_JSON"
  echo "catalog_new=$NEW_JSON"
  echo "diff=$DIFF_FILE"
} | tee "$SUMMARY"

if [ "$VERDICT" = "IDENTICAL" ]; then
  if [ "$EDGE_NEW" = "[]" ]; then
    log "IDENTICAL, but no schema in this data has owner_app_id without an identity hash, so that branch is not exercised by it"
  fi
  exit 0
fi

log "catalogs DIFFER; first lines of the diff:"
head -n 80 "$DIFF_FILE" >&2
exit 2
