#!/usr/bin/env bash
# Schema catalog diff: shows whether a change left schema loading, hashing and
# serializing unchanged.
#
# Boots an OLD and a NEW lastdbd, each on its own FRESH EMPTY home, declares the
# same synthetic schema on each (lib/synthetic-node.sh), dumps the full schema
# catalog from each over the owner socket, and diffs the two. The script never
# copies a home and never reads ~/.lastdb (Tom, 2026-10-12: a copy of the
# primary costs disk the machine does not have).
#
# Exit: 0 = catalogs identical, 2 = they differ, 1 = could not measure.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

usage() {
  cat <<'EOF'
Usage: schema-catalog-diff.sh --old-bin PATH --new-bin PATH
                              [--out DIR] [--old-label TEXT] [--new-label TEXT]
                              [--keep]

Boots --old-bin and --new-bin, one after the other, each on a fresh empty home
under /private/tmp. Each node registers its own system schemas and the
synthetic validation/MiniFreshInstall schema. The script reads
GET /api/schemas?include_full=true&include_system=true from each and diffs the
sorted JSON.

  --out     where the two catalogs, the diff and a summary are written
            (default a new directory under $TMPDIR). Kept after the run.
  --keep    keep the fresh homes and boot logs (default: delete them on exit).

Exit: 0 = IDENTICAL, 2 = DIFFERENT, 1 = could not measure.
EOF
}

OLD_BIN=""
NEW_BIN=""
OLD_LABEL="old"
NEW_LABEL="new"
OUT=""
KEEP=0
SCHEMAS_READY_TIMEOUT_SECS="${LASTDB_BAR_SCHEMAS_READY_TIMEOUT_SECS:-60}"

while [ "$#" -gt 0 ]; do
  case "$1" in
    --old-bin) OLD_BIN="$2"; shift 2 ;;
    --new-bin) NEW_BIN="$2"; shift 2 ;;
    --old-label) OLD_LABEL="$2"; shift 2 ;;
    --new-label) NEW_LABEL="$2"; shift 2 ;;
    --out) OUT="$2"; shift 2 ;;
    --keep) KEEP=1; shift ;;
    --source|--root) echo "$1 is gone: the script never copies a home" >&2; exit 64 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 64 ;;
  esac
done
export KEEP

log() { printf 'schema-catalog-diff: %s\n' "$*" >&2; }
fail() { log "$*"; exit 1; }

command -v jq >/dev/null 2>&1 || fail "jq is required"
command -v curl >/dev/null 2>&1 || fail "curl is required"

[ -n "$OLD_BIN" ] && [ -x "$OLD_BIN" ] || fail "--old-bin must be an executable lastdbd"
[ -n "$NEW_BIN" ] && [ -x "$NEW_BIN" ] || fail "--new-bin must be an executable lastdbd"

# shellcheck source=lib/synthetic-node.sh
. "$HERE/lib/synthetic-node.sh"

if [ -z "$OUT" ]; then
  OUT="$(mktemp -d "${TMPDIR:-/private/tmp}/schema-catalog-diff.XXXXXX")"
else
  mkdir -p "$OUT"
fi

trap syn_cleanup EXIT INT TERM

# dump_catalog <label> <bin> <outfile>: fresh empty home, synthetic schema, dump.
dump_catalog() {
  local label="$1" bin="$2" outfile="$3" i=0 ready=""
  log "[$label] boot $bin on a fresh empty home"
  syn_boot "$bin" || fail "[$label] node did not boot"
  syn_declare || fail "[$label] synthetic schema declare failed"

  while [ "$i" -lt "$SCHEMAS_READY_TIMEOUT_SECS" ]; do
    i=$((i + 1))
    ready="$(syn_curl 'http://x/api/schemas?include_full=true&include_system=true' 2>/dev/null \
      | jq -r 'select(.ready == true) | "yes"' 2>/dev/null || true)"
    [ "$ready" = "yes" ] && break
    sleep 1
  done
  [ "$ready" = "yes" ] || fail "[$label] schema catalog not ready in ${SCHEMAS_READY_TIMEOUT_SECS}s"
  log "[$label] catalog ready after ${i}s"

  # user_hash is the node identity. Every fresh home makes its own, so it
  # always differs between the two homes and says nothing about the catalog.
  syn_curl 'http://x/api/schemas?include_full=true&include_system=true' \
    | jq -S 'del(.user_hash) | .schemas |= sort_by(.name)' >"$outfile" \
    || fail "[$label] could not read the catalog"
  [ -s "$outfile" ] || fail "[$label] catalog dump is empty"
  syn_cleanup
}

OLD_JSON="$OUT/catalog-$OLD_LABEL.json"
NEW_JSON="$OUT/catalog-$NEW_LABEL.json"

dump_catalog "$OLD_LABEL" "$OLD_BIN" "$OLD_JSON"
dump_catalog "$NEW_LABEL" "$NEW_BIN" "$NEW_JSON"

OLD_COUNT="$(jq '.schemas | length' "$OLD_JSON")"
NEW_COUNT="$(jq '.schemas | length' "$NEW_JSON")"
# owner_app_id set but no identity hash: the one case the wire conversion
# recomputes. The synthetic schema does not set owner_app_id, so this branch
# stays unexercised unless a system schema reaches it.
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
  echo "data=fresh empty home per binary plus synthetic schema $SYN_SCHEMA_NAMESPACE/$SYN_SCHEMA_NAME (no copy)"
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
    log "IDENTICAL, but no schema here has owner_app_id without an identity hash, so that branch is not exercised by it"
  fi
  exit 0
fi

log "catalogs DIFFER; first lines of the diff:"
head -n 80 "$DIFF_FILE" >&2
exit 2
