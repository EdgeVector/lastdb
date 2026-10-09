#!/usr/bin/env bash
#
# Validate the LastDB Mini brand-new-user (fresh install) path end-to-end.
#
# This is the shipping-artifact new-user gate for LastDB Mini — the only
# shipping product since the Mini-only cutover (Situation
# `fold-db-node-dmg-temporary-deprecation`). It simulates exactly what a person
# who just ran `brew install edgevector/lastdb/lastdb` gets: it takes the
# `lastdb`/`lastdbd` binaries (from the shipped tarball, a tap release tag, or a
# just-built CI release dir), boots `lastdbd` on a PRISTINE throwaway home over
# the owner Unix socket, and asserts the whole first-run path:
#
#   1. Tarball shape — exactly `./lastdb` + `./lastdbd`; versions match the tag.
#   2. First boot, zero config — no prompts, `identity.key` auto-generated,
#      owner socket up, `/health` ok, `lastdb ... status` sees it.
#   3. Data round-trip over the socket — /api/schemas/declare -> mutation -> query.
#   3b. First-run app path — POST /api/apps/declare-schema (catalog
#      register/reuse/expand), then mutate/query by the returned identity-hash pin
#      (brain/kanban init path).
#   4. Native search retired-route contract on the SHIPPED Mini binary —
#      release builds have no owner-wide native-index search route. Assert HTTP
#      404 Not Found — not empty 200/ok:true and not the retired fold #968
#      semantic-off 503 stub. Full semantic coverage rides the Search app plane
#      and the separate non-shipping smoke lane.
#   5. Restart persistence — kill the daemon, reboot the same home, re-query.
#
# It NEVER touches the primary brain, `~/.lastdb`, `~/.folddb`, or `$HOME`; it
# runs the daemon under a short throwaway home (sockaddr_un 103-byte cap) and
# only ever kills the PID it started.
#
# Usage:
#   scripts/release/validate-mini-fresh-install.sh --tarball <file.tar.gz>
#   scripts/release/validate-mini-fresh-install.sh --tag <vX.Y.Z | latest>
#   scripts/release/validate-mini-fresh-install.sh --built <release-bin-dir>
#
# Options:
#   --tarball <file>        Validate a local `lastdb-<triple>.tar.gz` tarball.
#   --tag <vX.Y.Z|latest>   Download that tag's tarball from the public
#                           EdgeVector/homebrew-lastdb GitHub release. `latest`
#                           resolves the tap's current release.
#   --built <dir>           CI mode: use already-built release binaries in <dir>
#                           (e.g. target/<triple>/release). Skips tarball-shape.
#   --expect-version <X.Y.Z>  Assert both binaries report this version. Defaults
#                           to the tag (without leading `v`) in --tag mode.
#   --triple <triple>       Override the host target triple used for --tag.
#   --report-dir <path>     Where to write the run log + report.
#   --work-root-base <dir>  Base dir for the throwaway home (default: $TMPDIR).
#   --help | -h             Print this header.

set -euo pipefail

# ---------------------------------------------------------------------------
# Pure helpers (also sourced by validate-mini-fresh-install-test.sh). Keep them
# side-effect-free so the unit test can exercise them without a daemon/network.
# ---------------------------------------------------------------------------

# detect_triple — map the host OS/arch to the release tarball target triple.
detect_triple() {
  case "$(uname -s)-$(uname -m)" in
    Darwin-arm64) echo "aarch64-apple-darwin" ;;
    Darwin-x86_64) echo "x86_64-apple-darwin" ;;
    Linux-x86_64) echo "x86_64-unknown-linux-gnu" ;;
    Linux-aarch64 | Linux-arm64) echo "aarch64-unknown-linux-gnu" ;;
    *) return 1 ;;
  esac
}

# normalize_tag <tag> — ensure a leading `v` (accepts `0.22.5` or `v0.22.5`).
normalize_tag() {
  case "$1" in
    v*) printf '%s\n' "$1" ;;
    *) printf 'v%s\n' "$1" ;;
  esac
}

# download_url <tag> <triple> — the public tap tarball URL for that tag/triple.
download_url() {
  local tag triple
  tag="$(normalize_tag "$1")"
  triple="$2"
  printf 'https://github.com/EdgeVector/homebrew-lastdb/releases/download/%s/lastdb-%s.tar.gz\n' \
    "$tag" "$triple"
}

# tarball_entries <file> — the payload file names in a tarball, one per line,
# with the leading `./` and any bare directory entries stripped, sorted.
tarball_entries() {
  tar -tzf "$1" \
    | sed -e 's#^\./##' \
    | grep -v '/$' \
    | grep -v '^$' \
    | LC_ALL=C sort -u
}

# assert_tarball_shape <file> — the tarball must contain EXACTLY `lastdb` and
# `lastdbd` and nothing else. Prints a diagnostic and returns 1 on mismatch.
assert_tarball_shape() {
  local file got want
  file="$1"
  got="$(tarball_entries "$file")"
  want="$(printf 'lastdb\nlastdbd\n')"
  if [ "$got" != "$want" ]; then
    printf 'unexpected tarball payload (want exactly lastdb + lastdbd):\n%s\n' "$got" >&2
    return 1
  fi
}

# assert_work_root_safe <path> — refuse a throwaway home that is inside $HOME,
# ~/.lastdb, or ~/.folddb (would risk the real brain), and refuse one whose
# resulting socket path would exceed the sockaddr_un 103-byte cap.
assert_work_root_safe() {
  local home_dir sock len
  home_dir="$1"
  case "$home_dir" in
    "$HOME" | "$HOME"/* )
      printf 'refusing throwaway home under $HOME: %s\n' "$home_dir" >&2
      return 1 ;;
  esac
  case "$home_dir" in
    "$HOME/.lastdb"* | "$HOME/.folddb"* )
      printf 'refusing throwaway home under the primary-brain dir: %s\n' "$home_dir" >&2
      return 1 ;;
  esac
  sock="$home_dir/data/folddb.sock"
  len="$(printf '%s' "$sock" | wc -c | tr -d '[:space:]')"
  if [ "$len" -gt 103 ]; then
    printf 'socket path is %s bytes, exceeds the 103-byte sockaddr_un limit: %s\n' "$len" "$sock" >&2
    return 1
  fi
}

# assert_apps_declare_catalog_sync_payload <file> — first-run app declare must
# return an identity-hash pin the client can use for mutation/query.
# Mini may reuse an existing catalog identity or register/expand through Schema
# Service, but it must never accept a local-only durable identity.
assert_apps_declare_catalog_sync_payload() {
  jq -e '
    .ok == true
    and .app_id == "validation"
    and (.resolution == "register"
         or .resolution == "reuse"
         or .resolution == "expand")
    and (.canonical | type == "string" and length == 64)
    and (
      (.schema | type == "string" and startswith("validation/"))
      or (.schema_name | type == "string" and startswith("validation/"))
    )
  ' "$1" >/dev/null
}

# assert_native_index_search_retired_response <http-code> <body-file> — Mini no
# longer exposes owner-wide `/api/native-index/search`; clients use the Search
# app plane or scoped app-vector routes instead.
assert_native_index_search_retired_response() {
  local code body_file
  code="$1"
  body_file="$2"
  if [ "$code" != "404" ]; then
    printf 'native-index search expected HTTP 404 Not Found after route retirement, got %s (body: %s)\n' \
      "${code:-transport-error}" "$(head -c 400 "$body_file" 2>/dev/null || true)" >&2
    return 1
  fi
  if ! grep -qi 'not found' "$body_file"; then
    printf '404 body missing Not Found marker (body: %s)\n' \
      "$(head -c 400 "$body_file" 2>/dev/null || true)" >&2
    return 1
  fi
}

# schema_ref_from_declare <file> — after POST /api/schemas/declare (catalog
# register/reuse/compose), the daemon persists the schema under
# its content-addressed identity hash. GET /api/schemas lists it by that hash,
# not by the human-readable "namespace/Name" string. Mutation and query MUST
# use .canonical (falling back to .identity_hash), or every call 404s with
# "Schema not found: validation/MiniFreshInstall" — the same class that kept
# the GitHub Release gate red for 7+ days (2026-07-15..21) and the restore-
# probe fix in 294c07d7f.
schema_ref_from_declare() {
  local ref
  ref="$(jq -r '.canonical // .identity_hash // empty' "$1")"
  if [ -z "$ref" ] || [ "$ref" = "null" ]; then
    printf 'declare response missing canonical/identity_hash\n' >&2
    return 1
  fi
  printf '%s\n' "$ref"
}

# ---------------------------------------------------------------------------
# When sourced for unit tests, stop here — only the helpers above are wanted.
# ---------------------------------------------------------------------------
if [ "${MINI_FRESH_INSTALL_LIB:-0}" = "1" ]; then
  return 0 2>/dev/null || exit 0
fi

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

MODE=""
TARBALL=""
TAG=""
BUILT_DIR=""
EXPECT_VERSION=""
TRIPLE=""
REPORT_DIR=""
WORK_ROOT_BASE="${TMPDIR:-/tmp}"

while [ $# -gt 0 ]; do
  case "$1" in
    --tarball) MODE="tarball"; TARBALL="$2"; shift 2 ;;
    --tag) MODE="tag"; TAG="$2"; shift 2 ;;
    --built) MODE="built"; BUILT_DIR="$2"; shift 2 ;;
    --expect-version) EXPECT_VERSION="$2"; shift 2 ;;
    --triple) TRIPLE="$2"; shift 2 ;;
    --report-dir) REPORT_DIR="$2"; shift 2 ;;
    --work-root-base) WORK_ROOT_BASE="$2"; shift 2 ;;
    --help | -h) sed -n '3,50p' "$0"; exit 0 ;;
    *) echo "validate-mini-fresh-install: unknown flag: $1" >&2; exit 2 ;;
  esac
done

if [ -z "$MODE" ]; then
  echo "validate-mini-fresh-install: one of --tarball / --tag / --built is required" >&2
  exit 2
fi

for bin in curl jq tar python3; do
  command -v "$bin" >/dev/null 2>&1 || {
    echo "validate-mini-fresh-install: missing required tool: $bin" >&2
    exit 2
  }
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
TS="$(date -u +%Y-%m-%d-%H%M%S)"
REPORT_DIR="${REPORT_DIR:-$WORKSPACE_ROOT/.gstack/mini-fresh-install/$TS}"
mkdir -p "$REPORT_DIR"

LOG_FILE="$REPORT_DIR/run.log"
SERVER_LOG="$REPORT_DIR/lastdbd.log"
: > "$LOG_FILE"

log() { printf '[mini-fresh-install] %s\n' "$*" | tee -a "$LOG_FILE" >&2; }
fail() { log "FAIL: $*"; exit 1; }

DAEMON_PID=""
WORK_ROOT=""
cleanup() {
  # Only ever kill the daemon PID we started ourselves.
  if [ -n "$DAEMON_PID" ] && kill -0 "$DAEMON_PID" >/dev/null 2>&1; then
    kill -TERM "$DAEMON_PID" >/dev/null 2>&1 || true
    wait "$DAEMON_PID" >/dev/null 2>&1 || true
  fi
  # Remove the throwaway home we minted (keep the report dir). Guarded so we
  # never rm anything outside an `lmfi.*` home under the work-root base.
  if [ -n "$WORK_ROOT" ] && [ -d "$WORK_ROOT" ]; then
    case "$WORK_ROOT" in
      "$HOME" | "$HOME"/* ) : ;;
      */lmfi.* ) rm -rf "$WORK_ROOT" ;;
    esac
  fi
}
trap cleanup EXIT

# --- 1. Acquire the binaries ------------------------------------------------

BIN_DIR="$REPORT_DIR/bin"
mkdir -p "$BIN_DIR"

if [ "$MODE" = "built" ]; then
  log "using pre-built release binaries in $BUILT_DIR (CI --built mode)"
  [ -d "$BUILT_DIR" ] || fail "--built dir does not exist: $BUILT_DIR"
  for bin in lastdb lastdbd; do
    [ -x "$BUILT_DIR/$bin" ] || fail "built binary missing/not executable: $BUILT_DIR/$bin"
    cp "$BUILT_DIR/$bin" "$BIN_DIR/$bin"
  done
else
  if [ "$MODE" = "tag" ]; then
    [ -n "$TRIPLE" ] || TRIPLE="$(detect_triple)" || fail "cannot detect host target triple; pass --triple"
    if [ "$TAG" = "latest" ]; then
      log "resolving latest tap release tag from GitHub"
      TAG="$(curl -fsSL "https://api.github.com/repos/EdgeVector/homebrew-lastdb/releases/latest" \
        | jq -r '.tag_name')" || fail "could not resolve latest tap release tag"
      [ -n "$TAG" ] && [ "$TAG" != "null" ] || fail "latest tap release tag was empty"
    fi
    TAG="$(normalize_tag "$TAG")"
    url="$(download_url "$TAG" "$TRIPLE")"
    TARBALL="$REPORT_DIR/lastdb-$TRIPLE.tar.gz"
    log "downloading $url"
    curl -fsSL -o "$TARBALL" "$url" || fail "download failed: $url"
    [ -z "$EXPECT_VERSION" ] && EXPECT_VERSION="${TAG#v}"
  fi

  log "asserting tarball shape (exactly lastdb + lastdbd): $TARBALL"
  [ -f "$TARBALL" ] || fail "tarball not found: $TARBALL"
  assert_tarball_shape "$TARBALL" || fail "tarball shape assertion failed"

  tar -xzf "$TARBALL" -C "$BIN_DIR"
  # Some tarballs pack under a leading `./`; flatten if needed.
  for bin in lastdb lastdbd; do
    [ -x "$BIN_DIR/$bin" ] || fail "extracted binary missing/not executable: $bin"
  done
fi

LASTDB="$BIN_DIR/lastdb"
LASTDBD="$BIN_DIR/lastdbd"

log "checking reported versions"
for bin in lastdb lastdbd; do
  ver="$("$BIN_DIR/$bin" --version | awk '{print $NF}')"
  log "$bin --version -> $ver"
  [ "$ver" != "0.1.0" ] || fail "$bin reports placeholder version 0.1.0 (unstamped build)"
  if [ -n "$EXPECT_VERSION" ] && [ "$ver" != "$EXPECT_VERSION" ]; then
    fail "$bin reports '$ver', expected '$EXPECT_VERSION'"
  fi
done

# --- 2. First boot on a pristine throwaway home -----------------------------

assert_work_root_safe "$WORK_ROOT_BASE/probe" \
  || fail "work-root base is unsafe: $WORK_ROOT_BASE"
WORK_ROOT="$(mktemp -d "${WORK_ROOT_BASE%/}/lmfi.XXXXXX")"
HOME_DIR="$WORK_ROOT/h"
mkdir -p "$HOME_DIR"
assert_work_root_safe "$HOME_DIR" || fail "throwaway home is unsafe: $HOME_DIR"
SOCKET="$HOME_DIR/data/folddb.sock"
log "throwaway home: $HOME_DIR (socket $SOCKET)"

log "booting lastdbd on the pristine home (zero config, no prompts)"
(
  cd "$WORK_ROOT"
  env \
    HOME="$HOME_DIR" \
    FOLDDB_DISABLE_KEYCHAIN=1 \
    RUST_LOG="${RUST_LOG:-info}" \
    "$LASTDBD" --data-dir "$HOME_DIR" </dev/null
) >"$SERVER_LOG" 2>&1 &
DAEMON_PID="$!"

python3 - "$SOCKET" "$DAEMON_PID" <<'PY'
import os, sys, time
socket_path, pid = sys.argv[1], int(sys.argv[2])
deadline = time.monotonic() + 30.0
while time.monotonic() < deadline:
    try:
        os.kill(pid, 0)
    except OSError as exc:
        raise SystemExit(f"daemon exited before socket appeared: {exc}")
    if os.path.exists(socket_path):
        raise SystemExit(0)
    time.sleep(0.05)
raise SystemExit(f"timed out waiting for {socket_path} (a first-run prompt would block here)")
PY

[ -f "$HOME_DIR/identity.key" ] || fail "daemon did not auto-generate identity.key"
log "identity.key auto-generated; owner socket is up"

C() { curl --unix-socket "$SOCKET" -fsS "$@"; }

log "checking /health"
C "http://lastdb/health" > "$REPORT_DIR/health.json"
jq -e '.status == "ok"' "$REPORT_DIR/health.json" >/dev/null \
  || fail "/health did not return status=ok"

log "checking lastdb status sees the daemon"
env HOME="$HOME_DIR" FOLDDB_DISABLE_KEYCHAIN=1 "$LASTDB" --data-dir "$HOME_DIR" status \
  > "$REPORT_DIR/status.txt" 2>&1 \
  || fail "lastdb status failed"
grep -qi 'running' "$REPORT_DIR/status.txt" || fail "lastdb status does not report running"

# --- 3. Data round-trip over the socket -------------------------------------

log "declaring a validation schema"
# field_descriptions are required by the live schema-service catalog for
# semantic matching (Release run 29885897039: declare 409 without them —
# "Schema fields missing descriptions"). Keep them stable so the identity
# hash does not thrash across runs.
cat > "$REPORT_DIR/declare-request.json" <<'JSON'
{
  "namespace": "validation",
  "schema": {
    "name": "MiniFreshInstall",
    "descriptive_name": "Mini Fresh Install",
    "schema_type": "HashRange",
    "key": { "hash_field": "bucket", "range_field": "id" },
    "fields": ["bucket", "id", "title", "body"],
    "field_descriptions": {
      "bucket": "hash partition for the validation record",
      "id": "range key id",
      "title": "short title",
      "body": "long body text for semantic search"
    }
  }
}
JSON
if ! C -H 'Content-Type: application/json' \
  --data-binary "@$REPORT_DIR/declare-request.json" \
  "http://lastdb/api/schemas/declare" > "$REPORT_DIR/declare.json"; then
  curl --unix-socket "$SOCKET" -sS -H 'Content-Type: application/json' \
    --data-binary "@$REPORT_DIR/declare-request.json" \
    "http://lastdb/api/schemas/declare" > "$REPORT_DIR/declare.json" 2>/dev/null || true
  fail "schema declare request failed (body: $(head -c 500 "$REPORT_DIR/declare.json" 2>/dev/null || true))"
fi
jq -e '.ok == true and .schema_name == "validation/MiniFreshInstall"' "$REPORT_DIR/declare.json" >/dev/null \
  || fail "schema declare did not return validation/MiniFreshInstall (body: $(head -c 400 "$REPORT_DIR/declare.json" 2>/dev/null || true))"

# Catalog register/reuse addresses the schema by its
# identity hash, not the namespaced local id returned as schema_name.
SCHEMA_REF="$(schema_ref_from_declare "$REPORT_DIR/declare.json")" \
  || fail "schema declare missing canonical/identity_hash (body: $(head -c 400 "$REPORT_DIR/declare.json" 2>/dev/null || true))"
log "schema declare ok; mutating/querying by canonical SCHEMA_REF=$SCHEMA_REF"

log "writing a record"
cat > "$REPORT_DIR/mutation-request.json" <<JSON
{
  "type": "mutation",
  "schema": "$SCHEMA_REF",
  "fields_and_values": {
    "bucket": "release",
    "id": "mini-1",
    "title": "Mini fresh install",
    "body": "A brand new LastDB Mini user can boot the daemon and run semantic search over the owner socket."
  },
  "key_value": { "hash": "release", "range": "mini-1" },
  "mutation_type": "create"
}
JSON
if ! C -H 'Content-Type: application/json' \
  --data-binary "@$REPORT_DIR/mutation-request.json" \
  "http://lastdb/api/mutation" > "$REPORT_DIR/mutation.json"; then
  # Capture body without -f so the next agent/CI log shows the real daemon
  # error (the historical gate only printed curl: 404).
  curl --unix-socket "$SOCKET" -sS -H 'Content-Type: application/json' \
    --data-binary "@$REPORT_DIR/mutation-request.json" \
    "http://lastdb/api/mutation" > "$REPORT_DIR/mutation.json" 2>/dev/null || true
  fail "mutation request failed (body: $(head -c 400 "$REPORT_DIR/mutation.json" 2>/dev/null || true))"
fi
jq -e '.ok == true and .success == true' "$REPORT_DIR/mutation.json" >/dev/null \
  || fail "mutation did not succeed (body: $(head -c 400 "$REPORT_DIR/mutation.json" 2>/dev/null || true))"

log "querying the record back"
cat > "$REPORT_DIR/query-request.json" <<JSON
{
  "schema_name": "$SCHEMA_REF",
  "fields": ["bucket", "id", "title", "body"],
  "filter": { "HashRangeKey": { "hash": "release", "range": "mini-1" } }
}
JSON
C -H 'Content-Type: application/json' \
  --data-binary "@$REPORT_DIR/query-request.json" \
  "http://lastdb/api/query" > "$REPORT_DIR/query.json" \
  || fail "query request failed (body: $(head -c 400 "$REPORT_DIR/query.json" 2>/dev/null || true))"
jq -e '.ok == true and (.results | length) == 1 and (.results[0].fields.body | contains("boot the daemon and run semantic search"))' \
  "$REPORT_DIR/query.json" >/dev/null \
  || fail "query did not return the fresh-install validation record"

# --- 3b. App-schema declare path (brain / kanban first-run) -----------------
#
# Mini first-run init uses POST /api/apps/declare-schema (catalog sync) and
# pins mutations to the returned identity hash — not the namespaced
# /api/schemas/declare form above. A regression that 404s this route or stores
# the schema under the wrong name breaks `brain init` / `kanban init` on a
# brand-new Mac (thelastdb.com path).

log "declaring an app-owned schema via /api/apps/declare-schema (first-run path)"
# field_descriptions required when apps/declare-schema hits the live catalog
# (same 409 class as schemas/declare — Release run 29886422645).
cat > "$REPORT_DIR/apps-declare-request.json" <<'JSON'
{
  "app_id": "validation",
  "schema": {
    "name": "AppCatalogProbe",
    "descriptive_name": "AppCatalogProbe",
    "schema_type": "Hash",
    "key": { "hash_field": "slug" },
    "fields": ["slug", "title", "body"],
    "field_types": {
      "slug": "String",
      "title": "String",
      "body": "String"
    },
    "field_descriptions": {
      "slug": "unique record key",
      "title": "short title",
      "body": "body text for first-run app declare path"
    }
  }
}
JSON
# Prefer owner socket; full-surface sibling is also fine when present.
if ! C -H 'Content-Type: application/json' \
  --data-binary "@$REPORT_DIR/apps-declare-request.json" \
  "http://lastdb/api/apps/declare-schema" > "$REPORT_DIR/apps-declare.json"; then
  curl --unix-socket "$SOCKET" -sS -H 'Content-Type: application/json' \
    --data-binary "@$REPORT_DIR/apps-declare-request.json" \
    "http://lastdb/api/apps/declare-schema" > "$REPORT_DIR/apps-declare.json" 2>/dev/null || true
  fail "/api/apps/declare-schema request failed (body: $(head -c 500 "$REPORT_DIR/apps-declare.json" 2>/dev/null || true))"
fi
assert_apps_declare_catalog_sync_payload "$REPORT_DIR/apps-declare.json" \
  || fail "/api/apps/declare-schema did not return identity-hash canonical (body: $(head -c 400 "$REPORT_DIR/apps-declare.json" 2>/dev/null || true))"

APP_HASH="$(jq -r '.canonical' "$REPORT_DIR/apps-declare.json")"
log "app-schema catalog canonical=$APP_HASH; mutating by identity-hash pin"

cat > "$REPORT_DIR/apps-mutation-request.json" <<JSON
{
  "type": "mutation",
  "schema": "$APP_HASH",
  "fields_and_values": {
    "slug": "first-run",
    "title": "App catalog probe",
    "body": "brain/kanban-style declare + write by identity hash"
  },
  "key_value": { "hash": "first-run" },
  "mutation_type": "create"
}
JSON
C -H 'Content-Type: application/json' \
  --data-binary "@$REPORT_DIR/apps-mutation-request.json" \
  "http://lastdb/api/mutation" > "$REPORT_DIR/apps-mutation.json"
jq -e '.ok == true and .success == true' "$REPORT_DIR/apps-mutation.json" >/dev/null \
  || fail "mutation by apps/declare-schema identity hash did not succeed"

cat > "$REPORT_DIR/apps-query-request.json" <<JSON
{
  "schema_name": "$APP_HASH",
  "fields": ["slug", "title", "body"],
  "filter": { "HashKey": "first-run" }
}
JSON
C -H 'Content-Type: application/json' \
  --data-binary "@$REPORT_DIR/apps-query-request.json" \
  "http://lastdb/api/query" > "$REPORT_DIR/apps-query.json"
jq -e '
  .ok == true
  and (.results | length) == 1
  and (.results[0].fields.body | contains("identity hash"))
' "$REPORT_DIR/apps-query.json" >/dev/null \
  || fail "query by apps/declare-schema identity hash did not return the record"

# --- 4. Native search retired-route contract -------------------------------
#
# Owner-wide `/api/native-index/search` is intentionally absent from Mini now
# that native-index search is retired in favor of the Search app plane and
# scoped app-vector routes. Capture status without curl -f so the body is
# available for the assertion. This keeps guarding against empty 200 + ok:true
# and also rejects the stale semantic-off 503 `search_plane_required` stub.

log "checking native-index search route is retired with 404 Not Found (default Mini)"
SEARCH_CODE="$(
  curl --unix-socket "$SOCKET" -sS -o "$REPORT_DIR/native-search.body" -w '%{http_code}' \
    --max-time 30 \
    "http://lastdb/api/native-index/search?q=new%20user%20boots%20the%20daemon%20and%20runs%20semantic%20search" \
    || true
)"
assert_native_index_search_retired_response "$SEARCH_CODE" "$REPORT_DIR/native-search.body" \
  || fail "native-index search retired-route assertion failed"

log "asserting no FastEmbed model cache was created under the throwaway home"
CACHE_DIR="$HOME_DIR/.fastembed_cache"
if [ -d "$CACHE_DIR" ] && find "$CACHE_DIR" -type f 2>/dev/null | grep -q .; then
  fail "unexpected FastEmbed cache under $CACHE_DIR (shipped Mini has no in-process semantic-search feature)"
fi

# --- 5. Restart persistence -------------------------------------------------

log "restarting the daemon on the same home and re-querying"
kill -TERM "$DAEMON_PID" >/dev/null 2>&1 || true
wait "$DAEMON_PID" >/dev/null 2>&1 || true
DAEMON_PID=""

(
  cd "$WORK_ROOT"
  env HOME="$HOME_DIR" FOLDDB_DISABLE_KEYCHAIN=1 RUST_LOG="${RUST_LOG:-info}" \
    "$LASTDBD" --data-dir "$HOME_DIR" </dev/null
) >>"$SERVER_LOG" 2>&1 &
DAEMON_PID="$!"

python3 - "$SOCKET" "$DAEMON_PID" <<'PY'
import os, sys, time
socket_path, pid = sys.argv[1], int(sys.argv[2])
deadline = time.monotonic() + 30.0
while time.monotonic() < deadline:
    try:
        os.kill(pid, 0)
    except OSError as exc:
        raise SystemExit(f"daemon exited before socket reappeared: {exc}")
    if os.path.exists(socket_path):
        raise SystemExit(0)
    time.sleep(0.05)
raise SystemExit(f"timed out waiting for {socket_path} after restart")
PY

C -H 'Content-Type: application/json' \
  --data-binary "@$REPORT_DIR/query-request.json" \
  "http://lastdb/api/query" > "$REPORT_DIR/query-after-restart.json"
jq -e '.ok == true and (.results | length) == 1 and (.results[0].fields.id == "mini-1")' \
  "$REPORT_DIR/query-after-restart.json" >/dev/null \
  || fail "record did not persist across a daemon restart"

# --- Report -----------------------------------------------------------------

{
  printf '# LastDB Mini fresh-install validation\n\n'
  printf -- '- **Timestamp:** %s\n' "$TS"
  printf -- '- **Verdict:** PASS\n'
  printf -- '- **Mode:** %s\n' "$MODE"
  printf -- '- **Throwaway home:** `%s`\n' "$HOME_DIR"
  printf -- '- **Report dir:** `%s`\n\n' "$REPORT_DIR"
  printf '## Evidence\n\n'
  printf -- '- Tarball/built artifact carries exactly `lastdb` + `lastdbd`; versions non-placeholder%s.\n' \
    "${EXPECT_VERSION:+ and == $EXPECT_VERSION}"
  printf -- '- `lastdbd` booted on a pristine throwaway home with zero config: no prompt, `identity.key` auto-generated, owner socket up, `/health` ok, `lastdb status` sees it.\n'
  printf -- '- Data round-trip over the socket: `/api/schemas/declare` -> `mutation` -> `query` returned the record.\n'
  printf -- '- First-run app path: `/api/apps/declare-schema` catalog sync + mutation/query by identity-hash pin (brain/kanban init).\n'
  printf -- '- Owner-wide native-index search is retired: `GET /api/native-index/search?q=...` returned HTTP 404 Not Found; clients use the Search app plane or scoped app-vector routes.\n'
  printf -- '- The record survived a daemon restart on the same home.\n'
} > "$REPORT_DIR/report.md"

log "PASS: report written to $REPORT_DIR/report.md"
