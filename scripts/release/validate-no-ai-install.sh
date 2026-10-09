#!/usr/bin/env bash
#
# Validate the default LastDB no-embedder install lane.
#
# This is the lightweight release proof for a clean install that does not opt in
# to local semantic search. It intentionally exercises the minimal `lastdbd`
# daemon in a throwaway HOME/FOLDDB_HOME over the owner Unix socket, with
# Anthropic/Ollama/FastEmbed/HuggingFace environment variables removed.
#
# Usage:
#   scripts/release/validate-no-ai-install.sh [--report-dir <path>]

set -euo pipefail

REPORT_DIR=""
while [ $# -gt 0 ]; do
  case "$1" in
    --report-dir)
      REPORT_DIR="$2"
      shift 2
      ;;
    --help|-h)
      sed -n '3,18p' "$0"
      exit 0
      ;;
    *)
      echo "validate-no-ai-install: unknown flag: $1" >&2
      exit 2
      ;;
  esac
done

for bin in cargo curl jq python3 rg; do
  command -v "$bin" >/dev/null 2>&1 || {
    echo "validate-no-ai-install: missing required tool: $bin" >&2
    exit 2
  }
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
TS="$(date -u +%Y-%m-%d-%H%M%S)"
REPORT_DIR="${REPORT_DIR:-$WORKSPACE_ROOT/.gstack/no-ai-install/$TS}"
mkdir -p "$REPORT_DIR"

LOG_FILE="$REPORT_DIR/run.log"
REPORT_FILE="$REPORT_DIR/report.md"
DEPS_FILE="$REPORT_DIR/lastdb-node-normal-deps.txt"
SERVER_LOG="$REPORT_DIR/lastdbd.log"
RUN_ROOT="$(mktemp -d "/tmp/ldnai.XXXXXX")"
CLEAN_HOME="$RUN_ROOT/home"
LASTDB_HOME_RUN="$RUN_ROOT/lastdb-home"
SOCKET="$LASTDB_HOME_RUN/data/folddb.sock"
DAEMON_PID=""

: > "$LOG_FILE"

log() { printf '[no-ai-install] %s\n' "$*" | tee -a "$LOG_FILE" >&2; }
fail() { log "FAIL: $*"; exit 1; }

cleanup() {
  if [ -n "$DAEMON_PID" ] && kill -0 "$DAEMON_PID" >/dev/null 2>&1; then
    kill -TERM "$DAEMON_PID" >/dev/null 2>&1 || true
    wait "$DAEMON_PID" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

write_report() {
  local verdict="$1"
  {
    printf '# LastDB default no-embedder validation\n\n'
    printf -- '- **Timestamp:** %s\n' "$TS"
    printf -- '- **Verdict:** %s\n' "$verdict"
    printf -- '- **Report dir:** `%s`\n' "$REPORT_DIR"
    printf -- '- **Throwaway run root:** `%s`\n' "$RUN_ROOT"
    printf -- '- **Throwaway HOME:** `%s`\n' "$CLEAN_HOME"
    printf -- '- **Throwaway LASTDB_HOME/FOLDDB_HOME:** `%s`\n\n' "$LASTDB_HOME_RUN"
    printf '## Evidence\n\n'
    printf -- '- `cargo tree -p lastdb_node --edges normal --target all --locked` contains no FastEmbed/HuggingFace/ONNX/Ollama/Anthropic dependency names.\n'
    printf -- '- `cargo build --locked -p lastdb_node --bin lastdbd` succeeded in default feature mode.\n'
    printf -- '- `lastdbd` started on the owner Unix socket from a clean throwaway HOME with AI-related environment variables removed.\n'
    printf -- '- `POST /api/schemas/declare`, `POST /api/mutation`, and `POST /api/query` passed over the socket.\n'
    printf -- '- Owner-wide native-index search is retired: `GET /api/native-index/search?q=...` returned HTTP 404 Not Found; clients use the Search app plane or scoped app-vector routes.\n'
    printf -- '- No `.fastembed_cache`, HuggingFace cache, or model-download log evidence was created under the throwaway run root.\n\n'
    printf 'Key artifacts: `run.log`, `lastdbd.log`, `lastdb-node-normal-deps.txt`, `declare.json`, `mutation.json`, `query.json`, `native-search.body`.\n'
  } > "$REPORT_FILE"
}

cd "$WORKSPACE_ROOT"
mkdir -p "$CLEAN_HOME" "$LASTDB_HOME_RUN"

log "checking default lastdb_node dependency graph for embedder/provider crates"
cargo tree -p lastdb_node --edges normal --target all --locked > "$DEPS_FILE"
if rg -i '(^|[[:space:]])(fastembed|hf-hub|huggingface|ort|onnx|candle|tokenizers|ollama|anthropic)([[:space:]]|$)' "$DEPS_FILE" >/dev/null; then
  rg -n -i '(fastembed|hf-hub|huggingface|ort|onnx|candle|tokenizers|ollama|anthropic)' "$DEPS_FILE" >&2 || true
  fail "default lastdb_node dependency graph contains an embedder/provider dependency"
fi

log "building default lastdbd binary"
BUILD_JOBS="${CARGO_BUILD_JOBS:-1}"
log "using CARGO_BUILD_JOBS=$BUILD_JOBS and disabling RUSTC_WRAPPER for deterministic local validation"
CARGO_BUILD_JOBS="$BUILD_JOBS" RUSTC_WRAPPER= \
  cargo build --locked -p lastdb_node --bin lastdbd >>"$LOG_FILE" 2>&1
DAEMON_BIN="$WORKSPACE_ROOT/target/debug/lastdbd"
[ -x "$DAEMON_BIN" ] || fail "built daemon binary missing at $DAEMON_BIN"

log "starting lastdbd with clean HOME and AI/provider env removed"
(
  cd "$RUN_ROOT"
  env \
    -u ANTHROPIC_API_KEY \
    -u ANTHROPIC_AUTH_TOKEN \
    -u OLLAMA_HOST \
    -u OLLAMA_MODELS \
    -u FASTEMBED_CACHE_PATH \
    -u HF_HOME \
    -u HF_HUB_CACHE \
    -u HUGGINGFACE_HUB_CACHE \
    HOME="$CLEAN_HOME" \
    LASTDB_HOME="$LASTDB_HOME_RUN" \
    FOLDDB_HOME="$LASTDB_HOME_RUN" \
    FOLDDB_DISABLE_KEYCHAIN=1 \
    RUST_LOG=info \
    "$DAEMON_BIN" --data-dir "$LASTDB_HOME_RUN"
) >"$SERVER_LOG" 2>&1 &
DAEMON_PID="$!"

python3 - "$SOCKET" "$DAEMON_PID" <<'PY'
import os
import signal
import sys
import time

socket_path = sys.argv[1]
pid = int(sys.argv[2])
deadline = time.monotonic() + 20.0
while time.monotonic() < deadline:
    try:
        os.kill(pid, 0)
    except OSError as exc:
        raise SystemExit(f"daemon exited before socket appeared: {exc}") from exc
    if os.path.exists(socket_path):
        raise SystemExit(0)
    time.sleep(0.05)
raise SystemExit(f"timed out waiting for {socket_path}")
PY

curl_socket() {
  curl --unix-socket "$SOCKET" -fsS "$@"
}

log "checking socket health"
curl_socket "http://lastdb/health" > "$REPORT_DIR/health.txt"
jq -e '.status == "ok"' "$REPORT_DIR/health.txt" >/dev/null \
  || fail "health endpoint did not return status=ok"

log "declaring a validation schema over the owner socket"
cat > "$REPORT_DIR/declare-request.json" <<'JSON'
{
  "namespace": "validation",
  "schema": {
    "name": "NoEmbedderRecord",
    "descriptive_name": "No Embedder Record",
    "schema_type": "HashRange",
    "key": {
      "hash_field": "bucket",
      "range_field": "id"
    },
    "fields": ["bucket", "id", "title", "body"],
    "field_descriptions": {
      "bucket": "hash partition for the validation record",
      "id": "range key id",
      "title": "short title",
      "body": "body text proving no embedder is required"
    }
  }
}
JSON
curl_socket \
  -H 'Content-Type: application/json' \
  --data-binary "@$REPORT_DIR/declare-request.json" \
  "http://lastdb/api/schemas/declare" > "$REPORT_DIR/declare.json"
jq -e '.ok == true and .schema_name == "validation/NoEmbedderRecord"' "$REPORT_DIR/declare.json" >/dev/null \
  || fail "schema declare did not return validation/NoEmbedderRecord (body: $(head -c 400 "$REPORT_DIR/declare.json" 2>/dev/null || true))"
# Catalog register persists under identity hash — never hardcode namespace/Name
# for mutation/query (same class as release fresh-install 404, 2026-07-21).
SCHEMA_REF="$(jq -r '.canonical // .identity_hash // empty' "$REPORT_DIR/declare.json")"
[ -n "$SCHEMA_REF" ] && [ "$SCHEMA_REF" != "null" ] \
  || fail "schema declare missing canonical/identity_hash"

log "writing a record over the owner socket"
cat > "$REPORT_DIR/mutation-request.json" <<JSON
{
  "type": "mutation",
  "schema": "$SCHEMA_REF",
  "fields_and_values": {
    "bucket": "release",
    "id": "default-no-ai",
    "title": "Default no-embedder validation",
    "body": "LastDB writes and reads normal records without Ollama, Anthropic, FastEmbed, or a model cache."
  },
  "key_value": {
    "hash": "release",
    "range": "default-no-ai"
  },
  "mutation_type": "create"
}
JSON
curl_socket \
  -H 'Content-Type: application/json' \
  --data-binary "@$REPORT_DIR/mutation-request.json" \
  "http://lastdb/api/mutation" > "$REPORT_DIR/mutation.json"
jq -e '.ok == true and .success == true' "$REPORT_DIR/mutation.json" >/dev/null \
  || fail "mutation did not succeed (body: $(head -c 400 "$REPORT_DIR/mutation.json" 2>/dev/null || true))"

log "querying the record back over the owner socket"
cat > "$REPORT_DIR/query-request.json" <<JSON
{
  "schema_name": "$SCHEMA_REF",
  "fields": ["bucket", "id", "title", "body"],
  "filter": {
    "HashRangeKey": {
      "hash": "release",
      "range": "default-no-ai"
    }
  }
}
JSON
curl_socket \
  -H 'Content-Type: application/json' \
  --data-binary "@$REPORT_DIR/query-request.json" \
  "http://lastdb/api/query" > "$REPORT_DIR/query.json"
jq -e '.ok == true and (.results | length) == 1 and (.results[0].fields.body | contains("without Ollama, Anthropic, FastEmbed"))' "$REPORT_DIR/query.json" >/dev/null \
  || fail "query did not return the no-embedder validation record"

log "checking native vector-search route is retired with 404 Not Found"
# Owner-wide native-index search is intentionally absent from Mini; this also
# rejects the stale semantic-off 503 stub and empty 200 + ok:true false success.
SEARCH_CODE="$(
  curl --unix-socket "$SOCKET" -sS -o "$REPORT_DIR/native-search.body" -w '%{http_code}' \
    "http://lastdb/api/native-index/search?q=default-no-ai" \
    || true
)"
if [ "$SEARCH_CODE" != "404" ]; then
  fail "native-index search expected HTTP 404 Not Found after route retirement, got ${SEARCH_CODE:-transport-error} (body: $(head -c 400 "$REPORT_DIR/native-search.body" 2>/dev/null || true))"
fi
if ! grep -qi 'not found' "$REPORT_DIR/native-search.body"; then
  fail "404 body missing Not Found marker (body: $(head -c 400 "$REPORT_DIR/native-search.body" 2>/dev/null || true))"
fi

log "checking no FastEmbed/HuggingFace cache or model download evidence was created"
if find "$RUN_ROOT" \( -name '.fastembed_cache' -o -iname '*huggingface*' -o -iname '*hf_cache*' \) -print | tee "$REPORT_DIR/cache-findings.txt" | rg . >/dev/null; then
  fail "throwaway run root contains embedder/model cache artifacts"
fi
if rg -i 'fastembed|huggingface|hf-hub|download|ollama|anthropic' "$SERVER_LOG" > "$REPORT_DIR/model-log-grep.txt"; then
  fail "daemon log contains embedder/provider/model-download evidence"
fi

write_report "PASS"
log "PASS: report written to $REPORT_FILE"
