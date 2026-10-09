#!/usr/bin/env bash
set -euo pipefail

fail() {
  echo "RED restore-probe: $*" >&2
  exit 1
}

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LASTDBD="${LASTDBD:-$ROOT/target/debug/lastdbd}"
LASTDB="${LASTDB:-$ROOT/target/debug/lastdb}"
PROBE="${PROBE:-$ROOT/target/debug/lastdb_restore_probe}"
INVITE_CODE="${LASTDB_RESTORE_PROBE_INVITE_CODE:-}"
INVITE_FILE="${LASTDB_RESTORE_PROBE_INVITE_FILE:-}"
API_ENV="${LASTDB_RESTORE_PROBE_ENV:-dev}"

if [[ -z "$INVITE_CODE" && -n "$INVITE_FILE" ]]; then
  INVITE_CODE="$(tr -d '[:space:]' < "$INVITE_FILE")"
fi
[[ -n "$INVITE_CODE" ]] || fail "set LASTDB_RESTORE_PROBE_INVITE_CODE or LASTDB_RESTORE_PROBE_INVITE_FILE"

for bin in "$LASTDBD" "$LASTDB" "$PROBE" curl jq python3 pgrep; do
  command -v "$bin" >/dev/null 2>&1 || [[ -x "$bin" ]] || fail "missing executable: $bin"
done

# Short prefix + short home-dir names: the daemon's UDS socket lives at
# "$HOME/data/folddb.sock" and sockaddr_un caps at 103 usable bytes. A
# TMPDIR-default WORK_ROOT ("$TMPDIR/lastdb-restore-probe.XXXXXX/source-home")
# reproducibly overflows that limit on macOS, where $TMPDIR is itself a long
# per-session /var/folders/... path — the daemon then exits before its socket
# appears, with no diagnostic beyond "No such process" once the workdir is
# cleaned up. 2026-07-20 incident: this bit every run until diagnosed.
WORK_ROOT="${LASTDB_RESTORE_PROBE_WORKDIR:-$(mktemp -d "${TMPDIR:-/tmp}/lrp.XXXXXX")}"
REPORT_DIR="$WORK_ROOT/report"
HOME_A="$WORK_ROOT/src"
HOME_B="$WORK_ROOT/dst"
LOG_A="$REPORT_DIR/source-lastdbd.log"
LOG_B="$REPORT_DIR/restored-lastdbd.log"
mkdir -p "$REPORT_DIR" "$HOME_A" "$HOME_B"

# Fail fast with a clear message rather than a cryptic daemon exit.
SOCK_SUFFIX="/data/folddb.sock"
for h in "$HOME_A" "$HOME_B"; do
  sock_len=$(( ${#h} + ${#SOCK_SUFFIX} ))
  [[ "$sock_len" -le 100 ]] || fail \
    "workdir too deep for a Unix socket path ($h$SOCK_SUFFIX is $sock_len bytes, limit ~103) — set a shorter LASTDB_RESTORE_PROBE_WORKDIR"
done

# Capture the operator HOME before any per-daemon `env HOME=...` so the
# isolation census always watches the live ~/.lastdb primary, never a
# throwaway probe home or a repair/smoke copy.
USER_HOME="${HOME}"

DAEMON_A=""
DAEMON_B=""
cleanup() {
  for pid in "$DAEMON_A" "$DAEMON_B"; do
    if [[ -n "$pid" ]]; then
      kill -TERM "$pid" >/dev/null 2>&1 || true
      wait "$pid" >/dev/null 2>&1 || true
    fi
  done
  if [[ "${LASTDB_RESTORE_PROBE_KEEP_WORKDIR:-0}" != "1" ]]; then
    rm -rf "$WORK_ROOT"
  fi
}
trap cleanup EXIT

primary_pids() {
  # Only the process serving the live ~/.lastdb (or ~/.folddb) socket —
  # never machine-wide pgrep. Repair copies, smoke COWs, safe-upgrade
  # helpers, and this probe's own daemons must not flip the isolation bar
  # (2026-08-17: SOP went RED while pid 35303 on ~/.lastdb was unchanged).
  env HOME="$USER_HOME" "$PROBE" --write-primary-pids "$1"
}

wait_socket() {
  local socket="$1"
  local pid="$2"
  python3 - "$socket" "$pid" <<'PY'
import os, sys, time
socket_path, pid = sys.argv[1], int(sys.argv[2])
deadline = time.monotonic() + 60.0
while time.monotonic() < deadline:
    try:
        os.kill(pid, 0)
    except OSError as exc:
        raise SystemExit(f"daemon exited before socket appeared: {exc}")
    if os.path.exists(socket_path):
        raise SystemExit(0)
    time.sleep(0.05)
raise SystemExit(f"timed out waiting for {socket_path}")
PY
}

curl_socket() {
  local socket="$1"
  shift
  curl --unix-socket "$socket" -fsS "$@"
}

primary_pids "$REPORT_DIR/primary-pids-before.txt"

printf '%s' "$INVITE_CODE" | (
  cd "$ROOT"
  env HOME="$HOME_A" FOLDDB_DISABLE_KEYCHAIN=1 "$LASTDB" --data-dir "$HOME_A" \
    connect --env "$API_ENV" --invite-code-stdin >"$REPORT_DIR/connect.out" 2>"$REPORT_DIR/connect.err"
)
printf 'ok\n' > "$HOME_A/.bootstrap_done"
chmod 600 "$HOME_A/.bootstrap_done"

(
  cd "$ROOT"
  env HOME="$HOME_A" FOLDDB_DISABLE_KEYCHAIN=1 EXEMEM_ENV="$API_ENV" LASTDB_ENGINE=laststore RUST_LOG="${RUST_LOG:-info}" \
    "$LASTDBD" --data-dir "$HOME_A" </dev/null
) >>"$LOG_A" 2>&1 &
DAEMON_A="$!"
SOCKET_A="$HOME_A/data/folddb.sock"
wait_socket "$SOCKET_A" "$DAEMON_A"

# The live Schema Service requires a stable description for every field used
# in semantic matching. Keep this fixture aligned with the fresh-install probe.
cat > "$REPORT_DIR/declare-request.json" <<'JSON'
{
  "namespace": "restore_probe",
  "schema": {
    "name": "RestoreProbeRecord",
    "descriptive_name": "Restore Probe Record",
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
curl_socket "$SOCKET_A" -H 'Content-Type: application/json' \
  --data-binary "@$REPORT_DIR/declare-request.json" \
  "http://lastdb/api/schemas/declare" > "$REPORT_DIR/declare.json"
jq -e '.ok == true' "$REPORT_DIR/declare.json" >/dev/null

# Under LocalMint resolution (the expected path when no live schema resolver
# is reachable), the daemon persists the schema content-addressed under its
# identity hash (`GET /api/schemas` lists it by hash, not by the namespaced
# name) — the same identity_hash addressing catalog-reuse/compose already
# use. Every subsequent mutation/query MUST reference the schema by this
# canonical value, never by the human-readable "namespace/Name" string, or
# every call 404s with "Schema not found: restore_probe/RestoreProbeRecord"
# (2026-07-20 incident: this blocked every prior live-run attempt).
SCHEMA_REF="$(jq -r '.canonical // .identity_hash' "$REPORT_DIR/declare.json")"
[[ -n "$SCHEMA_REF" && "$SCHEMA_REF" != "null" ]] || fail "declare response missing canonical/identity_hash"

python3 - "$REPORT_DIR" "$SCHEMA_REF" <<'PY'
import hashlib, json, pathlib, sys
report = pathlib.Path(sys.argv[1])
schema_ref = sys.argv[2]
records = []
for i in range(1, 7):
    body = (
        f"restore probe fixture row {i}: cloud backup must download, decrypt, "
        f"replay, and serve query reads from source atoms."
    )
    records.append({
        "bucket": "restore-probe",
        "id": f"row-{i:02d}",
        "title": f"Restore probe row {i}",
        "body": body,
        "body_sha256": hashlib.sha256(body.encode()).hexdigest(),
    })
fixture = {
    "semantic_phrase": "download, decrypt, replay",
    "records": [{k: v for k, v in r.items() if k != "body"} for r in records],
}
(report / "fixture.json").write_text(json.dumps(fixture, indent=2))
for record in records:
    payload = {
        "type": "mutation",
        "schema": schema_ref,
        "fields_and_values": {
            "bucket": record["bucket"],
            "id": record["id"],
            "title": record["title"],
            "body": record["body"],
        },
        "key_value": {"hash": record["bucket"], "range": record["id"]},
        "mutation_type": "create",
    }
    (report / f"mutation-{record['id']}.json").write_text(json.dumps(payload, indent=2))
PY

for request in "$REPORT_DIR"/mutation-row-*.json; do
  out="$REPORT_DIR/$(basename "$request" .json)-response.json"
  curl_socket "$SOCKET_A" -H 'Content-Type: application/json' \
    --data-binary "@$request" \
    "http://lastdb/api/mutation" > "$out"
  jq -e '.ok == true and .success == true' "$out" >/dev/null
done

cat > "$REPORT_DIR/query-request.json" <<JSON
{
  "schema_name": "$SCHEMA_REF",
  "fields": ["bucket", "id", "title", "body"],
  "filter": { "HashRangeRange": { "hash": "restore-probe", "start": "row-00", "end": "row-99" } }
}
JSON
curl_socket "$SOCKET_A" -H 'Content-Type: application/json' \
  --data-binary "@$REPORT_DIR/query-request.json" \
  "http://lastdb/api/query" > "$REPORT_DIR/source-query.json"
jq -e '.ok == true and (.results | length) == 6' "$REPORT_DIR/source-query.json" >/dev/null

env HOME="$HOME_A" FOLDDB_DISABLE_KEYCHAIN=1 "$LASTDB" --data-dir "$HOME_A" \
  cloud snapshot --json > "$REPORT_DIR/snapshot.json"
jq -e '.ok == true and (.report.counter >= 1) and (.report.chunks_referenced >= 1)' \
  "$REPORT_DIR/snapshot.json" >/dev/null

env HOME="$HOME_A" FOLDDB_DISABLE_KEYCHAIN=1 "$LASTDB" --data-dir "$HOME_A" \
  restore --into "$HOME_B" --env "$API_ENV" --json > "$REPORT_DIR/restore.json"
jq -e '.counter >= 1 and .chunks_installed >= 1 and .bytes_installed >= 1' \
  "$REPORT_DIR/restore.json" >/dev/null

(
  cd "$ROOT"
  env HOME="$HOME_B" FOLDDB_DISABLE_KEYCHAIN=1 EXEMEM_ENV="$API_ENV" LASTDB_ENGINE=laststore RUST_LOG="${RUST_LOG:-info}" \
    "$LASTDBD" --data-dir "$HOME_B" </dev/null
) >>"$LOG_B" 2>&1 &
DAEMON_B="$!"
SOCKET_B="$HOME_B/data/folddb.sock"
wait_socket "$SOCKET_B" "$DAEMON_B"

curl_socket "$SOCKET_B" -H 'Content-Type: application/json' \
  --data-binary "@$REPORT_DIR/query-request.json" \
  "http://lastdb/api/query" > "$REPORT_DIR/restored-query.json"
jq -e '.ok == true and (.results | length) == 6' "$REPORT_DIR/restored-query.json" >/dev/null

# Mini has no native index. Capture the honest 503 (do not use -f).
# Owner 503 body is the plain search_plane_required string, not JSON.
# ok:true hits here are a strip-native-index regression, not GREEN.
curl --unix-socket "$SOCKET_B" -sS --max-time 30 \
  -o "$REPORT_DIR/native-search.body" -w '%{http_code}\n' \
  "http://lastdb/api/native-index/search?q=download%20decrypt%20replay" \
  > "$REPORT_DIR/native-search.status" || true
python3 - "$REPORT_DIR" <<'PY'
import json, pathlib, sys
report = pathlib.Path(sys.argv[1])
raw = (report / "native-search.body").read_text() if (report / "native-search.body").exists() else ""
status = (report / "native-search.status").read_text().strip() if (report / "native-search.status").exists() else ""
try:
    payload = json.loads(raw) if raw.strip() else {}
except json.JSONDecodeError:
    payload = {"ok": False, "message": raw.strip()}
if isinstance(payload, dict):
    payload.setdefault("ok", False)
    payload.setdefault("http_status", int(status) if status.isdigit() else None)
    if "search_plane_required" in raw or status == "503":
        payload["search_plane_required"] = True
else:
    payload = {"ok": False, "message": raw.strip(), "search_plane_required": True}
(report / "native-search.json").write_text(json.dumps(payload, indent=2) + "\n")
PY

# The staged probe runs a valid control restore and a same-length corrupt-chunk
# restore through the production restore function, using only a bounded local
# fixture and loopback auth/object endpoints. No Cargo or cloud mutation occurs.
"$PROBE" --prove-corrupt-restore "$REPORT_DIR/red-path.json" > "$REPORT_DIR/red-path.log" 2>&1

for pid_var in DAEMON_A DAEMON_B; do
  pid="${!pid_var}"
  if [[ -n "$pid" ]]; then
    kill -TERM "$pid" >/dev/null 2>&1 || true
    wait "$pid" >/dev/null 2>&1 || true
    printf -v "$pid_var" ''
  fi
done
primary_pids "$REPORT_DIR/primary-pids-after.txt"

"$PROBE" \
  --source-home "$HOME_A" \
  --restored-home "$HOME_B" \
  --identity-key "$HOME_A/identity.key" \
  --fixture "$REPORT_DIR/fixture.json" \
  --source-query "$REPORT_DIR/source-query.json" \
  --restored-query "$REPORT_DIR/restored-query.json" \
  --semantic-search "$REPORT_DIR/native-search.json" \
  --snapshot-report "$REPORT_DIR/snapshot.json" \
  --restore-report "$REPORT_DIR/restore.json" \
  --primary-pids-before "$REPORT_DIR/primary-pids-before.txt" \
  --primary-pids-after "$REPORT_DIR/primary-pids-after.txt" \
  --red-path-log "$REPORT_DIR/red-path.json" \
  --report-out "$REPORT_DIR/probe-report.json" | tee "$REPORT_DIR/probe.out"

echo "GREEN restore-probe report_dir=$REPORT_DIR"
