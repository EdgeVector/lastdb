#!/usr/bin/env bash
# Terminal proof for design-lastdb-delete-converge-then-reclaim.
#
# The primary home is a read-only copy source. Every mutation and daemon boot
# uses an APFS clone under LASTDB_DELETE_PROOF_ROOT. The proof refuses a work
# home or socket that resolves to the primary.
#
# Exit 0: all bars passed. Exit non-zero: a bar failed or could not run.
# The final stdout line is always `PROOF: PASS|FAIL ...`.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
PRIMARY_HOME="${LASTDB_PRIMARY_HOME:-$HOME/.lastdb}"
# APFS is macOS-only, and lastdbd requires a short data path for its Unix
# sockets. Keep the default path short enough for both folddb socket names.
PROOF_ROOT="${LASTDB_DELETE_PROOF_ROOT:-/private/tmp/lastdb-delete-proof}"
RUN_ID="${LASTDB_DELETE_PROOF_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)-$$}"
RUN_DIR="$PROOF_ROOT/runs/$RUN_ID"
WORK_HOME="${LASTDB_DELETE_PROOF_HOME:-$RUN_DIR/home}"
REPORT="$RUN_DIR/proof.json"
DAEMON_LOG="$RUN_DIR/lastdbd.log"
LASTDBD_OVERRIDE="${LASTDB_DELETE_PROOF_LASTDBD:-}"
LASTDB_OVERRIDE="${LASTDB_DELETE_PROOF_LASTDB:-}"
LASTDBD="${LASTDBD_OVERRIDE:-$ROOT/target/debug/lastdbd}"
LASTDB="${LASTDB_OVERRIDE:-$ROOT/target/debug/lastdb}"
SCHEMA="${LASTDB_DELETE_PROOF_SCHEMA:-1ef2e7a3a802dbacdf37f16e26548085741317e9b2d6656d6bc8201347461459}"
KEEP="${LASTDB_DELETE_PROOF_KEEP:-0}"
# A trap cannot run on SIGKILL, and `rm -rf` on a 21 GB clone does not finish
# inside a 30-second kill grace, so trap-only reclaim leaks a whole clone every
# time a caller timeboxes this harness. Two bounds replace that single hope:
# every run sweeps the clones its predecessors left behind, and no run clones
# onto a volume that is already low. KEEP_CLONES retains the newest owned
# clones so the previous run stays inspectable.
KEEP_CLONES="${LASTDB_DELETE_PROOF_KEEP_CLONES:-1}"
MIN_FREE_GIB="${LASTDB_DELETE_PROOF_MIN_FREE_GIB:-40}"
REPAIR_MAX_OPS="${LASTDB_DELETE_PROOF_REPAIR_MAX_OPS:-8}"
REPAIR_TIP_PAGE="${LASTDB_DELETE_PROOF_REPAIR_TIP_PAGE:-32}"
REPAIR_AUDIT_LIMIT="${LASTDB_DELETE_PROOF_REPAIR_AUDIT_LIMIT:-8}"
REQUEST_TIMEOUT_SECS="${LASTDB_DELETE_PROOF_REQUEST_TIMEOUT_SECS:-600}"
CONVERGE_TIMEOUT_SECS="${LASTDB_DELETE_PROOF_CONVERGE_TIMEOUT_SECS:-600}"
# The tail workload makes the persist lane non-idle before the SIGKILL.
# Its size is a measurement input, not a constant: four 4 MiB batches against
# a debug daemon on a real-data clone overran the old hard-coded 300s curl
# budget and returned zero bytes, so the lane gauge correctly read idle and
# the bar blamed the lane. Keep every dimension overridable so a sizing run
# can tune it without a code change.
TAIL_WORKERS="${LASTDB_DELETE_PROOF_TAIL_WORKERS:-4}"
TAIL_ROWS="${LASTDB_DELETE_PROOF_TAIL_ROWS:-8}"
TAIL_PAYLOAD_BYTES="${LASTDB_DELETE_PROOF_TAIL_PAYLOAD_BYTES:-524288}"
# A row title is one atom's content, and atoms are capped. The default cap is
# 64 KiB and the absolute maximum is 1 MiB (LASTDB_MAX_ATOM_CONTENT_BYTES,
# fold_db/docs/ATOM_CONTENT_SIZE_LIMIT.md). A 1 MiB title measured 1048578 bytes
# of atom content — the JSON quotes count — and the node answered 413
# atom_content_too_large. State the cap on boot and keep the payload under it.
TAIL_ATOM_CONTENT_BYTES="${LASTDB_DELETE_PROOF_TAIL_ATOM_CONTENT_BYTES:-1048576}"
# How many times the probe may halve the batch before it gives up.
TAIL_CALIBRATION_ATTEMPTS="${LASTDB_DELETE_PROOF_TAIL_CALIBRATION_ATTEMPTS:-8}"
# The probe runs alone; the workers run concurrently. A batch that the probe
# proved deferring at 4 x 512 KiB still wrote through 19 times out of 19 once
# four workers sent it at once (measured 2026-09-07, run 20260907T120226Z-72367,
# probe write_throughs 0->0 and final count 19). Concurrency moves the charge,
# so the workers take a stated fraction of the proven size rather than the size
# itself.
TAIL_WORKER_MARGIN_DIVISOR="${LASTDB_DELETE_PROOF_TAIL_WORKER_MARGIN_DIVISOR:-2}"
# A batch whose deferred bytes reach the write-through threshold skips the
# deferred window and persists inline (`should_write_through`), so it can never
# put a byte in the lane this bar watches. The old 128 x 32 KiB tail was 4 MiB
# of payload against a 4 MiB default threshold: the one shape guaranteed to
# leave the gauge at zero. Measured 2026-09-07 — the single batch that did ACK
# raised `deferred_write_throughs` to 1 and `deferred_persist_bytes` stayed 0.
# The harness now BOOTS the node with the threshold instead of inheriting it,
# so the sizing has a number it can trust.
TAIL_WRITE_THROUGH_BYTES="${LASTDB_DELETE_PROOF_TAIL_WRITE_THROUGH_BYTES:-4194304}"
# The lane charges atom bytes plus idempotency, search and share-prefix bytes,
# none of which the payload controls. Spend only this share of the threshold on
# payload so the unmeasured remainder cannot push the batch over.
TAIL_BATCH_SHARE_PERCENT="${LASTDB_DELETE_PROOF_TAIL_BATCH_SHARE_PERCENT:-50}"
# Sustained pressure writes real rows into the clone. Bound how many batches one
# worker may land so a lane that never goes non-idle cannot fill the volume
# while it waits out its deadline.
TAIL_MAX_ITERATIONS="${LASTDB_DELETE_PROOF_TAIL_MAX_ITERATIONS:-64}"
# Every other write in this harness uses REQUEST_TIMEOUT_SECS. The tail curl
# was the one request pinned to 300s, for no stated reason.
TAIL_REQUEST_TIMEOUT_SECS="${LASTDB_DELETE_PROOF_TAIL_REQUEST_TIMEOUT_SECS:-$REQUEST_TIMEOUT_SECS}"
TAIL_TIMEOUT_SECS="${LASTDB_DELETE_PROOF_TAIL_TIMEOUT_SECS:-600}"
# The batch size is MEASURED on the clone, not assumed. Before the workers
# start, the harness posts one small calibration batch of the same row shape and
# times its ACK. The per-row cost from that probe sizes the real batch so its
# projected ACK fits TAIL_ACK_BUDGET_SECS. A guessed 128-row batch is what
# failed on 2026-09-07: the ACK cost of a debug daemon over a real-data clone
# exceeded the curl budget, every request returned zero bytes, and the lane
# gauge correctly read idle.
TAIL_CALIBRATION_ROWS="${LASTDB_DELETE_PROOF_TAIL_CALIBRATION_ROWS:-1}"
TAIL_ACK_BUDGET_SECS="${LASTDB_DELETE_PROOF_TAIL_ACK_BUDGET_SECS:-$(( TAIL_REQUEST_TIMEOUT_SECS / 2 ))}"
TAIL_ROWS_EFFECTIVE="$TAIL_ROWS"
TAIL_ROWS_BUDGET_FIT="$TAIL_ROWS"
TAIL_ROWS_WRITE_THROUGH_FIT="$TAIL_ROWS"
TAIL_CALIBRATION_SECS=0
TAIL_ACK_SECS_PER_ROW=0
TAIL_ITERATIONS_TOTAL=0
PRIMARY_SOCKET=""
SOCKET=""
NODE_PID=""
TAIL_PIDS=()
VERDICT="FAIL"
DETAIL="phase=setup"
CLONE_ERR=""
WORK_HOME_OWNED=0

usage() {
  cat <<'EOF'
Usage: fold_db/scripts/proof-delete-converge-then-reclaim.sh

Build the current lastdbd, clone LASTDB_PRIMARY_HOME with APFS copy-on-write,
and prove delete converge on the clone. The script never mutates the primary.

Useful overrides:
  LASTDB_DELETE_PROOF_LASTDBD=/path/to/lastdbd
  LASTDB_DELETE_PROOF_LASTDB=/path/to/lastdb
  LASTDB_DELETE_PROOF_ROOT=/safe/output/root
  LASTDB_DELETE_PROOF_KEEP=1
  LASTDB_DELETE_PROOF_KEEP_CLONES=1
  LASTDB_DELETE_PROOF_MIN_FREE_GIB=40
  LASTDB_DELETE_PROOF_REPAIR_MAX_OPS=8
  LASTDB_DELETE_PROOF_REQUEST_TIMEOUT_SECS=600
  LASTDB_DELETE_PROOF_CONVERGE_TIMEOUT_SECS=600
  LASTDB_DELETE_PROOF_TAIL_WORKERS=4
  LASTDB_DELETE_PROOF_TAIL_CALIBRATION_ROWS=4
  LASTDB_DELETE_PROOF_TAIL_ACK_BUDGET_SECS=300
  LASTDB_DELETE_PROOF_TAIL_WRITE_THROUGH_BYTES=4194304
  LASTDB_DELETE_PROOF_TAIL_BATCH_SHARE_PERCENT=50
  LASTDB_DELETE_PROOF_TAIL_MAX_ITERATIONS=64
  LASTDB_DELETE_PROOF_TAIL_ATOM_CONTENT_BYTES=1048576
  LASTDB_DELETE_PROOF_TAIL_CALIBRATION_ATTEMPTS=8
  LASTDB_DELETE_PROOF_TAIL_WORKER_MARGIN_DIVISOR=2
  LASTDB_DELETE_PROOF_TAIL_ROWS=128
  LASTDB_DELETE_PROOF_TAIL_PAYLOAD_BYTES=32768
  LASTDB_DELETE_PROOF_TAIL_REQUEST_TIMEOUT_SECS=600
  LASTDB_DELETE_PROOF_TAIL_TIMEOUT_SECS=600
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi
if [[ "$#" -ne 0 ]]; then
  usage >&2
  exit 64
fi

log() {
  printf 'delete-converge-proof: %s\n' "$*" >&2
}

canonical_existing() {
  (cd "$1" && pwd -P)
}

canonical_planned() {
  local input="$1" normalized cursor leaf suffix="" existing
  [[ "$input" == /* ]] || return 1
  normalized="$(perl -e '
    my @parts;
    for my $part (split m{/+}, shift) {
      next if $part eq q{} || $part eq q{.};
      if ($part eq q{..}) {
        pop @parts or die "path escapes root\n";
      } else {
        push @parts, $part;
      }
    }
    print q{/}, join q{/}, @parts;
  ' "$input")" || return 1
  cursor="$normalized"
  while [[ ! -e "$cursor" ]]; do
    [[ "$cursor" != "/" ]] || return 1
    leaf="${cursor##*/}"
    suffix="/$leaf$suffix"
    cursor="${cursor%/*}"
    [[ -n "$cursor" ]] || cursor="/"
  done
  existing="$(canonical_existing "$cursor")" || return 1
  printf '%s%s\n' "$existing" "$suffix"
}

is_same_or_child() {
  local path="$1" parent="$2"
  [[ "$path" == "$parent" || "$path" == "$parent/"* ]]
}

socket_path_fits() {
  local socket_path="$1" path_bytes
  path_bytes="$(LC_ALL=C printf '%s' "$socket_path" | wc -c | tr -d ' ')"
  # sockaddr_un holds 104 bytes. The daemon needs four bytes for its atomic
  # temporary sibling, so the final socket name can use at most 99 bytes.
  [[ "$path_bytes" -le 99 ]]
}

fail() {
  DETAIL="$*"
  log "FAIL $DETAIL"
  return 1
}

stop_node() {
  if [[ -n "$NODE_PID" ]] && kill -0 "$NODE_PID" 2>/dev/null; then
    kill -TERM "$NODE_PID" 2>/dev/null || true
    for _ in $(seq 1 300); do
      if ! kill -0 "$NODE_PID" 2>/dev/null; then
        break
      fi
      perl -e 'select undef, undef, undef, 0.1'
    done
    if kill -0 "$NODE_PID" 2>/dev/null; then
      log "isolated node did not stop in 30 seconds; sending SIGKILL"
      kill -KILL "$NODE_PID" 2>/dev/null || true
    fi
    wait "$NODE_PID" 2>/dev/null || true
  fi
  NODE_PID=""
}

# A clone is reclaimable only when it sits under the proof root, does not
# overlap the primary home in either direction, and carries a proof run's own
# ownership marker. `$2` pins the marker to one run; empty accepts any proof
# run, which is what lets a later run reclaim an earlier run's leak.
clone_is_reclaimable() {
  local candidate="$1" expect="$2"
  local cand_abs proof_abs primary_abs owner_value
  cand_abs="$(canonical_existing "$candidate" 2>/dev/null || true)"
  proof_abs="$(canonical_existing "$PROOF_ROOT" 2>/dev/null || true)"
  primary_abs="$(canonical_existing "$PRIMARY_HOME" 2>/dev/null || true)"
  [[ -n "$cand_abs" && -n "$proof_abs" && -n "$primary_abs" ]] || return 1
  is_same_or_child "$cand_abs" "$proof_abs" || return 1
  ! is_same_or_child "$cand_abs" "$primary_abs" || return 1
  ! is_same_or_child "$primary_abs" "$cand_abs" || return 1
  owner_value="$(cat "$cand_abs/.hold-delete-converge-proof" 2>/dev/null || true)"
  case "$owner_value" in
    "delete-converge proof run="*) ;;
    *) return 1 ;;
  esac
  if [[ -n "$expect" && "$owner_value" != "$expect" ]]; then
    return 1
  fi
  return 0
}

# Reclaim THIS run's clone. Called once as the last normal step and again from
# the EXIT trap, so a kill after the checkpoint has nothing left to leak.
reclaim_work_home() {
  [[ "$WORK_HOME_OWNED" == "1" && -n "$WORK_HOME" && -d "$WORK_HOME" ]] || return 0
  if clone_is_reclaimable "$WORK_HOME" "delete-converge proof run=$RUN_ID"; then
    rm -rf "$WORK_HOME"
    WORK_HOME_OWNED=0
  else
    log "kept work home because cleanup ownership or primary exclusion was not proven: $WORK_HOME"
  fi
}

cleanup() {
  local pid
  set +e
  for pid in "${TAIL_PIDS[@]+"${TAIL_PIDS[@]}"}"; do
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  done
  stop_node
  if [[ "$KEEP" != "1" && "$WORK_HOME_OWNED" == "1" && -n "$WORK_HOME" && -d "$WORK_HOME" ]]; then
    reclaim_work_home
  elif [[ "$KEEP" != "1" && -n "$WORK_HOME" && -d "$WORK_HOME" ]]; then
    log "kept work home because this run did not create it: $WORK_HOME"
  elif [[ "$KEEP" == "1" && -d "$WORK_HOME" ]]; then
    log "kept clone: $WORK_HOME"
  fi
}

# A run directory whose name carries a live pid belongs to a harness that is
# still running. Only the default RUN_ID shape encodes one, so the pid test is
# gated on that shape rather than on any trailing number.
clone_owner_is_live() {
  local run_dir="$1" base pid socket
  base="${run_dir##*/}"
  if [[ "$base" =~ ^[0-9]{8}T[0-9]{6}Z-([0-9]+)$ ]]; then
    pid="${BASH_REMATCH[1]}"
    if kill -0 "$pid" 2>/dev/null; then
      return 0
    fi
  fi
  socket="$run_dir/home/data/folddb.sock"
  if [[ -S "$socket" ]] \
    && curl -fsS --max-time 2 --unix-socket "$socket" \
      -H 'Host: localhost' -H 'X-LastDB-Client: delete-converge-proof' \
      http://x/health >/dev/null 2>&1; then
    return 0
  fi
  return 1
}

# Reclaim the clones earlier runs leaked. This is the bound that survives every
# signal, because the next run performs it. It removes only `home/` and leaves
# each run's small JSON artifacts, which are the evidence a card cites.
sweep_stale_clones() {
  local runs_dir="$PROOF_ROOT/runs" kept=0 run_dir candidate
  [[ -d "$runs_dir" ]] || return 0
  while IFS= read -r run_dir; do
    [[ -n "$run_dir" ]] || continue
    [[ "${run_dir##*/}" != "$RUN_ID" ]] || continue
    candidate="$run_dir/home"
    [[ -d "$candidate" ]] || continue
    [[ "$candidate" != "$WORK_HOME" ]] || continue
    if clone_owner_is_live "$run_dir"; then
      log "sweep kept a clone whose owner is still live: $candidate"
      continue
    fi
    if ! clone_is_reclaimable "$candidate" ""; then
      log "sweep kept an unowned or unsafe directory: $candidate"
      continue
    fi
    if [[ "$kept" -lt "$KEEP_CLONES" ]]; then
      kept=$((kept + 1))
      log "sweep retained a recent clone: $candidate"
      continue
    fi
    log "sweep reclaiming a stale clone: $candidate"
    rm -rf "$candidate"
  done < <(ls -1dt "$runs_dir"/*/ 2>/dev/null | sed 's:/*$::')
}

# The primary LastDB home usually lives on this same volume. Refuse to clone
# onto a volume that is already low rather than discover it by filling it.
check_free_space() {
  local target="$1" free_gib
  [[ "$MIN_FREE_GIB" != "0" ]] || return 0
  free_gib="$(df -Pk "$target" 2>/dev/null | awk 'NR==2 {printf "%d", $4 / 1048576}')"
  [[ -n "$free_gib" ]] || return 0
  if [[ "$free_gib" -lt "$MIN_FREE_GIB" ]]; then
    fail "phase=preflight reason=insufficient-free-space free_gib=$free_gib min_gib=$MIN_FREE_GIB"
  fi
}

finish() {
  local rc=$?
  trap - EXIT INT TERM
  cleanup
  if [[ "$VERDICT" == "PASS" && "$rc" -eq 0 ]]; then
    printf 'PROOF: PASS report=%s\n' "$REPORT"
    exit 0
  fi
  printf 'PROOF: FAIL %s report=%s\n' "$DETAIL" "$REPORT"
  if [[ "$rc" -eq 0 ]]; then
    exit 1
  fi
  exit "$rc"
}
trap finish EXIT INT TERM

wait_for_node() {
  for _ in $(seq 1 600); do
    if [[ -S "$SOCKET" ]] \
      && curl -fsS --max-time 2 --unix-socket "$SOCKET" \
        -H 'Host: localhost' -H 'X-LastDB-Client: delete-converge-proof' \
        http://x/health >/dev/null 2>&1; then
      return 0
    fi
    if ! kill -0 "$NODE_PID" 2>/dev/null; then
      tail -80 "$DAEMON_LOG" >&2 || true
      fail "phase=boot reason=daemon-exited"
      return 1
    fi
    perl -e 'select undef, undef, undef, 0.1'
  done
  tail -80 "$DAEMON_LOG" >&2 || true
  fail "phase=boot reason=socket-timeout"
}

boot_node() {
  rm -f "$SOCKET"
  LASTDB_HOME="$WORK_HOME" \
  LASTDB_RESIDENT_MODE=write \
  LASTDB_ATOM_RECLAIM_MS=3600000 \
  LASTDB_UDS_WORKERS=4 \
  LASTDB_UDS_HANDLER_TIMEOUT_SECS="$REQUEST_TIMEOUT_SECS" \
  LASTDB_UDS_ADMIN_TIMEOUT_SECS=1800 \
  LASTDB_DEFER_WRITE_THROUGH_BYTES="$TAIL_WRITE_THROUGH_BYTES" \
  LASTDB_MAX_ATOM_CONTENT_BYTES="$TAIL_ATOM_CONTENT_BYTES" \
  RUST_MIN_STACK=67108864 \
    "$LASTDBD" --data-dir "$WORK_HOME" >>"$DAEMON_LOG" 2>&1 &
  NODE_PID=$!
  wait_for_node
}

get_json() {
  local path="$1" out="$2"
  curl -fsS --max-time 120 --unix-socket "$SOCKET" \
    -H 'Host: localhost' -H 'X-LastDB-Client: delete-converge-proof' \
    "http://x$path" >"$out"
}

post_json() {
  local path="$1" body="$2" out="$3" code
  code="$(curl -sS --max-time "$REQUEST_TIMEOUT_SECS" --unix-socket "$SOCKET" \
    -H 'Host: localhost' -H 'X-LastDB-Client: delete-converge-proof' \
    -H 'Content-Type: application/json' --data-binary "@$body" \
    -o "$out" -w '%{http_code}' "http://x$path")"
  if [[ "$code" -lt 200 || "$code" -ge 300 ]]; then
    log "request $path returned HTTP $code: $(head -c 800 "$out")"
    fail "phase=request path=$path http=$code"
    return 1
  fi
}

run_repair_dry_run() {
  local out="$1"
  LASTDB_UDS_ADMIN_TIMEOUT_SECS=1800 \
    "$LASTDB" --data-dir "$WORK_HOME" db repair-dangling-tips \
      --json --max-ops "$REPAIR_MAX_OPS" --tip-page "$REPAIR_TIP_PAGE" \
      --audit-limit "$REPAIR_AUDIT_LIMIT" >"$out"
  jq -e '
    (.tips_scanned | numbers)
    and (.repairable_tips | numbers)
    and (.completed | type == "boolean")
  ' "$out" >/dev/null
}

query_key() {
  local sk="$1" out="$2"
  jq -nc --arg schema "$SCHEMA" --arg board "$PROOF_BOARD" --arg sk "$sk" '
    {
      schema_name: $schema,
      fields: ["title"],
      filter: {HashRangeKey: {hash: $board, range: $sk}}
    }
  ' >"$RUN_DIR/query-body.json"
  post_json /api/query "$RUN_DIR/query-body.json" "$out"
}

assert_title() {
  local sk="$1" expected="$2" out="$3"
  query_key "$sk" "$out"
  # `/api/query` nests every requested field under `.results[].fields`. A flat
  # `.results[0].title` reads null for every row, so the bar could only fail.
  jq -e --arg expected "$expected" '
    .returned_count == 1 and .results[0].fields.title == $expected
  ' "$out" >/dev/null || fail "phase=query key=$sk expected=$expected"
}

assert_absent() {
  local sk="$1" out="$2"
  query_key "$sk" "$out"
  jq -e '.returned_count == 0 and (.results | length) == 0' "$out" >/dev/null \
    || fail "phase=query key=$sk expected=absent"
}

purge_stats() {
  local status_file="$1"
  jq -c --arg schema "$SCHEMA" '
    .status.purge_stats[$schema] // {
      purges: 0,
      records_purged: 0,
      exclusive_hold_us: 0,
      schema_barrier_acquisitions: 0,
      purge_target_slots: 0,
      purge_candidate_atoms: 0,
      purge_reverse_edge_reads: 0
    }
  ' "$status_file"
}

# Name WHY the lane never became non-idle. "persist-lane-idle" describes only
# one of three outcomes, and it is the one that sends a reader to the memory
# gauge. A tail request that never returned, or that the daemon rejected, is a
# workload fault, and the gauge was right. Read the per-worker HTTP codes the
# tail writer records; an absent file means that request never returned.
classify_tail_failure() {
  local workers="$1" dir="$2"
  local accepted=0 rejected=0 pending=0 worker code_file code
  for worker in $(seq 0 $((workers - 1))); do
    code_file="$dir/tail-$worker-code.txt"
    if [[ ! -s "$code_file" ]]; then
      pending=$((pending + 1))
      continue
    fi
    code="$(tr -d '[:space:]' <"$code_file")"
    if [[ "$code" =~ ^2[0-9][0-9]$ ]]; then
      accepted=$((accepted + 1))
    else
      rejected=$((rejected + 1))
    fi
  done
  if [[ "$accepted" -eq 0 && "$pending" -gt 0 ]]; then
    printf 'reason=tail-workload-never-returned'
  elif [[ "$accepted" -eq 0 ]]; then
    printf 'reason=tail-workload-rejected'
  else
    printf 'reason=persist-lane-idle'
  fi
  printf ' tail_accepted=%s tail_rejected=%s tail_pending=%s' \
    "$accepted" "$rejected" "$pending"
}

# Build one tail batch body. The calibration probe and the workers share this
# row shape, so the per-row cost the probe measures describes the rows the
# workers actually send.
tail_batch_json() {
  local prefix="$1" first="$2" rows="$3" out="$4"
  # The payload arrives by file, not by argv. A 1 MiB `--arg payload` overran
  # ARG_MAX and jq exited "Argument list too long" (measured 2026-09-07), which
  # is a harness fault that looks nothing like a lane verdict.
  jq -nc --arg schema "$SCHEMA" --arg board "$PROOF_BOARD" \
    --arg now "$NOW" --rawfile payload "$TAIL_PAYLOAD_FILE" --arg prefix "$prefix" \
    --argjson first "$first" --argjson rows "$rows" '
    def put($i):
      ($first + $i) as $n |
      ($prefix + "-" + ($n|tostring)) as $sk |
      {
        type: "mutation", schema: $schema,
        fields_and_values: {
          board: $board, sk: $sk, slug: $sk, title: $payload,
          column: "todo", position: $sk, created_at: $now, updated_at: $now
        },
        key_value: {hash: $board, range: $sk},
        mutation_type: "create"
      };
    {mutations: [range(0;$rows) | put(.)], convergence: "async"}
  ' >"$out"
}

# Rows that fit an ACK budget at a measured per-row cost. The cap is the
# requested size, the floor is one row, and a per-row cost of zero means the
# probe told us nothing, so the requested size stands.
tail_rows_for_budget() {
  local per_row="$1" budget="$2" want="$3"
  awk -v per="$per_row" -v budget="$budget" -v want="$want" '
    BEGIN {
      fit = (per + 0 > 0 ? int((budget + 0) / (per + 0)) : want + 0)
      if (fit > want + 0) { fit = want + 0 }
      if (fit < 1) { fit = 1 }
      print fit
    }'
}

# The daemon's own write-through counter. The probe reads it around itself, so
# the sizing learns whether the batch DEFERRED instead of assuming it did.
tail_write_throughs() {
  local out="$1" value
  get_json /api/status "$out" 2>/dev/null || { printf '0'; return 0; }
  value="$(jq -r '.status.memory_budget.deferred_write_throughs // 0' "$out")"
  [[ "$value" =~ ^[0-9]+$ ]] || value=0
  printf '%s' "$value"
}

# A payload that fits the atom content cap. The serialized atom carries the
# JSON quotes around the string, so the cap has to be met with room to spare.
tail_payload_under_atom_limit() {
  local payload="$1" limit="$2"
  awk -v payload="$payload" -v limit="$limit" '
    BEGIN {
      room = (limit + 0) - 2
      fit = payload + 0
      if (fit > room) { fit = room }
      if (fit < 1) { fit = 1 }
      print fit
    }'
}

# Rows whose payload stays under a batch byte budget. The budget is a share of
# the write-through threshold, because a batch that reaches the threshold
# persists inline and puts nothing in the lane.
tail_rows_under_write_through() {
  local payload="$1" threshold="$2" share_percent="$3" want="$4"
  awk -v payload="$payload" -v threshold="$threshold" \
    -v share="$share_percent" -v want="$want" '
    BEGIN {
      budget = (threshold + 0) * (share + 0) / 100
      fit = (payload + 0 > 0 ? int(budget / (payload + 0)) : want + 0)
      if (fit > want + 0) { fit = want + 0 }
      if (fit < 1) { fit = 1 }
      print fit
    }'
}

# How many batches the workers actually landed. A lane that stayed idle after
# one iteration and one that stayed idle after forty are different stories.
tail_iterations_total() {
  local workers="$1" dir="$2" worker count total=0
  for worker in $(seq 0 $((workers - 1))); do
    count="$(tr -d '[:space:]' <"$dir/tail-$worker-iterations.txt" 2>/dev/null || true)"
    [[ "$count" =~ ^[0-9]+$ ]] || count=0
    total=$((total + count))
  done
  printf '%s' "$total"
}

# The largest ACK any worker reported. An absent file means that worker never
# returned, which the classifier already names; treat it as zero here so the
# report never invents a duration for a request that produced none.
tail_ack_secs_max() {
  local workers="$1" dir="$2" worker secs_file best=0
  for worker in $(seq 0 $((workers - 1))); do
    secs_file="$dir/tail-$worker-secs.txt"
    [[ -s "$secs_file" ]] || continue
    best="$(awk -v a="$best" -v b="$(tr -d '[:space:]' <"$secs_file")" \
      'BEGIN { printf "%.3f", (b + 0 > a + 0 ? b + 0 : a + 0) }')"
  done
  printf '%s' "$best"
}

DETAIL="phase=preflight"
for tool in cargo curl jq perl; do
  command -v "$tool" >/dev/null || fail "phase=preflight missing=$tool"
done
[[ -d "$PRIMARY_HOME" ]] || fail "phase=preflight reason=primary-home-missing"
[[ -f "$PRIMARY_HOME/identity.key" ]] || fail "phase=preflight reason=identity-key-missing"
[[ "$REPAIR_MAX_OPS" =~ ^[1-9][0-9]*$ ]] \
  || fail "phase=preflight reason=repair-max-ops-invalid"
[[ "$REPAIR_TIP_PAGE" =~ ^[1-9][0-9]*$ ]] \
  || fail "phase=preflight reason=repair-tip-page-invalid"
[[ "$REPAIR_AUDIT_LIMIT" =~ ^[0-9]+$ ]] \
  || fail "phase=preflight reason=repair-audit-limit-invalid"
[[ "$REQUEST_TIMEOUT_SECS" =~ ^[1-9][0-9]*$ ]] \
  || fail "phase=preflight reason=request-timeout-invalid"
[[ "$KEEP_CLONES" =~ ^[0-9]+$ ]] \
  || fail "phase=preflight reason=keep-clones-invalid"
[[ "$MIN_FREE_GIB" =~ ^[0-9]+$ ]] \
  || fail "phase=preflight reason=min-free-gib-invalid"

PRIMARY_HOME="$(canonical_existing "$PRIMARY_HOME")"
PRIMARY_SOCKET="$PRIMARY_HOME/data/folddb.sock"

# Resolve existing symlink ancestors and lexical `..` components before any
# directory creation or clone. A post-clone check is too late if the selected
# destination already resolves inside the primary home.
PROOF_ROOT="$(canonical_planned "$PROOF_ROOT")" \
  || fail "phase=preflight reason=proof-root-not-absolute"
RUN_DIR="$(canonical_planned "$RUN_DIR")" \
  || fail "phase=preflight reason=run-dir-not-absolute"
WORK_HOME="$(canonical_planned "$WORK_HOME")" \
  || fail "phase=preflight reason=work-home-not-absolute"
REPORT="$RUN_DIR/proof.json"
DAEMON_LOG="$RUN_DIR/lastdbd.log"
CLONE_ERR="$RUN_DIR/clone.stderr"

DETAIL="phase=sweep"
if [[ "$KEEP" == "1" ]]; then
  log "sweep skipped because LASTDB_DELETE_PROOF_KEEP=1 asks to retain clones"
else
  sweep_stale_clones
fi

case "$WORK_HOME" in
  ""|/) fail "phase=preflight reason=unsafe-work-home" ;;
esac
if is_same_or_child "$WORK_HOME" "$PRIMARY_HOME" \
  || is_same_or_child "$PRIMARY_HOME" "$WORK_HOME"; then
  fail "phase=preflight reason=work-home-overlaps-primary"
fi
if ! is_same_or_child "$WORK_HOME" "$PROOF_ROOT"; then
  fail "phase=preflight reason=work-home-outside-proof-root"
fi
if ! socket_path_fits "$WORK_HOME/data/folddb-full.sock"; then
  fail "phase=preflight reason=socket-path-too-long"
fi
DETAIL="phase=preflight"
check_free_space "$PROOF_ROOT"

mkdir -p "$RUN_DIR"

if [[ -z "$LASTDBD_OVERRIDE" || -z "$LASTDB_OVERRIDE" ]]; then
  DETAIL="phase=build"
  "$ROOT/scripts/ci/with-fold-host-cargo-lock.sh" -- \
    cargo build -p lastdb_node --bin lastdbd --bin lastdb
fi
[[ -x "$LASTDBD" ]] || fail "phase=preflight reason=lastdbd-not-executable"
[[ -x "$LASTDB" ]] || fail "phase=preflight reason=lastdb-not-executable"

DETAIL="phase=clone"
mkdir -p "$(dirname "$WORK_HOME")"
if [[ -e "$WORK_HOME" ]]; then
  fail "phase=clone reason=work-home-already-exists"
fi
if ! cp -cR "$PRIMARY_HOME" "$WORK_HOME" 2>"$CLONE_ERR"; then
  # The live search inbox consumes files while cp walks it. Those ephemeral
  # request files are not database state and are not needed by this proof.
  if grep -Ev \
      -e '^cp: .*/apps/search/inbox/[^/]+: No such file or directory$' \
      -e '^cp: .* is a socket \(not copied\)\.$' \
      "$CLONE_ERR" | grep -q .; then
    sed -n '1,80p' "$CLONE_ERR" >&2
    fail "phase=clone reason=apfs-clone-failed"
  fi
  log "ignored transient search-inbox races during APFS clone"
fi
WORK_HOME="$(canonical_existing "$WORK_HOME")"
SOCKET="$WORK_HOME/data/folddb.sock"
if is_same_or_child "$WORK_HOME" "$PRIMARY_HOME" \
  || is_same_or_child "$PRIMARY_HOME" "$WORK_HOME" \
  || [[ "$SOCKET" == "$PRIMARY_SOCKET" ]]; then
  fail "phase=clone reason=primary-overlap"
fi
rm -f "$SOCKET" "$WORK_HOME/cloud_sync.json"
printf 'delete-converge proof run=%s\n' "$RUN_ID" \
  >"$WORK_HOME/.hold-delete-converge-proof"
WORK_HOME_OWNED=1

DETAIL="phase=boot-before"
boot_node

DETAIL="phase=schema"
get_json '/api/schemas?include_full=true' "$RUN_DIR/catalog.json"
jq -e --arg schema "$SCHEMA" '
  any(.schemas[];
    .name == $schema and .state == "Available"
    and .key.hash_field == "board" and .key.range_field == "sk"
    and (.fields | index("title") != null))
' "$RUN_DIR/catalog.json" >/dev/null \
  || fail "phase=schema reason=boardcards-hashrange-not-available"

DETAIL="phase=repair-before"
run_repair_dry_run "$RUN_DIR/repair-before.json"
REPAIR_BEFORE="$(jq -r '.repairable_tips' "$RUN_DIR/repair-before.json")"
REPAIR_SCANNED_BEFORE="$(jq -r '.tips_scanned' "$RUN_DIR/repair-before.json")"
REPAIR_COMPLETE_BEFORE="$(jq -r '.completed' "$RUN_DIR/repair-before.json")"
log "repairable_tips before=$REPAIR_BEFORE scanned=$REPAIR_SCANNED_BEFORE completed=$REPAIR_COMPLETE_BEFORE"

PROOF_BOARD="delete-converge-proof-$RUN_ID"
NOW="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

DETAIL="phase=seed"
jq -nc --arg schema "$SCHEMA" --arg board "$PROOF_BOARD" --arg now "$NOW" '
  def put($sk; $title; $kind): {
    type: "mutation", schema: $schema,
    fields_and_values: {
      board: $board, sk: $sk, slug: $sk, title: $title,
      column: "todo", position: $sk, created_at: $now, updated_at: $now
    },
    key_value: {hash: $board, range: $sk},
    mutation_type: $kind, durability: "durable"
  };
  {mutations: ([range(0;4) | put("delete-\(.)"; "delete seed \(.)"; "create")]
    + [put("order"; "order-0"; "create")]), convergence: "async"}
' >"$RUN_DIR/seed.json"
post_json /api/mutations/batch "$RUN_DIR/seed.json" "$RUN_DIR/seed-response.json"
jq -e '.count == 5 and .durability == "durable"' "$RUN_DIR/seed-response.json" >/dev/null \
  || fail "phase=seed reason=receipt-not-durable"

get_json /api/status "$RUN_DIR/status-before.json"
PURGE_BEFORE="$(purge_stats "$RUN_DIR/status-before.json")"

DETAIL="phase=mixed-workload"
jq -nc --arg schema "$SCHEMA" --arg board "$PROOF_BOARD" --arg now "$NOW" '
  def put($sk; $title; $kind): {
    type: "mutation", schema: $schema,
    fields_and_values: (if $kind == "delete" then {} else {
      board: $board, sk: $sk, slug: $sk, title: $title,
      column: "todo", position: $sk, created_at: $now, updated_at: $now
    } end),
    key_value: {hash: $board, range: $sk},
    mutation_type: $kind, durability: "durable"
  };
  {mutations:
    ([range(0;4) | put("upsert-\(.)"; "upserted-\(.)"; "create")]
    + [range(0;4) | put("delete-\(.)"; ""; "delete")]
    + [put("order"; "order-1"; "update")]),
    convergence: "async"}
' >"$RUN_DIR/mixed.json"
post_json /api/mutations/batch "$RUN_DIR/mixed.json" "$RUN_DIR/mixed-response.json"
jq -e '
  .count == 9 and .durability == "durable"
  and .background_tasks_drained == false and .convergence_pending == true
' "$RUN_DIR/mixed-response.json" >/dev/null \
  || fail "phase=mixed-workload reason=receipt-contract"

# One batch shares one author clock. Random mutation UUIDs break ties within
# that batch, so array order cannot prove that order-2 supersedes order-1.
# Observe the first durable update, then reserve a later clock in a new request.
assert_title order order-1 "$RUN_DIR/resident-order-1.json"
DETAIL="phase=ordered-update"
jq -c '{
  mutations: [.mutations[] | select(.key_value.range == "order")
    | .fields_and_values.title = "order-2"],
  convergence: "async"
}' "$RUN_DIR/mixed.json" >"$RUN_DIR/order-2.json"
post_json /api/mutations/batch "$RUN_DIR/order-2.json" "$RUN_DIR/order-2-response.json"
jq -e '
  .count == 1 and .durability == "durable"
  and .background_tasks_drained == false and .convergence_pending == true
' "$RUN_DIR/order-2-response.json" >/dev/null \
  || fail "phase=ordered-update reason=receipt-contract"

for index in $(seq 0 3); do
  assert_title "upsert-$index" "upserted-$index" "$RUN_DIR/resident-upsert-$index.json"
  assert_absent "delete-$index" "$RUN_DIR/resident-delete-$index.json"
done
assert_title order order-2 "$RUN_DIR/resident-order.json"

DETAIL="phase=converge-observable"
# The mixed receipt reports `convergence_pending: true`, so the delete converge
# runs after the ACK. `/api/status` also omits `purge_stats` while the map is
# empty, so a single read right after the ACK samples an absent ledger and
# reports every delta as zero. Wait for the counters to arrive, then judge the
# path they took. Separate the two verdicts: a converge that never purged is
# not a converge that purged through the bulk scan.
# The deadline is a wall clock, not an iteration count. Each poll also reads a
# ~19 KB `/api/status` from a debug daemon on a real-data clone, which costs far
# more than the pause between polls, so counting iterations overruns the stated
# timeout by an unbounded factor.
PURGE_AFTER="$PURGE_BEFORE"
CONVERGE_DEADLINE="$(( $(date +%s) + CONVERGE_TIMEOUT_SECS ))"
while :; do
  get_json /api/status "$RUN_DIR/status-after-mixed.json"
  PURGE_AFTER="$(purge_stats "$RUN_DIR/status-after-mixed.json")"
  if jq -en --argjson before "$PURGE_BEFORE" --argjson after "$PURGE_AFTER" '
    ($after.purges - $before.purges) >= 1
    and ($after.records_purged - $before.records_purged) >= 4
  ' >/dev/null; then
    break
  fi
  [[ "$(date +%s)" -lt "$CONVERGE_DEADLINE" ]] || break
  perl -e 'select undef, undef, undef, 0.1'
done
# `purges` counts converge CALLS, `records_purged` counts rows. The write path
# takes one call per `PURGE_BARRIER_CHUNK` (64) keys, so this batch's four
# deletes are one call that purges four records. Asserting `purges >= 4` asked
# the batch to be chunked 64x smaller than it is, so the bar could only fail.
jq -en --argjson before "$PURGE_BEFORE" --argjson after "$PURGE_AFTER" '
  ($after.purges - $before.purges) >= 1
  and ($after.records_purged - $before.records_purged) >= 4
' >/dev/null \
  || fail "phase=converge-observable reason=converge-purge-not-observed timeout_secs=$CONVERGE_TIMEOUT_SECS"
jq -en --argjson before "$PURGE_BEFORE" --argjson after "$PURGE_AFTER" '
  ($after.purge_target_slots - $before.purge_target_slots) > 0
  and ($after.schema_barrier_acquisitions - $before.schema_barrier_acquisitions) == 0
  and ($after.purge_candidate_atoms - $before.purge_candidate_atoms) == 0
  and ($after.purge_reverse_edge_reads - $before.purge_reverse_edge_reads) == 0
' >/dev/null || fail "phase=converge-observable reason=purge-bulk-path-observed"

# Queue unrelated, large BoardCards writes after the durable proof batch. The
# exact pressure gauge must show that this schema lane is non-idle before the
# SIGKILL. The tail rows are not part of the post-restart assertions.
DETAIL="phase=non-idle-crash"
TAIL_PAYLOAD_FILE="$RUN_DIR/tail-payload.txt"

# Measure the DEFERRAL, not only the ACK. The lane charges `defer_bytes`, which
# is atom bytes plus idempotency, search and share-prefix bytes — the search
# batch carries the row text a second time, so the charge measured about twice
# the payload. A batch that reaches the write-through threshold persists inline
# and puts nothing in the lane this bar reads.
#
# Two runs on a real-data clone, 2026-09-07, sized from the payload alone and
# both landed inline: 117 x 32 KiB (3.66 MiB payload) raised
# deferred_write_throughs to 1, and 4 x 512 KiB (2 MiB payload) raised it to 23
# over 24 batches. Guessing the multiplier is what failed. The probe now reads
# `deferred_write_throughs` around itself and halves the batch until the daemon
# actually defers it.
TAIL_SHARE_EFFECTIVE="$TAIL_BATCH_SHARE_PERCENT"
TAIL_CALIBRATION_TRIES=0
TAIL_WRITE_THROUGHS_BEFORE=0
TAIL_WRITE_THROUGHS_AFTER=0
while :; do
  TAIL_CALIBRATION_TRIES=$((TAIL_CALIBRATION_TRIES + 1))
  TAIL_PAYLOAD_BYTES="$(tail_payload_under_atom_limit "$TAIL_PAYLOAD_BYTES" \
    "$TAIL_ATOM_CONTENT_BYTES")"
  # `--rawfile` keeps a trailing newline, so write the bytes without one and the
  # row title is exactly TAIL_PAYLOAD_BYTES.
  printf "%${TAIL_PAYLOAD_BYTES}s" '' | tr ' ' x >"$TAIL_PAYLOAD_FILE"
  TAIL_ROWS_WRITE_THROUGH_FIT="$(tail_rows_under_write_through "$TAIL_PAYLOAD_BYTES" \
    "$TAIL_WRITE_THROUGH_BYTES" "$TAIL_SHARE_EFFECTIVE" "$TAIL_ROWS")"
  TAIL_CALIBRATION_ROWS="$TAIL_ROWS_WRITE_THROUGH_FIT"
  TAIL_WRITE_THROUGHS_BEFORE="$(tail_write_throughs "$RUN_DIR/status-tail-probe.json")"
  tail_batch_json "calibration-$TAIL_CALIBRATION_TRIES" 0 "$TAIL_CALIBRATION_ROWS" \
    "$RUN_DIR/tail-calibration.json"
  TAIL_CALIBRATION_MEASURE="$(curl -sS --max-time "$TAIL_REQUEST_TIMEOUT_SECS" \
    --unix-socket "$SOCKET" -H 'Host: localhost' \
    -H 'X-LastDB-Client: delete-converge-proof-tail' \
    -H 'Content-Type: application/json' \
    --data-binary "@$RUN_DIR/tail-calibration.json" \
    -o "$RUN_DIR/tail-calibration-response.json" -w '%{http_code} %{time_total}' \
    http://x/api/mutations/batch)" || TAIL_CALIBRATION_MEASURE="000 0"
  TAIL_CALIBRATION_CODE="${TAIL_CALIBRATION_MEASURE%% *}"
  TAIL_CALIBRATION_SECS="${TAIL_CALIBRATION_MEASURE##* }"
  printf '%s\n' "$TAIL_CALIBRATION_CODE" >"$RUN_DIR/tail-calibration-code.txt"
  printf '%s\n' "$TAIL_CALIBRATION_SECS" >"$RUN_DIR/tail-calibration-secs.txt"
  # A probe that never returned is not a sizing input. Say so with its own reason
  # instead of sizing the workload from a zero.
  [[ "$TAIL_CALIBRATION_CODE" =~ ^2[0-9][0-9]$ ]] \
    || fail "phase=non-idle-crash reason=tail-calibration-failed" \
      "http=$TAIL_CALIBRATION_CODE calibration_rows=$TAIL_CALIBRATION_ROWS" \
      "calibration_payload_bytes=$TAIL_PAYLOAD_BYTES" \
      "calibration_secs=$TAIL_CALIBRATION_SECS" \
      "request_timeout_secs=$TAIL_REQUEST_TIMEOUT_SECS"
  TAIL_WRITE_THROUGHS_AFTER="$(tail_write_throughs "$RUN_DIR/status-tail-probe.json")"
  log "tail probe try=$TAIL_CALIBRATION_TRIES rows=$TAIL_CALIBRATION_ROWS payload_bytes=$TAIL_PAYLOAD_BYTES share_percent=$TAIL_SHARE_EFFECTIVE secs=$TAIL_CALIBRATION_SECS write_throughs=$TAIL_WRITE_THROUGHS_BEFORE->$TAIL_WRITE_THROUGHS_AFTER"
  if [[ "$TAIL_WRITE_THROUGHS_AFTER" -le "$TAIL_WRITE_THROUGHS_BEFORE" ]]; then
    break
  fi
  if [[ "$TAIL_CALIBRATION_TRIES" -ge "$TAIL_CALIBRATION_ATTEMPTS" ]]; then
    fail "phase=non-idle-crash reason=tail-batch-always-write-through" \
      "tries=$TAIL_CALIBRATION_TRIES rows=$TAIL_CALIBRATION_ROWS" \
      "payload_bytes=$TAIL_PAYLOAD_BYTES share_percent=$TAIL_SHARE_EFFECTIVE" \
      "write_through_bytes=$TAIL_WRITE_THROUGH_BYTES"
  fi
  # Halve the batch. Rows first, because fewer larger rows keep the ACK cheap;
  # once one row is all that is left, the row itself has to shrink.
  if [[ "$TAIL_CALIBRATION_ROWS" -gt 1 ]]; then
    TAIL_SHARE_EFFECTIVE=$(( TAIL_SHARE_EFFECTIVE / 2 ))
    [[ "$TAIL_SHARE_EFFECTIVE" -ge 1 ]] || TAIL_SHARE_EFFECTIVE=1
  else
    TAIL_PAYLOAD_BYTES=$(( TAIL_PAYLOAD_BYTES / 2 ))
    [[ "$TAIL_PAYLOAD_BYTES" -ge 1024 ]] || TAIL_PAYLOAD_BYTES=1024
  fi
done
TAIL_ACK_SECS_PER_ROW="$(awk -v secs="$TAIL_CALIBRATION_SECS" \
  -v rows="$TAIL_CALIBRATION_ROWS" \
  'BEGIN { printf "%.6f", (rows > 0 ? secs / rows : 0) }')"
# Two independent caps. The ACK budget keeps a batch inside the curl timeout.
# The proven-deferring size keeps it in the lane at all. The smaller one wins,
# because either violation empties the gauge this bar reads.
TAIL_ROWS_BUDGET_FIT="$(tail_rows_for_budget "$TAIL_ACK_SECS_PER_ROW" \
  "$TAIL_ACK_BUDGET_SECS" "$TAIL_ROWS")"
TAIL_ROWS_PROVEN_FIT="$TAIL_ROWS_WRITE_THROUGH_FIT"
TAIL_ROWS_WRITE_THROUGH_FIT=$(( TAIL_ROWS_PROVEN_FIT / TAIL_WORKER_MARGIN_DIVISOR ))
[[ "$TAIL_ROWS_WRITE_THROUGH_FIT" -ge 1 ]] || TAIL_ROWS_WRITE_THROUGH_FIT=1
TAIL_ROWS_EFFECTIVE="$TAIL_ROWS_BUDGET_FIT"
if [[ "$TAIL_ROWS_WRITE_THROUGH_FIT" -lt "$TAIL_ROWS_EFFECTIVE" ]]; then
  TAIL_ROWS_EFFECTIVE="$TAIL_ROWS_WRITE_THROUGH_FIT"
fi
log "tail sizing tries=$TAIL_CALIBRATION_TRIES proven_fit=$TAIL_ROWS_PROVEN_FIT worker_margin_divisor=$TAIL_WORKER_MARGIN_DIVISOR calibration_rows=$TAIL_CALIBRATION_ROWS calibration_secs=$TAIL_CALIBRATION_SECS per_row_secs=$TAIL_ACK_SECS_PER_ROW ack_budget_secs=$TAIL_ACK_BUDGET_SECS write_through_bytes=$TAIL_WRITE_THROUGH_BYTES batch_share_percent=$TAIL_SHARE_EFFECTIVE payload_bytes=$TAIL_PAYLOAD_BYTES rows_requested=$TAIL_ROWS budget_fit=$TAIL_ROWS_BUDGET_FIT write_through_fit=$TAIL_ROWS_WRITE_THROUGH_FIT rows_effective=$TAIL_ROWS_EFFECTIVE"

# The deadline is a wall clock, not an iteration count. `for _ in $(seq 1 300)`
# with a 0.01s pause reads as a 3-second budget, but each pass also reads a
# ~19 KB /api/status from a debug daemon on a real-data clone, so the true wait
# was minutes and the stated deadline was fiction. fold PR 1984 fixed this exact
# shape in the converge bar; this was the same defect in the next loop.
TAIL_WAIT_STARTED="$(date +%s)"
TAIL_DEADLINE="$(( TAIL_WAIT_STARTED + TAIL_TIMEOUT_SECS ))"

# Sustained pressure, not one shot. A deferred batch ACKs as soon as it is
# queued, so a single batch per worker can drain before the poller reads the
# gauge. Each worker keeps submitting fresh keys until the poller says the lane
# is non-idle, the deadline passes, or the daemon stops accepting.
tail_worker() {
  local worker="$1" iteration=0 measure code secs
  while :; do
    [[ ! -e "$RUN_DIR/tail-stop" ]] || break
    [[ "$(date +%s)" -lt "$TAIL_DEADLINE" ]] || break
    [[ "$iteration" -lt "$TAIL_MAX_ITERATIONS" ]] || break
    tail_batch_json "tail-$worker-$iteration" 0 "$TAIL_ROWS_EFFECTIVE" \
      "$RUN_DIR/tail-$worker.json"
    # Record the HTTP code and the ACK duration per worker. Without the code a
    # tail request that never returned is indistinguishable from one the daemon
    # accepted, and the only available reason was the lane gauge. Without the
    # duration the next sizing run has to re-derive the cost this run paid for.
    measure="$(curl -sS --max-time "$TAIL_REQUEST_TIMEOUT_SECS" --unix-socket "$SOCKET" \
      -H 'Host: localhost' -H 'X-LastDB-Client: delete-converge-proof-tail' \
      -H 'Content-Type: application/json' --data-binary "@$RUN_DIR/tail-$worker.json" \
      -o "$RUN_DIR/tail-$worker-response.json" -w '%{http_code} %{time_total}' \
      http://x/api/mutations/batch)" || measure="000 0"
    code="${measure%% *}"
    secs="${measure##* }"
    printf '%s\n' "$code" >"$RUN_DIR/tail-$worker-code.txt"
    printf '%s\n' "$secs" >"$RUN_DIR/tail-$worker-secs.txt"
    iteration=$((iteration + 1))
    printf '%s\n' "$iteration" >"$RUN_DIR/tail-$worker-iterations.txt"
    [[ "$code" =~ ^2[0-9][0-9]$ ]] || break
  done
}

rm -f "$RUN_DIR/tail-stop"
for worker in $(seq 0 $((TAIL_WORKERS - 1))); do
  tail_worker "$worker" &
  TAIL_PIDS+=("$!")
done

LANE_BYTES=0
LANE_SCHEMA=""
while :; do
  if get_json /api/status "$RUN_DIR/status-before-kill.json" 2>/dev/null; then
    LANE_BYTES="$(jq -r '.status.memory_budget.deferred_persist_bytes // 0' "$RUN_DIR/status-before-kill.json")"
    LANE_SCHEMA="$(jq -r '.status.memory_budget.deferred_heaviest_lane // ""' "$RUN_DIR/status-before-kill.json")"
    if [[ "$LANE_BYTES" -gt 0 && "$LANE_SCHEMA" == "$SCHEMA" ]]; then
      break
    fi
  fi
  [[ "$(date +%s)" -lt "$TAIL_DEADLINE" ]] || break
  perl -e 'select undef, undef, undef, 0.1'
done
TAIL_WAIT_SECS="$(( $(date +%s) - TAIL_WAIT_STARTED ))"
touch "$RUN_DIR/tail-stop"
TAIL_ITERATIONS_TOTAL="$(tail_iterations_total "$TAIL_WORKERS" "$RUN_DIR")"
if [[ ! ( "$LANE_BYTES" -gt 0 && "${LANE_SCHEMA:-}" == "$SCHEMA" ) ]]; then
  fail "phase=non-idle-crash $(classify_tail_failure "$TAIL_WORKERS" "$RUN_DIR")" \
    "lane=${LANE_SCHEMA:-none} deferred_persist_bytes=$LANE_BYTES" \
    "timeout_secs=$TAIL_TIMEOUT_SECS request_timeout_secs=$TAIL_REQUEST_TIMEOUT_SECS" \
    "tail_rows_effective=$TAIL_ROWS_EFFECTIVE tail_ack_secs_per_row=$TAIL_ACK_SECS_PER_ROW" \
    "tail_batch_bytes=$(( TAIL_ROWS_EFFECTIVE * TAIL_PAYLOAD_BYTES ))" \
    "tail_write_through_bytes=$TAIL_WRITE_THROUGH_BYTES" \
    "tail_iterations=$TAIL_ITERATIONS_TOTAL" \
    "tail_ack_secs_max=$(tail_ack_secs_max "$TAIL_WORKERS" "$RUN_DIR")"
fi

log "SIGKILL pid=$NODE_PID lane=$LANE_SCHEMA deferred_persist_bytes=$LANE_BYTES"
kill -KILL "$NODE_PID"
wait "$NODE_PID" 2>/dev/null || true
NODE_PID=""
for pid in "${TAIL_PIDS[@]}"; do
  wait "$pid" 2>/dev/null || true
done
TAIL_PIDS=()

DETAIL="phase=boot-after-kill"
boot_node

DETAIL="phase=restart-reads"
for index in $(seq 0 3); do
  assert_title "upsert-$index" "upserted-$index" "$RUN_DIR/restart-upsert-$index.json"
  assert_absent "delete-$index" "$RUN_DIR/restart-delete-$index.json"
done
assert_title order order-2 "$RUN_DIR/restart-order.json"

DETAIL="phase=repair-after"
run_repair_dry_run "$RUN_DIR/repair-after.json"
REPAIR_AFTER="$(jq -r '.repairable_tips' "$RUN_DIR/repair-after.json")"
REPAIR_SCANNED_AFTER="$(jq -r '.tips_scanned' "$RUN_DIR/repair-after.json")"
REPAIR_COMPLETE_AFTER="$(jq -r '.completed' "$RUN_DIR/repair-after.json")"
log "repairable_tips after=$REPAIR_AFTER scanned=$REPAIR_SCANNED_AFTER completed=$REPAIR_COMPLETE_AFTER"

DETAIL="phase=report"
jq -n \
  --arg run_id "$RUN_ID" \
  --arg schema "$SCHEMA" \
  --arg board "$PROOF_BOARD" \
  --arg primary "$PRIMARY_HOME" \
  --arg work_home "$WORK_HOME" \
  --argjson repair_sample_max_ops "$REPAIR_MAX_OPS" \
  --argjson repairable_tips_before "$REPAIR_BEFORE" \
  --argjson repairable_tips_after "$REPAIR_AFTER" \
  --argjson repair_tips_scanned_before "$REPAIR_SCANNED_BEFORE" \
  --argjson repair_tips_scanned_after "$REPAIR_SCANNED_AFTER" \
  --argjson repair_sample_completed_before "$REPAIR_COMPLETE_BEFORE" \
  --argjson repair_sample_completed_after "$REPAIR_COMPLETE_AFTER" \
  --argjson deferred_persist_bytes_at_kill "$LANE_BYTES" \
  --argjson tail_workers "$TAIL_WORKERS" \
  --argjson tail_rows "$TAIL_ROWS" \
  --argjson tail_payload_bytes "$TAIL_PAYLOAD_BYTES" \
  --argjson tail_request_timeout_secs "$TAIL_REQUEST_TIMEOUT_SECS" \
  --argjson tail_wait_secs "$TAIL_WAIT_SECS" \
  --argjson tail_rows_effective "$TAIL_ROWS_EFFECTIVE" \
  --argjson tail_calibration_rows "$TAIL_CALIBRATION_ROWS" \
  --argjson tail_calibration_secs "$TAIL_CALIBRATION_SECS" \
  --argjson tail_ack_secs_per_row "$TAIL_ACK_SECS_PER_ROW" \
  --argjson tail_ack_budget_secs "$TAIL_ACK_BUDGET_SECS" \
  --argjson tail_rows_budget_fit "$TAIL_ROWS_BUDGET_FIT" \
  --argjson tail_rows_write_through_fit "$TAIL_ROWS_WRITE_THROUGH_FIT" \
  --argjson tail_write_through_bytes "$TAIL_WRITE_THROUGH_BYTES" \
  --argjson tail_batch_share_percent "$TAIL_BATCH_SHARE_PERCENT" \
  --argjson tail_atom_content_bytes "$TAIL_ATOM_CONTENT_BYTES" \
  --argjson tail_calibration_tries "$TAIL_CALIBRATION_TRIES" \
  --argjson tail_share_effective "$TAIL_SHARE_EFFECTIVE" \
  --argjson tail_rows_proven_fit "$TAIL_ROWS_PROVEN_FIT" \
  --argjson tail_worker_margin_divisor "$TAIL_WORKER_MARGIN_DIVISOR" \
  --argjson tail_iterations "$TAIL_ITERATIONS_TOTAL" \
  --argjson tail_ack_secs_max "$(tail_ack_secs_max "$TAIL_WORKERS" "$RUN_DIR")" \
  --argjson purge_before "$PURGE_BEFORE" \
  --argjson purge_after "$PURGE_AFTER" '
  {
    verdict: "PASS",
    run_id: $run_id,
    schema: $schema,
    board: $board,
    primary_role: "read-only clone source",
    primary_home: $primary,
    work_home: $work_home,
    checks: {
      mixed_durable_restart: "pass",
      delete_avoids_purge_records_bulk: "pass",
      same_key_resident_durable_order: "pass",
      sigkill_before_lane_idle: "pass",
      repairable_tips_reported: "pass"
    },
    measurements: {
      repair_sample_max_ops: $repair_sample_max_ops,
      repair_tips_scanned_before: $repair_tips_scanned_before,
      repair_tips_scanned_after: $repair_tips_scanned_after,
      repair_sample_completed_before: $repair_sample_completed_before,
      repair_sample_completed_after: $repair_sample_completed_after,
      repairable_tips_before: $repairable_tips_before,
      repairable_tips_after: $repairable_tips_after,
      repairable_tips_delta: ($repairable_tips_after - $repairable_tips_before),
      deferred_persist_bytes_at_kill: $deferred_persist_bytes_at_kill,
      tail_workers: $tail_workers,
      tail_rows_requested: $tail_rows,
      tail_rows_effective: $tail_rows_effective,
      tail_payload_bytes: $tail_payload_bytes,
      tail_bytes_total: ($tail_workers * $tail_rows_effective * $tail_payload_bytes),
      tail_request_timeout_secs: $tail_request_timeout_secs,
      tail_wait_secs: $tail_wait_secs,
      tail_calibration_rows: $tail_calibration_rows,
      tail_calibration_secs: $tail_calibration_secs,
      tail_ack_secs_per_row: $tail_ack_secs_per_row,
      tail_ack_budget_secs: $tail_ack_budget_secs,
      tail_ack_secs_max: $tail_ack_secs_max,
      tail_rows_budget_fit: $tail_rows_budget_fit,
      tail_rows_write_through_fit: $tail_rows_write_through_fit,
      tail_batch_bytes: ($tail_rows_effective * $tail_payload_bytes),
      tail_write_through_bytes: $tail_write_through_bytes,
      tail_batch_share_percent: $tail_batch_share_percent,
      tail_atom_content_bytes: $tail_atom_content_bytes,
      tail_calibration_tries: $tail_calibration_tries,
      tail_batch_share_percent_effective: $tail_share_effective,
      tail_rows_proven_fit: $tail_rows_proven_fit,
      tail_worker_margin_divisor: $tail_worker_margin_divisor,
      tail_iterations: $tail_iterations,
      purge_before: $purge_before,
      purge_after: $purge_after
    }
  }
' >"$REPORT"

stop_node
# Checkpoint reclaim. Every bar has passed and the report is written, so the
# clone is dead weight from here on. Releasing it now means a kill during the
# EXIT trap leaks nothing.
reclaim_work_home
VERDICT="PASS"
DETAIL="checks=5"
