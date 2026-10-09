#!/usr/bin/env bash
# Hourly multi-app admin deliver — fill every admin SPA tab mailbox.
#
# Reuses ExememKanbanConsumer-prod public keys (same enrolled consumer the
# Kanban tab already uses). Each app stages + auto-approves a slim
# lastdb.slice.v1 delivery. Tabs select by schema; they share one mailbox.
#
# Usage:
#   ./run.sh                 # all apps
#   ./run.sh kanban brain    # subset
#   ./run.sh --dry-run
#
# Recipient resolution (first wins):
#   1. ~/.lastdb/admin-deliver-recipient.env  (KEY=value public keys)
#   2. LASTDB_ADMIN_DELIVER_RECIPIENT_JSON / AWS secret via kanban deliver
#   3. AWS Secrets Manager ExememKanbanConsumer-prod
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd -P)"
FOLD_ROOT="$(cd "$ROOT/../.." && pwd -P)"
LASDB_HOME="${LASTDB_HOME:-$HOME/.lastdb}"
ENV_FILE="${ADMIN_DELIVER_ENV:-$LASDB_HOME/admin-deliver-recipient.env}"
SECRET_ID="${LASTDB_ADMIN_DELIVER_SECRET_ID:-ExememKanbanConsumer-prod}"
AWS_REGION="${AWS_REGION:-us-east-1}"

DRY_RUN=0
APPS=()
while [ "$#" -gt 0 ]; do
  case "$1" in
    --dry-run) DRY_RUN=1; shift ;;
    -h|--help)
      sed -n '1,30p' "$0"
      exit 0
      ;;
    *)
      APPS+=("$1")
      shift
      ;;
  esac
done

if [ "${#APPS[@]}" -eq 0 ]; then
  APPS=(kanban brain routines last-stack discovery situations lastgit)
fi

log() { printf '%s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"; }

ensure_recipient_env() {
  if [ -f "$ENV_FILE" ]; then
    # shellcheck disable=SC1090
    set -a
    # shellcheck source=/dev/null
    source "$ENV_FILE"
    set +a
    return 0
  fi
  if [ -n "${LASTDB_ADMIN_DELIVER_RECIPIENT_JSON:-}" ]; then
    python3 - "$ENV_FILE" <<'PY'
import json, os, sys
from pathlib import Path
path = Path(sys.argv[1])
data = json.loads(os.environ["LASTDB_ADMIN_DELIVER_RECIPIENT_JSON"])
pk = data.get("recipient_pubkey") or data.get("ed25519_public_key")
mk = data.get("messaging_public_key") or data.get("x25519_public_key")
ps = data.get("messaging_pseudonym") or data.get("pseudonym")
if not (pk and mk and ps):
    raise SystemExit("LASTDB_ADMIN_DELIVER_RECIPIENT_JSON missing public keys")
prefixes = [
    "ROUTINES_ADMIN", "FBRAIN_ADMIN", "LASTGIT_ADMIN",
    "LAST_STACK_ADMIN", "DISCOVERY_ADMIN",
]
lines = []
for p in prefixes:
    lines.append(f"{p}_RECIPIENT_PUBKEY={pk}")
    lines.append(f"{p}_MESSAGING_PUBLIC_KEY={mk}")
    lines.append(f"{p}_MESSAGING_PSEUDONYM={ps}")
path.parent.mkdir(parents=True, exist_ok=True)
path.write_text("\n".join(lines) + "\n")
path.chmod(0o600)
print(f"wrote {path}")
PY
    set -a
    # shellcheck source=/dev/null
    source "$ENV_FILE"
    set +a
    return 0
  fi
  # Pull public fields only from AWS SM enroll secret.
  log "loading recipient public keys from secretsmanager://$SECRET_ID"
  python3 - "$ENV_FILE" "$SECRET_ID" "$AWS_REGION" <<'PY'
import json, subprocess, sys
from pathlib import Path
path, secret_id, region = Path(sys.argv[1]), sys.argv[2], sys.argv[3]
raw = subprocess.check_output(
    [
        "aws", "secretsmanager", "get-secret-value",
        "--secret-id", secret_id, "--region", region,
        "--query", "SecretString", "--output", "text",
    ],
    text=True,
)
data = json.loads(raw)
pk = data.get("recipient_pubkey") or data.get("ed25519_public_key")
mk = data.get("messaging_public_key") or data.get("x25519_public_key")
ps = data.get("messaging_pseudonym") or data.get("pseudonym")
if not (pk and mk and ps):
    raise SystemExit(f"secret {secret_id} missing public key fields")
prefixes = [
    "ROUTINES_ADMIN", "FBRAIN_ADMIN", "LASTGIT_ADMIN",
    "LAST_STACK_ADMIN", "DISCOVERY_ADMIN",
]
lines = []
for p in prefixes:
    lines.append(f"{p}_RECIPIENT_PUBKEY={pk}")
    lines.append(f"{p}_MESSAGING_PUBLIC_KEY={mk}")
    lines.append(f"{p}_MESSAGING_PSEUDONYM={ps}")
# Also materialize JSON for Mini-stage helpers (public only).
json_path = path.with_suffix(".json")
json_path.write_text(json.dumps({
    "recipient_pubkey": pk,
    "messaging_public_key": mk,
    "messaging_pseudonym": ps,
}) + "\n")
json_path.chmod(0o600)
path.parent.mkdir(parents=True, exist_ok=True)
path.write_text("\n".join(lines) + "\n")
path.chmod(0o600)
print(f"wrote {path} and {json_path}")
PY
  set -a
  # shellcheck source=/dev/null
  source "$ENV_FILE"
  set +a
}

run_one() {
  local app="$1"
  local rc=0
  log "START app=$app dry_run=$DRY_RUN"
  case "$app" in
    kanban)
      if [ "$DRY_RUN" -eq 1 ]; then
        python3 "$FOLD_ROOT/scripts/admin-kanban-hourly-deliver/deliver.py" --dry-run || rc=$?
      else
        python3 "$FOLD_ROOT/scripts/admin-kanban-hourly-deliver/deliver.py" || rc=$?
      fi
      ;;
    brain)
      if [ "$DRY_RUN" -eq 1 ]; then
        brain admin-snapshot deliver --dry-run --max-records 5 || rc=$?
      else
        brain admin-snapshot deliver --approve --max-records 5 || rc=$?
      fi
      ;;
    routines)
      # Prefer slim snapshot+status via helper when installed CLI still embeds rows_json.
      if [ "$DRY_RUN" -eq 1 ]; then
        routines deliver-status --dry-run --max-records 8 || rc=$?
      else
        if ! routines deliver-status --approve --max-records 8; then
          log "routines CLI deliver failed; trying slim Mini stage helper"
          python3 "$ROOT/slim_routines_deliver.py" || rc=$?
        fi
      fi
      ;;
    last-stack)
      if [ "$DRY_RUN" -eq 1 ]; then
        last-stack-deliver-status --dry-run --max-records 1 || rc=$?
      else
        last-stack-deliver-status --approve --max-records 1 || rc=$?
      fi
      ;;
    discovery)
      DISCOVERY_SCRIPT="${DISCOVERY_PUBLISH_ADMIN:-$HOME/code/edgevector/discovery/publish_admin_status.py}"
      if [ ! -f "$DISCOVERY_SCRIPT" ]; then
        log "SKIP discovery — missing $DISCOVERY_SCRIPT"
        return 0
      fi
      if [ "$DRY_RUN" -eq 1 ]; then
        python3 "$DISCOVERY_SCRIPT" deliver --dry-run --json || rc=$?
      else
        python3 "$DISCOVERY_SCRIPT" deliver --approve --json || rc=$?
      fi
      ;;
    situations)
      if [ "$DRY_RUN" -eq 1 ]; then
        python3 "$ROOT/situations_deliver.py" --dry-run || rc=$?
      else
        python3 "$ROOT/situations_deliver.py" --approve || rc=$?
      fi
      ;;
    lastgit)
      if [ "$DRY_RUN" -eq 1 ]; then
        lastgit deliver-status --dry-run --max-records 3 || rc=$?
      else
        # lastgit open-CR queries can fail under Mini QoS; do not fail the fleet.
        lastgit deliver-status --approve --max-records 3 || {
          log "WARN lastgit deliver failed (non-fatal under load)"
          rc=0
        }
      fi
      ;;
    *)
      log "unknown app: $app"
      return 2
      ;;
  esac
  if [ "$rc" -eq 0 ]; then
    log "OK app=$app"
  else
    log "FAIL app=$app rc=$rc"
  fi
  return "$rc"
}

ensure_recipient_env

failures=0
for app in "${APPS[@]}"; do
  if ! run_one "$app"; then
    failures=$((failures + 1))
  fi
done

if [ "$failures" -gt 0 ]; then
  log "DONE with $failures failure(s)"
  exit 1
fi
log "DONE all ok"
exit 0
