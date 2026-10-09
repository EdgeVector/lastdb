#!/usr/bin/env bash
# Repeatable first-party dogfood exerciser for `lastdb app …` (design: brain
# `design-lastdb-app-registry`). Drives at least two REAL first-party apps —
# brain (EdgeVector/brain) and kanban (EdgeVector/fkanban) — through the
# actual shipped registry flow (check -> publish/reserve -> register-schemas
# -> promote) against the real DEV registry, using each app's own
# `src/schemas.ts` as the manifest source. No hand-inserted registry rows, no
# fixture schemas, no mock schema/exemem services — this is the real DEV
# stack (see `lastdb_node/scripts/app-registry-cli-e2e.sh` for the hermetic
# mock-backed version of the same flow).
#
# After both apps are promoted, Phase 2 clean-install dogfood additionally:
#   - publishes `fkanban-dogfood` with a git-cloneable local source pointer
#     that carries a `run` block + probe entrypoint
#   - installs it from the DEV registry into a clean, isolated install dir
#   - writes/verifies the install receipt
#   - runs it via `lastdb app run fkanban-dogfood --dir … -- --probe`
#
# App ids are `-dogfood` suffixed (not the bare `fbrain`/`fkanban` ids) per
# the `dogfood-registry` app-registry-publish-flow recipe's "rotate throwaway
# app ids" rule: the DEV registry already holds real `fbrain`/`fkanban` rows
# owned by a different developer identity, and registry rows are
# first-write-wins, so reusing those bare ids 409s. The manifest content
# still comes straight from the real app's shipped schemas.ts.
#
# State: an isolated, PERSISTENT per-app dogfood home under
# $HOME/.last-stack/dogfood/app-identity/<app_id> — never Tom's primary
# ~/.lastdb. A dedicated ephemeral lastdbd is started against that home only
# for the `check` / `register-schemas` steps (they declare against a local
# Mini's catalog cache); `publish` / `promote` / `list` / `info` / `install`
# talk to the DEV registry directly and need no local daemon. Reruns reuse
# the same per-app dev-signing key and data dir (idempotent promote; publish
# bumps a local SemVer counter so monotonic version rules stay happy).
#
# Requires a DEV developer API key: EXEMEM_DEV_API_KEY env var, or
# EXEMEM_DEV_API_KEY_FILE pointing at a 0600 file (never printed). Mint one
# with the `app-identity-dev-enroll` skill if you don't have one yet.
#
# Usage:
#   EXEMEM_DEV_API_KEY=em_... ./lastdb_node/scripts/app-registry-first-party-dogfood.sh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
WORKSPACE_ROOT="${EDGEVECTOR_WORKSPACE:-$HOME/code/edgevector}"
DOGFOOD_HOME_ROOT="${APP_REGISTRY_DOGFOOD_HOME:-$HOME/.last-stack/dogfood/app-identity}"
CLI="${LASTDB_CLI:-lastdb}"
DAEMON="${LASTDBD:-lastdbd}"
# Gateway hostnames are owned by folddb_profile/environments.json, never
# hardcoded here (scripts/lints/lint-no-hardcoded-urls.sh enforces this).
DEV_SCHEMA_URL="${FOLD_SCHEMA_SERVICE_URL:-$(jq -r '.environments.dev.schema_service' "$ROOT_DIR/folddb_profile/environments.json")}"
GEN_SCRIPT="$ROOT_DIR/lastdb_node/scripts/generate-app-manifest.ts"

# Clean install target for fkanban-dogfood — reset every run; never ~/.lastdb.
CLEAN_INSTALL_HOME="$DOGFOOD_HOME_ROOT/clean-install-home"
CLEAN_INSTALL_DIR="$CLEAN_INSTALL_HOME/apps/fkanban-dogfood"
FKANBAN_DOGFOOD_ID="fkanban-dogfood"

fail() { echo "FAIL: $1" >&2; exit 1; }

API_KEY="${EXEMEM_DEV_API_KEY:-}"
if [ -z "$API_KEY" ] && [ -n "${EXEMEM_DEV_API_KEY_FILE:-}" ]; then
  API_KEY="$(cat "$EXEMEM_DEV_API_KEY_FILE")"
fi
[ -n "$API_KEY" ] || fail "EXEMEM_DEV_API_KEY not set (env or EXEMEM_DEV_API_KEY_FILE). Mint one with the app-identity-dev-enroll skill."

command -v bun >/dev/null 2>&1 || fail "bun not on PATH"
command -v "$CLI" >/dev/null 2>&1 || fail "$CLI not on PATH"
command -v "$DAEMON" >/dev/null 2>&1 || fail "$DAEMON not on PATH"
command -v git >/dev/null 2>&1 || fail "git not on PATH"
command -v jq >/dev/null 2>&1 || fail "jq not on PATH"

PID=""
cleanup() { [ -n "$PID" ] && kill "$PID" 2>/dev/null || true; }
trap cleanup EXIT

start_node() { # $1 = data dir
  rm -f "$1/data/folddb.sock"
  FOLD_SCHEMA_SERVICE_URL="$DEV_SCHEMA_URL" "$DAEMON" --data-dir "$1" >>"$1/lastdbd.log" 2>&1 &
  PID=$!
  for _ in $(seq 1 100); do [ -S "$1/data/folddb.sock" ] && return 0; sleep 0.2; done
  fail "ephemeral lastdbd socket never appeared for $1 (log: $1/lastdbd.log)"
}

stop_node() {
  [ -n "$PID" ] && kill "$PID" 2>/dev/null || true
  wait "$PID" 2>/dev/null || true
  PID=""
}

# Bump a local SemVer patch counter so re-publishes stay monotonic against DEV.
next_version() { # $1 = version file path
  local vf="$1" cur major minor patch
  if [ -f "$vf" ]; then
    cur="$(tr -d '[:space:]' <"$vf")"
    if [[ "$cur" =~ ^([0-9]+)\.([0-9]+)\.([0-9]+)$ ]]; then
      major="${BASH_REMATCH[1]}"
      minor="${BASH_REMATCH[2]}"
      patch="${BASH_REMATCH[3]}"
      echo "${major}.${minor}.$((patch + 1))"
      return 0
    fi
  fi
  echo "0.1.0"
}

# Prepare a local git source repo that install can clone and run can execute.
# The cloned checkout must carry lastdb-app.json with matching app_id + run.
prepare_fkanban_source() { # $1 = source repo dir, $2 = manifest path, $3 = version
  local source_repo="$1" manifest="$2" version="$3"
  mkdir -p "$source_repo"
  if [ ! -d "$source_repo/.git" ]; then
    git -C "$source_repo" init -q
    git -C "$source_repo" config user.email "app-registry-dogfood@example.invalid"
    git -C "$source_repo" config user.name "App Registry Dogfood"
  fi

  cat >"$source_repo/dogfood-probe.sh" <<'EOF'
#!/usr/bin/env sh
# Installed-source probe for fkanban-dogfood clean-install dogfood.
# Proves lastdb app run executed the checkout entrypoint with LastDB env.
set -eu
printf 'dogfood_probe_app_id=%s\n' "${LASTDB_APP_ID:-}"
printf 'dogfood_probe_socket=%s\n' "${LASTDB_SOCKET:-}"
printf 'dogfood_probe_data_dir=%s\n' "${LASTDB_DATA_DIR:-}"
printf 'dogfood_probe_install_dir=%s\n' "${LASTDB_APP_INSTALL_DIR:-}"
printf 'dogfood_probe_source_dir=%s\n' "${LASTDB_APP_SOURCE_DIR:-}"
printf 'dogfood_probe_arg=%s\n' "${1:-}"
test "${LASTDB_APP_ID:-}" = "fkanban-dogfood"
test -n "${LASTDB_SOCKET:-}"
test -n "${LASTDB_DATA_DIR:-}"
test -n "${LASTDB_APP_INSTALL_DIR:-}"
test -n "${LASTDB_APP_SOURCE_DIR:-}"
test "${1:-}" = "--probe"
printf 'dogfood_probe_ok=1\n'
EOF
  chmod +x "$source_repo/dogfood-probe.sh"
  cp "$manifest" "$source_repo/lastdb-app.json"

  git -C "$source_repo" add lastdb-app.json dogfood-probe.sh
  if git -C "$source_repo" diff --cached --quiet; then
    echo "=== $FKANBAN_DOGFOOD_ID: source repo already committed for this content ==="
  else
    git -C "$source_repo" commit -q -m "dogfood source v${version}"
    echo "=== $FKANBAN_DOGFOOD_ID: committed installable source at $source_repo (v${version}) ==="
  fi
}

# Resolve a first-party app's shipped schemas.ts. Workspace portals are thin
# (no src/); prefer an explicit override, then a full workspace checkout, then
# the host-track current install (the live dogfood source of truth).
resolve_schemas_module() { # $1 = app slug (brain|fkanban), $2 = optional override path
  local slug="$1" override="${2:-}"
  if [ -n "$override" ] && [ -f "$override" ]; then
    printf '%s\n' "$override"
    return 0
  fi
  local candidates=(
    "$WORKSPACE_ROOT/$slug/src/schemas.ts"
    "${HOST_TRACK_APPS:-$HOME/.host-track/apps}/$slug/current/src/schemas.ts"
  )
  local c
  for c in "${candidates[@]}"; do
    if [ -f "$c" ]; then
      printf '%s\n' "$c"
      return 0
    fi
  done
  return 1
}

FBRAIN_SCHEMAS="$(resolve_schemas_module brain "${FBRAIN_SCHEMAS_MODULE:-}")" \
  || fail "brain schemas.ts not found (set FBRAIN_SCHEMAS_MODULE or install host-track brain)"
FKANBAN_SCHEMAS="$(resolve_schemas_module fkanban "${FKANBAN_SCHEMAS_MODULE:-}")" \
  || fail "fkanban schemas.ts not found (set FKANBAN_SCHEMAS_MODULE or install host-track fkanban)"

# app_id|schemas_module|pass_types(csv, empty=all)|display_name|description|homepage_url
APPS=(
  "fbrain-dogfood|$FBRAIN_SCHEMAS|design,task,concept,preference,reference,agent,project,spike,sop|fbrain (dogfood)|EdgeVector's CLI brain over fold_db: knowledge records (design, task, concept, preference, reference, agent, project, spike, sop) with semantic search and sharing. Dogfood mirror of the real fbrain app, published under its own throwaway registry id.|https://github.com/EdgeVector/fbrain"
  "fkanban-dogfood|$FKANBAN_SCHEMAS||fkanban (dogfood)|A kanban board over fold_db. Cards move through fixed columns (backlog, todo, doing, done); every change persists in folddb. Dogfood mirror of the real fkanban app, published under its own throwaway registry id.|https://github.com/EdgeVector/fkanban"
)

PROMOTED=()

for entry in "${APPS[@]}"; do
  IFS='|' read -r APP_ID SCHEMAS_MODULE PASS_TYPES DISPLAY_NAME DESCRIPTION HOMEPAGE_URL <<<"$entry"
  [ -f "$SCHEMAS_MODULE" ] || fail "$APP_ID: schemas module not found at $SCHEMAS_MODULE"

  HOME_DIR="$DOGFOOD_HOME_ROOT/$APP_ID"
  mkdir -p "$HOME_DIR"
  MANIFEST="$HOME_DIR/lastdb-app.json"
  VERSION_FILE="$HOME_DIR/app-version"
  VERSION="$(next_version "$VERSION_FILE")"

  # fkanban-dogfood carries a cloneable local source + run probe so Phase 2
  # clean install can prove install+run against the real DEV registry record.
  SOURCE_REPO=""
  GEN_ENV=(
    "APP_ID=$APP_ID"
    "DISPLAY_NAME=$DISPLAY_NAME"
    "DESCRIPTION=$DESCRIPTION"
    "HOMEPAGE_URL=$HOMEPAGE_URL"
    "PASS_TYPES=$PASS_TYPES"
    "VERSION=$VERSION"
    "SCHEMAS_MODULE=$SCHEMAS_MODULE"
    "OUT_FILE=$MANIFEST"
  )
  if [ "$APP_ID" = "$FKANBAN_DOGFOOD_ID" ]; then
    SOURCE_REPO="$HOME_DIR/publish-source"
    mkdir -p "$SOURCE_REPO"
    GEN_ENV+=("SOURCE=$SOURCE_REPO" "RUN_RUNTIME=sh" "RUN_ENTRYPOINT=dogfood-probe.sh")
  fi

  echo "=== $APP_ID: generating manifest v${VERSION} from $SCHEMAS_MODULE ==="
  env "${GEN_ENV[@]}" bun run "$GEN_SCRIPT"

  if [ "$APP_ID" = "$FKANBAN_DOGFOOD_ID" ]; then
    prepare_fkanban_source "$SOURCE_REPO" "$MANIFEST" "$VERSION"
    # Re-copy after probe/git prep so the committed source matches the published
    # registry record exactly (source + run + schemas + version).
    cp "$MANIFEST" "$SOURCE_REPO/lastdb-app.json"
    git -C "$SOURCE_REPO" add lastdb-app.json dogfood-probe.sh
    if ! git -C "$SOURCE_REPO" diff --cached --quiet; then
      git -C "$SOURCE_REPO" commit -q -m "dogfood source v${VERSION} (final manifest)"
    fi
  fi

  if [ ! -f "$HOME_DIR/dev-signing.key" ]; then
    echo "=== $APP_ID: generating dev signing key (first run for this home) ==="
    "$CLI" --data-dir "$HOME_DIR" app dev-init --key-file "$HOME_DIR/dev-signing.key" >/dev/null
  fi

  start_node "$HOME_DIR"

  echo "=== $APP_ID: check ==="
  NOVEL=0
  "$CLI" --data-dir "$HOME_DIR" app check --manifest "$MANIFEST" >"$HOME_DIR/check.out" 2>&1 || NOVEL=1
  cat "$HOME_DIR/check.out"

  echo "=== $APP_ID: publish (reserve sandbox namespace) v${VERSION} ==="
  "$CLI" --data-dir "$HOME_DIR" app publish --manifest "$MANIFEST" --env dev \
    --api-key "$API_KEY" --key-file "$HOME_DIR/dev-signing.key" >"$HOME_DIR/publish.out" 2>&1 \
    || { cat "$HOME_DIR/publish.out" >&2; fail "$APP_ID: publish (reserve) failed (see $HOME_DIR/publish.out)"; }
  cat "$HOME_DIR/publish.out"
  printf '%s\n' "$VERSION" >"$VERSION_FILE"

  if [ "$NOVEL" -eq 1 ]; then
    echo "=== $APP_ID: register-schemas (novel schemas present) ==="
    "$CLI" --data-dir "$HOME_DIR" app register-schemas --manifest "$MANIFEST" --env dev \
      --api-key "$API_KEY" --key-file "$HOME_DIR/dev-signing.key" >"$HOME_DIR/register.out" 2>&1 \
      || { cat "$HOME_DIR/register.out" >&2; fail "$APP_ID: register-schemas failed (see $HOME_DIR/register.out)"; }
    cat "$HOME_DIR/register.out"
  else
    echo "=== $APP_ID: register-schemas skipped (check reported no novel schemas) ==="
  fi

  echo "=== $APP_ID: promote ==="
  "$CLI" --data-dir "$HOME_DIR" app promote --manifest "$MANIFEST" --env dev \
    --api-key "$API_KEY" --key-file "$HOME_DIR/dev-signing.key" >"$HOME_DIR/promote.out" 2>&1 \
    || { cat "$HOME_DIR/promote.out" >&2; fail "$APP_ID: promote failed (see $HOME_DIR/promote.out)"; }
  cat "$HOME_DIR/promote.out"

  stop_node
  PROMOTED+=("$APP_ID")
  echo "PASS $APP_ID: reserve -> register-schemas -> promote against real DEV (v${VERSION})"
done

echo
echo "=== registry readback (no local node needed) ==="
"$CLI" app list --env dev --json >"$DOGFOOD_HOME_ROOT/list.json" 2>&1 \
  || fail "app list --env dev failed (see $DOGFOOD_HOME_ROOT/list.json)"
for APP_ID in "${PROMOTED[@]}"; do
  grep -q "\"$APP_ID\"" "$DOGFOOD_HOME_ROOT/list.json" \
    || fail "registry list is missing $APP_ID (see $DOGFOOD_HOME_ROOT/list.json)"
  echo "--- $APP_ID: app info ---"
  "$CLI" app info "$APP_ID" --env dev
done

# ── Phase 2: clean-home install + run of fkanban-dogfood from DEV ──────────
echo
echo "=== $FKANBAN_DOGFOOD_ID: clean install from DEV registry ==="
# Isolated install home — never Tom's primary ~/.lastdb. Reset install dir so
# each dogfood run proves a fresh source-first install path.
mkdir -p "$CLEAN_INSTALL_HOME"
rm -rf "$CLEAN_INSTALL_DIR"

"$CLI" --data-dir "$CLEAN_INSTALL_HOME" app install "$FKANBAN_DOGFOOD_ID" \
  --env dev --dir "$CLEAN_INSTALL_DIR" --json \
  >"$CLEAN_INSTALL_HOME/install.json" 2>&1 \
  || { cat "$CLEAN_INSTALL_HOME/install.json" >&2
       fail "$FKANBAN_DOGFOOD_ID: app install from DEV failed (artifacts under $CLEAN_INSTALL_HOME)"; }

cat "$CLEAN_INSTALL_HOME/install.json"
grep -q "\"app_id\": \"$FKANBAN_DOGFOOD_ID\"" "$CLEAN_INSTALL_HOME/install.json" \
  || fail "$FKANBAN_DOGFOOD_ID: install output missing app id (see $CLEAN_INSTALL_HOME/install.json)"
grep -q '"tier": "live"' "$CLEAN_INSTALL_HOME/install.json" \
  || fail "$FKANBAN_DOGFOOD_ID: install output missing live tier (see $CLEAN_INSTALL_HOME/install.json)"
[ -f "$CLEAN_INSTALL_DIR/lastdb-app-install.json" ] \
  || fail "$FKANBAN_DOGFOOD_ID: install receipt missing at $CLEAN_INSTALL_DIR/lastdb-app-install.json"
[ -f "$CLEAN_INSTALL_DIR/source/lastdb-app.json" ] \
  || fail "$FKANBAN_DOGFOOD_ID: source checkout missing lastdb-app.json under $CLEAN_INSTALL_DIR/source"
[ -f "$CLEAN_INSTALL_DIR/source/dogfood-probe.sh" ] \
  || fail "$FKANBAN_DOGFOOD_ID: source checkout missing dogfood-probe.sh under $CLEAN_INSTALL_DIR/source"
grep -q "\"app_id\": \"$FKANBAN_DOGFOOD_ID\"" "$CLEAN_INSTALL_DIR/lastdb-app-install.json" \
  || fail "$FKANBAN_DOGFOOD_ID: install receipt missing app id"
grep -q '"tier": "live"' "$CLEAN_INSTALL_DIR/lastdb-app-install.json" \
  || fail "$FKANBAN_DOGFOOD_ID: install receipt missing live tier"
echo "PASS $FKANBAN_DOGFOOD_ID: installed from DEV into clean dir $CLEAN_INSTALL_DIR (receipt written)"

echo
echo "=== $FKANBAN_DOGFOOD_ID: run installed source via lastdb app run --probe ==="
"$CLI" --data-dir "$CLEAN_INSTALL_HOME" app run "$FKANBAN_DOGFOOD_ID" \
  --dir "$CLEAN_INSTALL_DIR" -- --probe \
  >"$CLEAN_INSTALL_HOME/run.out" 2>&1 \
  || { cat "$CLEAN_INSTALL_HOME/run.out" >&2
       fail "$FKANBAN_DOGFOOD_ID: app run failed (see $CLEAN_INSTALL_HOME/run.out)"; }

cat "$CLEAN_INSTALL_HOME/run.out"
grep -q "dogfood_probe_app_id=$FKANBAN_DOGFOOD_ID" "$CLEAN_INSTALL_HOME/run.out" \
  || fail "$FKANBAN_DOGFOOD_ID: run output missing probe app id (see $CLEAN_INSTALL_HOME/run.out)"
grep -q "dogfood_probe_arg=--probe" "$CLEAN_INSTALL_HOME/run.out" \
  || fail "$FKANBAN_DOGFOOD_ID: run output missing --probe arg (see $CLEAN_INSTALL_HOME/run.out)"
grep -q "dogfood_probe_ok=1" "$CLEAN_INSTALL_HOME/run.out" \
  || fail "$FKANBAN_DOGFOOD_ID: run probe did not report ok (see $CLEAN_INSTALL_HOME/run.out)"
grep -q "app run completed: $FKANBAN_DOGFOOD_ID" "$CLEAN_INSTALL_HOME/run.out" \
  || fail "$FKANBAN_DOGFOOD_ID: CLI did not report run completion (see $CLEAN_INSTALL_HOME/run.out)"
echo "PASS $FKANBAN_DOGFOOD_ID: promoted, installed from DEV, and run from installed source checkout"

echo
echo "PASS app-registry-first-party-dogfood: ${PROMOTED[*]} reserved/schema-registered/promoted; $FKANBAN_DOGFOOD_ID clean-installed + run from DEV ($("$CLI" --version 2>/dev/null || true))"
