#!/usr/bin/env bash
# Install the hourly schema-eval routine into a RUN home.
#
# Product code stays in fold (DEV). This copies the eval tree + a prebuilt
# schema_service binary into ~/.local/share/edgevector/schema-eval at a pinned
# SHA and renders a LaunchAgent that never points at ~/code/edgevector/*.
#
#   schema_service/eval/scripts/install-launchd.sh install --from <fold-root>
#   schema_service/eval/scripts/install-launchd.sh refresh --from <fold-root>
#   schema_service/eval/scripts/install-launchd.sh status
#   schema_service/eval/scripts/install-launchd.sh uninstall
set -euo pipefail

LABEL="com.tomtang.schema-eval-routine"
DEFAULT_RUN_HOME="${HOME}/.local/share/edgevector/schema-eval"
DEFAULT_STATE_DIR="${HOME}/.schema-eval"
PLIST_DST="${HOME}/Library/LaunchAgents/${LABEL}.plist"
UID_NUM="$(id -u)"
DOMAIN="gui/${UID_NUM}"
LAUNCHD_PATH="${HOME}/.local/bin:${HOME}/.bun/bin:${HOME}/.cargo/bin:/usr/bin:/opt/homebrew/bin:/opt/homebrew/sbin:/usr/local/bin:/bin:/usr/sbin:/sbin"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
EVAL_SRC="$(cd "$SCRIPT_DIR/.." && pwd)"

RUN_HOME="${SCHEMA_EVAL_RUN_HOME:-$DEFAULT_RUN_HOME}"
STATE_DIR="${SCHEMA_EVAL_STATE_DIR:-$DEFAULT_STATE_DIR}"
FROM=""
SKIP_BUILD=0
SKIP_LAUNCHCTL=0
NO_LOAD=0

usage() {
  cat <<EOF
Usage: $0 <install|refresh|uninstall|status> [options]

  install   Copy eval tree + prebuilt binary into RUN home, render plist, load
  refresh   Rebuild binary + recopy eval tree when fold main moved, reload
  uninstall Bootout and remove the installed plist (leaves RUN/STATE data)
  status    Show launchctl + RUN home pin + binary

Options:
  --from <fold-root>   fold checkout used to copy eval/ and cargo-build
  --run-home <dir>     RUN home (default: $DEFAULT_RUN_HOME)
  --state-dir <dir>    STATE dir (default: $DEFAULT_STATE_DIR)
  --skip-build         reuse existing RUN-home binary (tests / dry copies)
  --no-load            render files but do not launchctl bootstrap
EOF
}

die() { echo "error: $*" >&2; exit 1; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --from) FROM="${2:-}"; shift 2 ;;
    --run-home) RUN_HOME="${2:-}"; shift 2 ;;
    --state-dir) STATE_DIR="${2:-}"; shift 2 ;;
    --skip-build) SKIP_BUILD=1; shift ;;
    --no-load) NO_LOAD=1; shift ;;
    -h|--help) usage; exit 0 ;;
    --*) die "unknown option $1" ;;
    *) break ;;
  esac
done

CMD="${1:-}"
shift || true
while [[ $# -gt 0 ]]; do
  case "$1" in
    --from) FROM="${2:-}"; shift 2 ;;
    --run-home) RUN_HOME="${2:-}"; shift 2 ;;
    --state-dir) STATE_DIR="${2:-}"; shift 2 ;;
    --skip-build) SKIP_BUILD=1; shift ;;
    --no-load) NO_LOAD=1; shift ;;
    *) die "unexpected argument $1" ;;
  esac
done

resolve_from() {
  if [[ -n "$FROM" ]]; then
    FROM="$(cd "$FROM" && pwd)"
    return
  fi
  if [[ -f "$EVAL_SRC/../../Cargo.toml" ]]; then
    FROM="$(cd "$EVAL_SRC/../.." && pwd)"
    return
  fi
  die "pass --from <fold-root> (no fold Cargo.toml above this eval tree)"
}

copy_eval_tree() {
  local dest="$1"
  mkdir -p "$dest/bin" "$dest/launchd" "$dest/scripts" "$dest/test"
  # Copy product files only — never results/, .cache/, or a compiled bin.
  local f
  for f in "$EVAL_SRC"/*.mjs "$EVAL_SRC"/*.json "$EVAL_SRC"/*.sh "$EVAL_SRC"/*.md "$EVAL_SRC"/.gitignore; do
    [[ -e "$f" ]] || continue
    cp "$f" "$dest/"
  done
  [[ -d "$EVAL_SRC/launchd" ]] && cp -R "$EVAL_SRC/launchd/." "$dest/launchd/"
  [[ -d "$EVAL_SRC/scripts" ]] && cp -R "$EVAL_SRC/scripts/." "$dest/scripts/"
  [[ -d "$EVAL_SRC/test" ]] && cp -R "$EVAL_SRC/test/." "$dest/test/"
  chmod +x "$dest/routine.sh" "$dest/scripts/install-launchd.sh"
}

write_pin() {
  local dest="$1"
  local sha="unknown"
  if git -C "$FROM" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    sha="$(git -C "$FROM" rev-parse HEAD)"
  fi
  printf '%s\n' "$sha" > "$dest/PINNED_SHA"
  printf '%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" > "$dest/PINNED_AT"
}

build_binary() {
  local dest="$1"
  if [[ "$SKIP_BUILD" -eq 1 ]]; then
    if [[ -x "$dest/bin/schema_service" ]]; then
      echo "skip-build: reusing $dest/bin/schema_service"
      return
    fi
    echo "skip-build: no existing binary (tests may stub later)"
    return
  fi
  [[ -f "$FROM/Cargo.toml" ]] || die "not a fold checkout: $FROM"
  echo "building schema_service --features fastembed from $FROM …"
  cargo build --quiet -p schema_service_server_http --bin schema_service \
    --features fastembed --manifest-path "$FROM/Cargo.toml"
  local built="$FROM/target/debug/schema_service"
  [[ -x "$built" ]] || die "cargo build did not produce $built"
  mkdir -p "$dest/bin"
  cp "$built" "$dest/bin/schema_service"
  chmod +x "$dest/bin/schema_service"
}

render_plist() {
  local dest="$1"
  local out="$2"
  mkdir -p "$(dirname "$out")" "$STATE_DIR"
  local src="$dest/launchd/${LABEL}.plist"
  [[ -f "$src" ]] || die "missing plist template $src"
  local kanban="${KANBAN_BIN:-$HOME/.local/bin/kanban}"
  local brain="${BRAIN_BIN:-$HOME/.local/bin/brain}"
  python3 - "$src" "$out" "$dest" "$STATE_DIR" "$LAUNCHD_PATH" "$HOME" "$kanban" "$brain" <<'PY'
import pathlib
import sys

src, dst, run_home, state_dir, launchd_path, home, kanban, brain = sys.argv[1:]
text = pathlib.Path(src).read_text()
repl = {
    "REPLACE_ROUTINE_SH": str(pathlib.Path(run_home) / "routine.sh"),
    "REPLACE_RUN_HOME": run_home,
    "REPLACE_LOG": str(pathlib.Path(state_dir) / "routine.log"),
    "REPLACE_PATH": launchd_path,
    "REPLACE_BIN": str(pathlib.Path(run_home) / "bin" / "schema_service"),
    "REPLACE_STATE": state_dir,
    "REPLACE_KANBAN": kanban,
    "REPLACE_BRAIN": brain,
}
for k, v in repl.items():
    text = text.replace(k, v)
if "REPLACE_" in text:
    raise SystemExit("unexpanded REPLACE_ token remains in plist")
if "/code/edgevector/" in text:
    raise SystemExit("plist still points at a portal checkout")
pathlib.Path(dst).write_text(text)
print(f"rendered {dst}")
print(f"  WorkingDirectory: {run_home}")
print(f"  SCHEMA_EVAL_SERVER_BIN: {repl['REPLACE_BIN']}")
PY
}

do_install() {
  resolve_from
  mkdir -p "$RUN_HOME" "$STATE_DIR"
  copy_eval_tree "$RUN_HOME"
  build_binary "$RUN_HOME"
  write_pin "$RUN_HOME"
  # Hourly job runs from RUN home, not the DEV worktree we copied from.
  EVAL_SRC="$RUN_HOME"
  local rendered="$RUN_HOME/launchd/rendered.plist"
  render_plist "$RUN_HOME" "$rendered"
  if [[ "$NO_LOAD" -eq 1 ]]; then
    echo "no-load: files written, launchctl skipped"
    echo "RUN_HOME=$RUN_HOME"
    echo "STATE_DIR=$STATE_DIR"
    echo "PLIST=$rendered"
    return
  fi
  mkdir -p "$(dirname "$PLIST_DST")"
  cp "$rendered" "$PLIST_DST"
  launchctl bootout "$DOMAIN/$LABEL" 2>/dev/null || true
  launchctl bootstrap "$DOMAIN" "$PLIST_DST"
  launchctl enable "$DOMAIN/$LABEL" 2>/dev/null || true
  echo "Installed: $LABEL"
  echo "RUN_HOME: $RUN_HOME"
  echo "STATE:    $STATE_DIR"
  echo "Logs:     $STATE_DIR/routine.log"
}

cmd_status() {
  launchctl print "$DOMAIN/$LABEL" 2>/dev/null | head -40 || echo "(not loaded)"
  echo
  echo "RUN_HOME: $RUN_HOME"
  if [[ -f "$RUN_HOME/PINNED_SHA" ]]; then
    echo "PINNED_SHA: $(cat "$RUN_HOME/PINNED_SHA")"
  fi
  if [[ -x "$RUN_HOME/bin/schema_service" ]]; then
    echo "BIN: $RUN_HOME/bin/schema_service"
  else
    echo "BIN: missing"
  fi
  if [[ -f "$PLIST_DST" ]]; then
    echo "Installed plist:"
    plutil -p "$PLIST_DST" 2>/dev/null | head -50 || true
  fi
}

case "$CMD" in
  install) do_install ;;
  refresh)
    resolve_from
    EVAL_SRC="$(cd "$FROM/schema_service/eval" && pwd)"
    do_install
    ;;
  uninstall)
    launchctl bootout "$DOMAIN/$LABEL" 2>/dev/null || true
    rm -f "$PLIST_DST"
    echo "Uninstalled $LABEL"
    ;;
  status) cmd_status ;;
  *) usage; exit 1 ;;
esac
