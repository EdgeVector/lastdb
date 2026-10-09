#!/bin/zsh
# Wrapper for the hourly schema-eval routine under launchd.
#
# launchd starts jobs with a minimal PATH. Host-track CLIs live in ~/.local/bin.
# Do not point at ~/code/edgevector/* portals — those have no product tree.
set -u
export PATH="${HOME}/.local/bin:${HOME}/.bun/bin:${HOME}/.cargo/bin:/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin:${PATH:-}"

EVAL_DIR="${0:A:h}"
STATE_DIR="${SCHEMA_EVAL_STATE_DIR:-$HOME/.schema-eval}"
mkdir -p "$STATE_DIR"

if [[ -z "${SCHEMA_EVAL_SERVER_BIN:-}" && -x "$EVAL_DIR/bin/schema_service" ]]; then
  export SCHEMA_EVAL_SERVER_BIN="$EVAL_DIR/bin/schema_service"
fi

cd "$EVAL_DIR" || exit 1
echo "===== $(date -u '+%Y-%m-%dT%H:%M:%SZ') schema-eval routine start ====="
set +e
node routine.mjs --file
node_rc=$?
set -e
echo "===== $(date -u '+%Y-%m-%dT%H:%M:%SZ') schema-eval routine end (exit $node_rc) ====="
exit "$node_rc"
