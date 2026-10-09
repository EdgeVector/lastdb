#!/usr/bin/env bash
# Install the committed git hooks for this clone (covers every worktree).
# Idempotent — safe to re-run; run.sh calls it on every dev launch.
#
# Why committed hooks: keeps every checkout gated on `cargo fmt --all --check`,
# the no-hardcoded-URLs lint, lint-no-build-artifacts (pre-commit +
# pre-push), and Mini-lane `cargo clippy -p lastdb_node --lib --bins`
# (pre-push, matching Forge CI) without a separate tool like husky or
# pre-commit. A fresh clone never ran this, so its `.git/hooks/` holds only
# `.sample` files and the fmt/clippy gates silently don't fire — unformatted
# or clippy-dirty code then slips to CI as a confusing Mini red. This script
# (auto-invoked by run.sh) closes that gap.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
HOOKS_SRC="$REPO_ROOT/hooks"

# Resolve the destination hooks directory. Two cases must be handled:
#
#  1. `core.hooksPath` is set — git ignores `$GIT_DIR/hooks` entirely and
#     only consults the configured path, so we MUST install there or the
#     hooks we copy are dead. (Tom's main clone pins it to an absolute
#     `…/fold/.git/hooks`; a stale absolute path from a moved/renamed clone
#     would otherwise route commits at a non-existent dir — we repoint it.)
#  2. No `core.hooksPath` (the default, and what a fresh clone has) — git
#     uses `$GIT_COMMON_DIR/hooks`. `--git-common-dir` (not `--git-dir`)
#     is the key: in a linked worktree `--git-dir` points at
#     `.git/worktrees/<name>/` whose hooks git never runs, while the common
#     dir is the shared `.git/` whose hooks ALL worktrees inherit. Installing
#     once there covers every present and future worktree of this clone.
hooks_path="$(git -C "$REPO_ROOT" config --get core.hooksPath || true)"
if [ -n "$hooks_path" ]; then
    case "$hooks_path" in
        /*) HOOKS_DST="$hooks_path" ;;
        *)  HOOKS_DST="$REPO_ROOT/$hooks_path" ;;
    esac
    # A stale absolute hooksPath (clone was moved/renamed) routes commits at
    # a dead dir. Repoint it at this clone's real shared hooks dir so the
    # gate fires instead of silently no-op'ing.
    if [ ! -d "$HOOKS_DST" ]; then
        common_dir="$(git -C "$REPO_ROOT" rev-parse --git-common-dir)"
        case "$common_dir" in
            /*) HOOKS_DST="$common_dir/hooks" ;;
            *)  HOOKS_DST="$REPO_ROOT/$common_dir/hooks" ;;
        esac
        echo "note: core.hooksPath '$hooks_path' missing — repointing at $HOOKS_DST"
        git -C "$REPO_ROOT" config core.hooksPath "$HOOKS_DST"
    fi
else
    # Default layout: shared common dir's hooks (inherited by all worktrees).
    common_dir="$(git -C "$REPO_ROOT" rev-parse --git-common-dir)"
    case "$common_dir" in
        /*) HOOKS_DST="$common_dir/hooks" ;;
        *)  HOOKS_DST="$REPO_ROOT/$common_dir/hooks" ;;
    esac
fi

mkdir -p "$HOOKS_DST"
for hook in "$HOOKS_SRC"/*; do
    name=$(basename "$hook")
    # Skip the no-op when the installed hook is already byte-identical, so
    # the per-launch run.sh call stays quiet on the common path.
    if [ -f "$HOOKS_DST/$name" ] && cmp -s "$hook" "$HOOKS_DST/$name"; then
        continue
    fi
    cp "$hook" "$HOOKS_DST/$name"
    chmod +x "$HOOKS_DST/$name"
    echo "installed: $HOOKS_DST/$name"
done
