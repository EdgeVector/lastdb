#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'EOF'
usage: with-target-repo-cwd.sh [--workspace ROOT] TARGET_REPO -- COMMAND [ARG...]

Resolve TARGET_REPO, verify that it is a concrete git checkout under the
workspace, cd there, then exec COMMAND. Use this wrapper before launching tools
whose worktree isolation is derived from the parent process cwd.

TARGET_REPO may be an absolute checkout path or an owner/name token such as
EdgeVector/fold, which resolves to ROOT/fold.
EOF
}

workspace_root="${FOLD_WORKSPACE_ROOT:-}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --workspace)
      [[ $# -ge 2 ]] || { usage; exit 64; }
      workspace_root="$2"
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    --)
      usage
      exit 64
      ;;
    -*)
      echo "with-target-repo-cwd: unknown option: $1" >&2
      usage
      exit 64
      ;;
    *)
      break
      ;;
  esac
done

[[ $# -ge 3 ]] || { usage; exit 64; }

target_repo_arg="$1"
shift
[[ "${1:-}" == "--" ]] || { usage; exit 64; }
shift
[[ $# -gt 0 ]] || { usage; exit 64; }

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
repo_root="$(git -C "$script_dir/../.." rev-parse --show-toplevel)"

if [[ -z "$workspace_root" ]]; then
  workspace_root="$(cd "$repo_root/.." && pwd -P)"
else
  workspace_root="$(cd "$workspace_root" && pwd -P)"
fi

resolve_target_repo() {
  local target="$1"
  if [[ "$target" = /* ]]; then
    printf '%s\n' "$target"
    return
  fi

  if [[ "$target" == */* ]]; then
    local repo_name="${target##*/}"
    printf '%s/%s\n' "$workspace_root" "$repo_name"
    return
  fi

  printf '%s/%s\n' "$workspace_root" "$target"
}

target_repo="$(resolve_target_repo "$target_repo_arg")"
target_repo="$(cd "$target_repo" && pwd -P)"

if [[ -z "$target_repo" || "$target_repo" == "$workspace_root" ]]; then
  echo "with-target-repo-cwd: refusing ambiguous target checkout: $target_repo_arg" >&2
  exit 65
fi

last_stack="${LAST_STACK_ROOT:-$HOME/.last-stack}"
repo_guard="$last_stack/bin/last-stack-repo-op-guard"
if [[ -x "$repo_guard" ]]; then
  target_repo="$("$repo_guard" "$target_repo" "$workspace_root")"
else
  target_repo="$(git -C "$target_repo" rev-parse --show-toplevel)"
fi

git_top="$(git -C "$target_repo" rev-parse --show-toplevel)"
if [[ "$target_repo" != "$git_top" ]]; then
  echo "with-target-repo-cwd: target is not the git top-level: $target_repo" >&2
  echo "with-target-repo-cwd: git top-level is: $git_top" >&2
  exit 65
fi

cd "$target_repo"
exec "$@"
