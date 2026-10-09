#!/usr/bin/env bash
set -euo pipefail

parent_repo="${1:-.}"
submodule_path="${2:-fold}"
main_ref="${3:-origin/main}"

parent_root="$(git -C "$parent_repo" rev-parse --show-toplevel)"
gitlink_line="$(git -C "$parent_root" ls-tree HEAD "$submodule_path" || true)"

if [[ -z "$gitlink_line" ]]; then
  echo "verify-fold-gitlink-main: no gitlink at $submodule_path in $parent_root" >&2
  exit 1
fi

gitlink_mode="$(awk '{print $1}' <<<"$gitlink_line")"
gitlink_type="$(awk '{print $2}' <<<"$gitlink_line")"
gitlink_sha="$(awk '{print $3}' <<<"$gitlink_line")"

if [[ "$gitlink_mode" != "160000" || "$gitlink_type" != "commit" || -z "$gitlink_sha" ]]; then
  echo "verify-fold-gitlink-main: $submodule_path is not a git submodule gitlink in $parent_root" >&2
  exit 1
fi

submodule_root="$parent_root/$submodule_path"
if [[ ! -d "$submodule_root/.git" && ! -f "$submodule_root/.git" ]]; then
  echo "verify-fold-gitlink-main: $submodule_path is not checked out; run git submodule update --init $submodule_path" >&2
  exit 1
fi

git -C "$submodule_root" fetch origin main >/dev/null

if ! git -C "$submodule_root" cat-file -e "$gitlink_sha^{commit}" 2>/dev/null; then
  echo "verify-fold-gitlink-main: $submodule_path points at $gitlink_sha, which is not present after fetching origin/main" >&2
  echo "verify-fold-gitlink-main: update the gitlink to a fold commit reachable from $main_ref" >&2
  exit 1
fi

if ! git -C "$submodule_root" merge-base --is-ancestor "$gitlink_sha" "$main_ref"; then
  echo "verify-fold-gitlink-main: $submodule_path points at $gitlink_sha, which is not reachable from fold $main_ref" >&2
  echo "verify-fold-gitlink-main: downstream deploy repos must pin fold to a merged main commit, not an off-main/cherry-picked commit" >&2
  exit 1
fi

echo "verify-fold-gitlink-main: ok - $submodule_path gitlink $gitlink_sha is reachable from fold $main_ref"
