#!/usr/bin/env bash
# Shared release preflight. The caller supplies fail() and require_tool().
require_dev_tap() {
  local dir="$1" repo="${2:-EdgeVector/homebrew-lastdb}" canonical expected
  local portal cache common admin branch marker_slug marker_branch marker_created
  [ "$repo" = EdgeVector/homebrew-lastdb ] || fail "publication requires the exact GitHub tap"
  [ -f "$dir/.git" ] || fail "publish requires a registered DEV worktree"
  canonical="$(cd "$dir" && pwd -P)"
  [ "$(dirname "$canonical")" = "$(cd "$HOME/.fkanban/worktrees" && pwd -P)" ] || fail "publication requires a portal DEV root"
  portal="$HOME/code/edgevector/homebrew-lastdb/.portal"
  [ "$(cat "$portal/slug")" = homebrew-lastdb ] || fail "tap portal slug differs"
  [ "$(cat "$portal/venue")" = github ] || fail "tap portal venue differs"
  cache="$(cat "$portal/cache")"
  [[ "$cache" = /* ]] || fail "tap portal cache must be absolute"
  cache="$(cd "$cache" && pwd -P)"
  [ -f "$canonical/.portal-worktree" ] || fail "tap portal marker is absent"
  [ "$(wc -c < "$canonical/.portal-worktree")" -le 4096 ] || fail "tap portal marker exceeds its bound"
  [ "$(wc -l < "$canonical/.portal-worktree")" -eq 3 ] || fail "tap portal marker shape differs"
  marker_slug="$(sed -n 's/^slug=//p' "$canonical/.portal-worktree")"
  marker_branch="$(sed -n 's/^branch=//p' "$canonical/.portal-worktree")"
  marker_created="$(sed -n 's/^created=//p' "$canonical/.portal-worktree")"
  branch="$(git -C "$canonical" symbolic-ref --short HEAD)"
  [ "$marker_slug" = homebrew-lastdb ] && [ "$marker_branch" = "$branch" ] || fail "tap portal marker identity differs"
  [[ "$marker_created" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$ ]] || fail "tap portal marker date differs"
  [ "$(basename "$canonical")" = "homebrew-lastdb-${branch//\//-}" ] || fail "tap portal root name differs"
  [ -z "$(git -C "$canonical" ls-files -- .portal-worktree)" ] || fail "tap portal marker must stay untracked"
  git -C "$canonical" check-ignore -q .portal-worktree || fail "tap portal marker must stay ignored"
  common="$(git -C "$canonical" rev-parse --git-common-dir)"
  [[ "$common" = /* ]] || common="$canonical/$common"
  [ "$(cd "$common" && pwd -P)" = "$cache" ] || fail "tap Git common repository differs from portal cache"
  admin="$(git -C "$canonical" rev-parse --absolute-git-dir)"
  [ "$(dirname "$admin")" = "$cache/worktrees" ] || fail "tap Git registration is outside the portal cache"
  [ "$(cat "$canonical/.git")" = "gitdir: $admin" ] || fail "tap Git registration pointer differs"
  [ "$(cat "$admin/gitdir")" = "$canonical/.git" ] || fail "tap reciprocal Git registration differs"
  [ "$(cd "$admin/$(cat "$admin/commondir")" && pwd -P)" = "$cache" ] || fail "tap registration common repository differs"
  [ "$(git --git-dir "$cache" rev-parse --is-bare-repository)" = true ] || fail "tap portal cache must be bare"
  [ "$(git -C "$dir" rev-parse --show-toplevel)" = "$canonical" ] || fail "tap path must be its DEV root"
  [ -z "$(git -C "$dir" status --porcelain)" ] || fail "tap DEV worktree is not clean"
  expected="https://github.com/${repo}.git"
  [ "$(git -C "$dir" remote get-url --all origin)" = "$expected" ] || fail "tap fetch URLs differ from GitHub"
  [ "$(git -C "$dir" remote get-url --push --all origin)" = "$expected" ] || fail "tap push URLs differ from GitHub"
  [ "$(head -n1 "$dir/.last-stack/pr-venue" 2>/dev/null || true)" = github ] || fail "tap source must declare GitHub"
  git -C "$dir" fetch --quiet origin main 2>/dev/null || fail "tap accepted-main fetch refused"
  [ "$(git -C "$dir" rev-parse HEAD)" = "$(git -C "$dir" rev-parse origin/main)" ] || fail "tap DEV must start at current accepted main"
  require_tool last-stack-pr-venue
  last-stack-pr-venue "$repo" "$dir" >/dev/null 2>&1 || fail "tap venue check refused"
}

require_formula_change() {
  local dir="$1" changed
  changed="$(git -C "$dir" diff --name-only)"
  case "$changed" in
    $'Formula/folddb.rb\nFormula/lastdb.rb'|Formula/lastdb.rb|Formula/folddb.rb) ;;
    *) fail "tap changed outside the two formula files, or no formula change exists" ;;
  esac
  [ -z "$(git -C "$dir" ls-files --others --exclude-standard)" ] || fail "tap gained untracked files"
}
