#!/usr/bin/env bash
#
# Promote source-bound LastDB Mini artifacts to the public Homebrew release
# boundary.
#
# - Bottle CDN: GitHub Releases on EdgeVector/homebrew-lastdb (anonymous HTTPS
#   for `brew`).
# - Formula SoT: GitHub `EdgeVector/homebrew-lastdb` PR (the tap moved
#   to GitHub, brain decision-2026-09-29-retire-lastgit-all-repos-to-github).
# - Dry-run validates the same artifact metadata and formula bump path without
#   network writes. Accepts full clones and git worktrees (`.git` file or dir).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BUMP_SCRIPT="$ROOT/scripts/release/bump-homebrew-formula.rb"

usage() {
  cat <<'EOF'
Usage:
  forge-promote-homebrew-stable.sh --version-tag vX.Y.Z --artifact-dir DIR --tap-dir DIR --dry-run [options]
  forge-promote-homebrew-stable.sh --version-tag vX.Y.Z --artifact-dir DIR --tap-dir DEV --publish [options]

Options:
  --version-tag TAG       Stable tag to promote. Must be vX.Y.Z-style and not a prerelease.
  --artifact-dir DIR      Directory containing lastdb-*.tar.gz and matching manifests.
  --tap-dir DIR           Existing homebrew-lastdb checkout. Publish requires a clean
                          registered DEV worktree at accepted GitHub main.
  --require-dev-worktree  Require the registered DEV publication contract.
  --source-git-oid OID    Expected source OID recorded in manifests.
  --release-repo REPO     Public bottle release repository (default: EdgeVector/homebrew-lastdb).
  --formula-venue github  Compatibility with the installed release caller; GitHub is the only venue.
  --proof-report FILE     Write machine-readable promotion proof JSON.
  --dry-run               Validate release + formula bump path without network writes.
  --publish               Create public bottle release + formula PR on the tap
                          under the normal GitHub release and required PR gates.
  --help | -h             Show this help.
EOF
}

fail() {
  echo "forge-promote-homebrew-stable: $*" >&2
  exit 64
}

require_tool() {
  command -v "$1" >/dev/null 2>&1 || fail "missing required tool: $1"
}

sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

# True for normal clones (.git dir) and linked worktrees (.git file with gitdir:).
is_git_checkout() {
  local dir="$1"
  git -C "$dir" rev-parse --is-inside-work-tree >/dev/null 2>&1
}

# shellcheck source=homebrew-tap-dev.sh
. "$ROOT/scripts/release/homebrew-tap-dev.sh"

version_tag=""
artifact_dir=""
tap_dir=""
source_git_oid=""
release_repo="EdgeVector/homebrew-lastdb"
proof_report=""
mode=""

while [ "$#" -gt 0 ]; do
  case "$1" in
    --version-tag) [ "$#" -ge 2 ] || fail "missing value for --version-tag"; version_tag="$2"; shift 2 ;;
    --artifact-dir) [ "$#" -ge 2 ] || fail "missing value for --artifact-dir"; artifact_dir="$2"; shift 2 ;;
    --tap-dir) [ "$#" -ge 2 ] || fail "missing value for --tap-dir"; tap_dir="$2"; shift 2 ;;
    --require-dev-worktree) shift ;;
    --source-git-oid) [ "$#" -ge 2 ] || fail "missing value for --source-git-oid"; source_git_oid="$2"; shift 2 ;;
    --release-repo) [ "$#" -ge 2 ] || fail "missing value for --release-repo"; release_repo="$2"; shift 2 ;;
    --formula-venue)
      [ "$#" -ge 2 ] || fail "missing value for --formula-venue"
      [ "$2" = "github" ] || fail "formula venue must be github"
      shift 2 ;;
    --proof-report) [ "$#" -ge 2 ] || fail "missing value for --proof-report"; proof_report="$2"; shift 2 ;;
    --dry-run|--publish) [ -z "$mode" ] || fail "choose exactly one mode"; mode="${1#--}"; shift ;;
    --help|-h) usage; exit 0 ;;
    *) fail "unknown argument: $1" ;;
  esac
done

[ -n "$version_tag" ] || fail "--version-tag is required"
[ -n "$artifact_dir" ] || fail "--artifact-dir is required"
[ -n "$mode" ] || fail "choose exactly one of --dry-run or --publish"
[ -d "$artifact_dir" ] || fail "artifact directory not found: $artifact_dir"
[ -x "$BUMP_SCRIPT" ] || fail "missing bump script: $BUMP_SCRIPT"

case "$version_tag" in
  *-*) fail "stable promotion refuses prerelease tag: $version_tag" ;;
esac
[[ "$version_tag" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] ||
  fail "--version-tag must look like vX.Y.Z (got $version_tag)"

require_tool git
require_tool ruby
require_tool python3
if ! command -v shasum >/dev/null 2>&1 && ! command -v sha256sum >/dev/null 2>&1; then
  fail "missing required tool: shasum or sha256sum"
fi
if [ "$mode" = "publish" ]; then
  require_tool gh
fi
[ -n "$tap_dir" ] || fail "--tap-dir is required"
if [ "$mode" = "publish" ]; then
  [[ "$source_git_oid" =~ ^[0-9a-f]{40}$ ]] || fail "publish requires an exact source OID"
fi
if [ "$mode" = "publish" ]; then
  require_dev_tap "$tap_dir" "$release_repo"
  [ -n "$proof_report" ] || fail "publish requires a durable --proof-report path"
  [ ! -e "$proof_report" ] && [ ! -L "$proof_report" ] || fail "an exact prior promotion receipt exists; reconcile it before retry"
fi

tmp="$(mktemp -d "${TMPDIR:-/tmp}/forge-promote-homebrew.XXXXXX")"
trap 'rm -rf "$tmp"' EXIT
release_dir="$tmp/release"
tap_work="$tmp/tap"
mkdir -p "$release_dir"

find "$artifact_dir" -maxdepth 1 -type f -name 'lastdb-*.tar.gz' -exec cp {} "$release_dir/" \;
find "$artifact_dir" -maxdepth 1 -type f -name 'lastdb-*.manifest.json' -exec cp {} "$release_dir/" \;

if ! ls "$release_dir"/lastdb-*.tar.gz >/dev/null 2>&1; then
  fail "no lastdb-*.tar.gz artifacts found under $artifact_dir"
fi

(
  cd "$release_dir"
  : > SHA256SUMS.txt
  for tarball in lastdb-*.tar.gz; do
    printf '%s  %s\n' "$(sha256_file "$tarball")" "$tarball" >> SHA256SUMS.txt
  done
)

artifact_report="$tmp/artifacts.json"
python3 - "$release_dir" "$source_git_oid" "$artifact_report" <<'PY'
import hashlib
import json
import os
import sys

release_dir, expected_oid, report_path = sys.argv[1:]

def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

items = []
for name in sorted(os.listdir(release_dir)):
    if not name.endswith(".tar.gz") or not name.startswith("lastdb-"):
        continue
    target = name[len("lastdb-") : -len(".tar.gz")]
    tarball = os.path.join(release_dir, name)
    manifest_name = f"lastdb-{target}.manifest.json"
    manifest_path = os.path.join(release_dir, manifest_name)
    if not os.path.exists(manifest_path):
        raise SystemExit(f"missing manifest for {name}: {manifest_name}")
    with open(manifest_path, encoding="utf-8") as fh:
        manifest = json.load(fh)
    if manifest.get("schema") != "lastdb.mini.bundle.v1":
        raise SystemExit(f"{manifest_name}: unexpected schema {manifest.get('schema')!r}")
    if manifest.get("target") != target:
        raise SystemExit(f"{manifest_name}: target mismatch {manifest.get('target')!r} != {target!r}")
    if expected_oid and manifest.get("source_git_oid") != expected_oid:
        raise SystemExit(
            f"{manifest_name}: source_git_oid mismatch {manifest.get('source_git_oid')!r} != {expected_oid!r}"
        )
    tar_sha = sha256_file(tarball)
    if manifest.get("artifact", {}).get("sha256") != tar_sha:
        raise SystemExit(f"{manifest_name}: artifact sha256 does not match {name}")
    binary_names = {item.get("name") for item in manifest.get("binaries", [])}
    if binary_names != {"lastdb", "lastdbd"}:
        raise SystemExit(f"{manifest_name}: expected lastdb + lastdbd binaries, got {sorted(binary_names)}")
    versions = {item.get("version") for item in manifest.get("binaries", [])}
    if len(versions) != 1:
        raise SystemExit(f"{manifest_name}: binary versions differ")
    items.append(
        {
            "target": target,
            "tarball": name,
            "manifest": manifest_name,
            "sha256": tar_sha,
            "version": sorted(versions)[0],
            "source_git_oid": manifest.get("source_git_oid", ""),
        }
    )

if not items:
    raise SystemExit("no validated tarball artifacts")
if not any(item["target"] == "aarch64-apple-darwin" for item in items):
    raise SystemExit("missing required aarch64-apple-darwin Homebrew artifact")

with open(report_path, "w", encoding="utf-8") as fh:
    json.dump(items, fh, indent=2, sort_keys=True)
    fh.write("\n")
PY

sha_aarch64_darwin="$(awk '$2 == "lastdb-aarch64-apple-darwin.tar.gz" { print $1 }' "$release_dir/SHA256SUMS.txt")"
[ -n "$sha_aarch64_darwin" ] || fail "could not resolve aarch64 darwin sha from SHA256SUMS.txt"
version="${version_tag#v}"
tap_base="$(git -C "$tap_dir" rev-parse HEAD)"

[ -d "$tap_dir/Formula" ] || fail "tap Formula directory is missing"
is_git_checkout "$tap_dir" || fail "tap checkout is not a Git worktree"
if [ "$mode" = "publish" ]; then
  # Publish only from the caller's real DEV history. Never replace it with a clone.
  tap_work="$(cd "$tap_dir" && pwd -P)"
else
  # Read-only preview retains the caller checkout and uses owned scratch data.
  mkdir -p "$tap_work"
  if [ -d "$tap_dir/.git" ]; then
    cp -R "$tap_dir"/. "$tap_work"/
  else
    if command -v rsync >/dev/null 2>&1; then
      rsync -a --exclude '.git' "$tap_dir"/ "$tap_work"/
    else
      tar -C "$tap_dir" --exclude '.git' -cf - . | tar -C "$tap_work" -xf -
    fi
    git -C "$tap_work" init -q
    git -C "$tap_work" -c user.name="LastDB preview" -c user.email="preview@localhost" add -A
    git -C "$tap_work" -c user.name="LastDB preview" -c user.email="preview@localhost" commit -qm "tap preview"
  fi
fi

for formula in lastdb folddb; do
  [ -f "$tap_work/Formula/${formula}.rb" ] || fail "missing tap formula: Formula/${formula}.rb"
done

is_git_checkout "$tap_work" || fail "tap checkout must be a git worktree so the formula diff guard can run"

status_file="$tmp/tap-status.txt"
git -C "$tap_work" status --short >"$status_file"
if [ -s "$status_file" ]; then
  fail "tap checkout is not clean"
fi

if [ "$mode" = "publish" ]; then
  branch="auto-bump/v${version}-$(date -u +%Y%m%dT%H%M%SZ)-$$"
  git -C "$tap_work" checkout --quiet -b "$branch" 2>"$tmp/branch.err" || fail "formula branch create refused"
fi

export VERSION="$version"
export SHA_AARCH64_DARWIN="$sha_aarch64_darwin"
for formula in lastdb folddb; do
  ruby "$BUMP_SCRIPT" "$tap_work/Formula/${formula}.rb"
done

git -C "$tap_work" diff -- Formula/lastdb.rb Formula/folddb.rb > "$tmp/bump.diff"
unexpected="$(grep -E '^[+-]' "$tmp/bump.diff" \
  | grep -vE '^[+-]{3}|^[+-]\s*(version\s|url\s|sha256\s)' \
  || true)"
if [ -n "$unexpected" ]; then
  echo "$unexpected" >&2
  fail "surgical formula bump touched lines outside version/url/sha256"
fi

release_url=""
tap_pr_url=""
receipt_tool="$ROOT/scripts/release/homebrew-promotion-receipt.py"
receipt_stage() {
  [ -n "$proof_report" ] || return 0
  python3 "$receipt_tool" --path "$proof_report" --stage "$@" || fail "durable promotion receipt refused"
}
if [ -n "$proof_report" ]; then
  receipt_stage prepared --init --mode "$mode" --tag "$version_tag" --source-oid "$source_git_oid" \
    --tap-base "$tap_base" --tap-root "$tap_dir" --artifacts "$artifact_report" \
    --bump-diff "$tmp/bump.diff" --sums "$release_dir/SHA256SUMS.txt"
fi
if [ "$mode" = "publish" ]; then
  require_formula_change "$tap_work"
  cd "$release_dir"
  if gh release view "$version_tag" --repo "$release_repo" >/dev/null 2>&1; then
    fail "$release_repo release $version_tag already exists; refusing to clobber"
  fi
  notes_file="$tmp/release-notes.md"
  cat >"$notes_file" <<EOF
## LastDB Mini

\`brew install edgevector/lastdb/lastdb\`, or download \`lastdb-aarch64-apple-darwin.tar.gz\` (Apple Silicon; \`lastdb\` CLI + \`lastdbd\` daemon).

Public release for \`$version_tag\`. The artifact manifest names the exact \`EdgeVector/lastdb\` source OID \`$source_git_oid\`.
EOF
  receipt_stage release_requested --branch "$branch"
  if ! release_result="$(gh release create "$version_tag" --repo "$release_repo" \
    --title "$version_tag" \
    --notes-file "$notes_file" \
    lastdb-aarch64-apple-darwin.tar.gz \
    lastdb-aarch64-apple-darwin.manifest.json \
    SHA256SUMS.txt 2>"$tmp/release-create.err")"; then
    fail "release create outcome is unresolved; reconcile the recorded exact tag"
  fi
  release_url="https://github.com/${release_repo}/releases/tag/${version_tag}"
  [ "$release_result" = "$release_url" ] || fail "release create returned an unexpected public identity"
  receipt_stage release_created --release-url "$release_url"

  cd "$tap_work"
  if ! git diff --quiet Formula/lastdb.rb Formula/folddb.rb; then
    require_formula_change "$tap_work"
    git add -A
    receipt_stage commit_requested
    git -c user.name="LastDB release" -c user.email="release@localhost" commit -qm "bump: lastdb/folddb -> v${version} (auto)" 2>"$tmp/commit.err" || fail "formula commit refused"
    formula_head="$(git rev-parse HEAD)"
    receipt_stage commit_created --head "$formula_head"
    pr_body="$tmp/tap-pr-body.md"
    cat >"$pr_body" <<EOF
Proposed by EdgeVector/lastdb stable release promotion for tag \`$version_tag\`.

Bumps both the canonical \`lastdb\` formula and the back-compat \`folddb\` alias using the exact source-bound artifact manifest and SHA256SUMS check.

Validation: exact paired Mini binary versions, artifact SHA256, selected source OID, clean accepted-main DEV base, and formula-only version/url/sha256 diff. Normal required tap CI controls merge.

Formula SoT: GitHub \`${release_repo}\`.
Bottle CDN: GitHub Releases on \`${release_repo}\`.

Verify with:
\`\`\`
brew update
brew info edgevector/lastdb/lastdb
\`\`\`
EOF

    [ -z "$(git status --porcelain)" ] || fail "tap commit did not preserve all changes"
    last-stack-pr-venue "$release_repo" "$tap_work" >/dev/null 2>&1 || fail "tap venue check refused"
    receipt_stage push_requested
    git push --quiet -u origin "$branch" 2>"$tmp/push.err" || fail "formula push outcome is unresolved; reconcile the recorded exact branch"
    receipt_stage branch_pushed
    receipt_stage pr_requested
    if ! tap_pr_url="$(gh pr create \
      --repo "$release_repo" \
      --title "bump: lastdb/folddb -> v${version}" \
      --base main --head "$branch" --body-file "$pr_body" 2>"$tmp/pr-create.err")"; then
      fail "formula PR create outcome is unresolved; reconcile the recorded exact branch"
    fi
    receipt_stage pr_created --pr-url "$tap_pr_url"
    receipt_stage auto_arm_requested
    gh pr merge --auto --squash "$tap_pr_url" --match-head-commit "$formula_head" --repo "$release_repo" >"$tmp/auto-arm.out" 2>"$tmp/auto-arm.err" || fail "formula PR auto-merge refused; the exact PR receipt remains"
    receipt_stage auto_arm_returned
    gh pr view "$tap_pr_url" --repo "$release_repo" --json url,headRefOid,autoMergeRequest \
      >"$tmp/auto-read.out" 2>"$tmp/auto-read.err" || fail "formula PR auto-merge readback refused; the exact PR receipt remains"
    receipt_stage auto_arm_verified --auto-arm-result "$tmp/auto-read.out"
  fi
fi

if [ "$mode" = dry-run ]; then
  receipt_stage dry_run_complete
fi

echo "FORGE_HOMEBREW_PROMOTION=ok mode=$mode version_tag=$version_tag release_repo=$release_repo formula_venue=github proof=${proof_report:-none}"
