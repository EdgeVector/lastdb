#!/usr/bin/env bash
#
# Promote Forge-built LastDB Mini artifacts to the public Homebrew release
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
  forge-promote-homebrew-stable.sh --version-tag vX.Y.Z --artifact-dir DIR --publish [options]

Options:
  --version-tag TAG       Stable tag to promote. Must be vX.Y.Z-style and not a prerelease.
  --artifact-dir DIR      Directory containing lastdb-*.tar.gz and matching manifests.
  --tap-dir DIR           Existing homebrew-lastdb checkout. Required for dry-run;
                          optional for publish (must use a GitHub origin).
  --source-git-oid OID    Expected source OID recorded in manifests.
  --release-repo REPO     Public bottle release repository (default: EdgeVector/homebrew-lastdb).
  --formula-venue github  Compatibility with the installed release caller; GitHub is the only venue.
  --proof-report FILE     Write machine-readable promotion proof JSON.
  --dry-run               Validate release + formula bump path without network writes.
  --publish               Create public bottle release + formula PR on the tap
                          (GitHub by default). Requires GH_TOKEN for the bottle CDN.
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

require_github_tap_origin() {
  local dir="$1"
  local url
  url="$(git -C "$dir" config --get remote.origin.url 2>/dev/null || true)"
  case "$url" in
    "https://github.com/${release_repo}"|"https://github.com/${release_repo}.git"|"git@github.com:${release_repo}"|"git@github.com:${release_repo}.git") ;;
    *) fail "tap origin must be GitHub ${release_repo} for publish" ;;
  esac
}

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
    --source-git-oid) [ "$#" -ge 2 ] || fail "missing value for --source-git-oid"; source_git_oid="$2"; shift 2 ;;
    --release-repo) [ "$#" -ge 2 ] || fail "missing value for --release-repo"; release_repo="$2"; shift 2 ;;
    --formula-venue)
      [ "$#" -ge 2 ] || fail "missing value for --formula-venue"
      [ "$2" = "github" ] || fail "formula venue must be github"
      shift 2 ;;
    --proof-report) [ "$#" -ge 2 ] || fail "missing value for --proof-report"; proof_report="$2"; shift 2 ;;
    --dry-run) mode="dry-run"; shift ;;
    --publish) mode="publish"; shift ;;
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
  [ -n "${GH_TOKEN:-}" ] || fail "GH_TOKEN is required for --publish (public bottle CDN)"
else
  [ -n "$tap_dir" ] || fail "--tap-dir is required for --dry-run"
fi

tmp="$(mktemp -d "${TMPDIR:-/tmp}/forge-promote-homebrew.XXXXXX")"
trap 'rm -rf "$tmp"' EXIT
release_dir="$tmp/release"
tap_work="$tmp/tap"
mkdir -p "$release_dir"

find "$artifact_dir" -type f -name 'lastdb-*.tar.gz' -exec cp {} "$release_dir/" \;
find "$artifact_dir" -type f -name 'lastdb-*.manifest.json' -exec cp {} "$release_dir/" \;

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

clone_formula_sot() {
  git clone "https://github.com/${release_repo}.git" "$tap_work"
}

if [ -n "$tap_dir" ]; then
  [ -d "$tap_dir/Formula" ] || fail "tap Formula directory not found: $tap_dir/Formula"
  is_git_checkout "$tap_dir" || fail "tap checkout must be a git clone or worktree: $tap_dir"
  mkdir -p "$tap_work"
  if [ -d "$tap_dir/.git" ]; then
    # Full clone: copy tree + history so dry-run and publish keep real remotes/oids.
    cp -R "$tap_dir"/. "$tap_work"/
  elif [ "$mode" = "dry-run" ]; then
    # Linked worktree (`.git` file): snapshot content into a local git repo for
    # the bump/diff guard only. Never push from this re-init path.
    if command -v rsync >/dev/null 2>&1; then
      rsync -a --exclude '.git' "$tap_dir"/ "$tap_work"/
    else
      # Portable fallback when rsync is unavailable.
      tar -C "$tap_dir" --exclude '.git' -cf - . | tar -C "$tap_work" -xf -
    fi
    git -C "$tap_work" init -q
    git -C "$tap_work" config user.name "forge-promote"
    git -C "$tap_work" config user.email "forge-promote@localhost"
    git -C "$tap_work" add -A
    git -C "$tap_work" commit -qm "tap snapshot for promote dry-run"
  else
    # Publish + linked worktree: do not re-init (would invent history). Clone
    # formula SoT instead so the bump branch is based on real main.
    echo "forge-promote-homebrew-stable: --tap-dir is a linked worktree; cloning formula SoT for publish" >&2
    rm -rf "$tap_work"
    clone_formula_sot
  fi
else
  clone_formula_sot
fi

for formula in lastdb folddb; do
  [ -f "$tap_work/Formula/${formula}.rb" ] || fail "missing tap formula: Formula/${formula}.rb"
done

is_git_checkout "$tap_work" || fail "tap checkout must be a git worktree so the formula diff guard can run"
if [ "$mode" = "publish" ]; then
  require_github_tap_origin "$tap_work"
fi

status_file="$tmp/tap-status.txt"
git -C "$tap_work" status --short >"$status_file"
if [ -s "$status_file" ]; then
  cat "$status_file" >&2
  fail "tap checkout is not clean"
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
if [ "$mode" = "publish" ]; then
  cd "$release_dir"
  if gh release view "$version_tag" --repo "$release_repo" >/dev/null 2>&1; then
    fail "$release_repo release $version_tag already exists; refusing to clobber"
  fi
  notes_file="$tmp/release-notes.md"
  cat >"$notes_file" <<EOF
## LastDB Mini

\`brew install edgevector/lastdb/lastdb\`, or download \`lastdb-aarch64-apple-darwin.tar.gz\` (Apple Silicon; \`lastdb\` CLI + \`lastdbd\` daemon).

Public release for \`$version_tag\`. Source provenance is the matching \`EdgeVector/fold\` git tag.
EOF
  gh release create "$version_tag" --repo "$release_repo" \
    --title "$version_tag" \
    --notes-file "$notes_file" \
    lastdb-aarch64-apple-darwin.tar.gz \
    lastdb-aarch64-apple-darwin.manifest.json \
    SHA256SUMS.txt
  release_url="https://github.com/${release_repo}/releases/tag/${version_tag}"

  cd "$tap_work"
  git config user.name "Fold release"
  git config user.email "fold-release@localhost"
  branch="auto-bump/v${version}"
  git checkout -B "$branch"
  if ! git diff --quiet Formula/lastdb.rb Formula/folddb.rb; then
    git add Formula/lastdb.rb Formula/folddb.rb
    git commit -m "bump: lastdb/folddb -> v${version} (auto)"
    pr_body="$tmp/tap-pr-body.md"
    cat >"$pr_body" <<EOF
Auto-generated by EdgeVector/fold local/Forge release promotion for tag \`$version_tag\`.

Bumps both the canonical \`lastdb\` formula and the back-compat \`folddb\` alias using the Forge-built artifact manifest and SHA256SUMS proof.

Formula SoT: GitHub \`${release_repo}\`.
Bottle CDN: GitHub Releases on \`${release_repo}\`.

Verify with:
\`\`\`
brew update
brew info edgevector/lastdb/lastdb
\`\`\`
EOF

    git push -u origin "$branch"
    tap_pr_url="$(gh pr create \
      --repo "$release_repo" \
      --title "bump: lastdb/folddb -> v${version}" \
      --body-file "$pr_body")"
    gh pr merge --auto --squash "$tap_pr_url" --repo "$release_repo"
    gh pr view "$tap_pr_url" --repo "$release_repo" --json autoMergeRequest \
      --jq 'if .autoMergeRequest then empty else error("auto-merge enable was silently refused") end'
  fi
fi

if [ -n "$proof_report" ]; then
  mkdir -p "$(dirname "$proof_report")"
  python3 - "$proof_report" "$mode" "$version_tag" "$release_repo" "$release_dir" \
    "$artifact_report" "$tmp/bump.diff" "$release_url" "$tap_pr_url" \
    <<'PY'
import json
import os
import sys

(
    proof_path,
    mode,
    version_tag,
    release_repo,
    release_dir,
    artifact_report,
    bump_diff,
    release_url,
    tap_pr_url,
) = sys.argv[1:]

with open(artifact_report, encoding="utf-8") as fh:
    artifacts = json.load(fh)
with open(bump_diff, encoding="utf-8") as fh:
    diff_lines = fh.read().splitlines()
with open(os.path.join(release_dir, "SHA256SUMS.txt"), encoding="utf-8") as fh:
    sha256sums = fh.read().splitlines()

payload = {
    "schema": "lastdb.mini.forge_homebrew_promotion.v1",
    "publish": mode == "publish",
    "mode": mode,
    "version_tag": version_tag,
    "release_repo": release_repo,
    "formula_venue": "github",
    "release_assets": {
        "files": sorted(os.listdir(release_dir)),
        "sha256sums": sha256sums,
    },
    "validated_artifacts": artifacts,
    "homebrew_bump": {
        "formulas": ["Formula/lastdb.rb", "Formula/folddb.rb"],
        "changed_line_classes": ["version", "url", "sha256"],
        "diff_line_count": len(diff_lines),
    },
    "live_outputs": {
        "release_url": release_url,
        "tap_pr_url": tap_pr_url,
        # Keep the v1 proof contract while the retired venue stays unavailable.
        "forge_pr_number": "",
    },
}

with open(proof_path, "w", encoding="utf-8") as fh:
    json.dump(payload, fh, indent=2, sort_keys=True)
    fh.write("\n")
PY
fi

echo "FORGE_HOMEBREW_PROMOTION=ok mode=$mode version_tag=$version_tag release_repo=$release_repo formula_venue=github proof=${proof_report:-none}"
