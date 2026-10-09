#!/usr/bin/env bash
#
# One-shot local-canonical stable cut for LastDB Mini Homebrew:
#   verify fold SHA → (optional tag) → cargo build → package → promote
#
# Does NOT depend on fold GitHub Actions. Bottle CDN still uses GitHub Releases;
# formula SoT is the GitHub homebrew-lastdb repository.
#
# Primary Mini on this host remains a separate lastdb-safe-upgrade step.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PACKAGE_SCRIPT="$ROOT/scripts/release/package-lastdb-bundle.sh"
PROMOTE_SCRIPT="$ROOT/scripts/release/forge-promote-homebrew-stable.sh"

usage() {
  cat <<'EOF'
Usage:
  cut-local-stable.sh --version-tag vX.Y.Z [options]

Required:
  --version-tag TAG     Stable tag vX.Y.Z (no prerelease suffix).

Options:
  --git-oid OID         Pin fold SHA (default: HEAD of this checkout).
  --out-dir DIR         Artifact output dir (default: /tmp/lastdb-release-<tag>).
  --tap-dir DIR         homebrew-lastdb checkout for dry-run promote (required with --dry-run).
  --proof-report FILE   Promotion proof JSON path.
  --skip-build          Reuse existing target/release/lastdb{,d} (still version-checked).
  --skip-tag            Do not create/push the fold tag (assume it already exists).
  --push-tag            After local annotated tag, push to origin (GitHub).
  --dry-run             Stop after promote --dry-run (no public release, no formula PR).
  --publish             Run promote --publish (public bottle + formula PR on the tap).
  --target TRIPLE       Package target (default: aarch64-apple-darwin).
  --help | -h

Exactly one of --dry-run or --publish is required.

Examples:
  # Validate package + formula bump without publishing:
  bash scripts/release/cut-local-stable.sh \
    --version-tag v0.23.4 --dry-run --tap-dir ~/code/edgevector/homebrew-lastdb

  # Full public cut from a clean fold worktree already at the release SHA:
  GH_TOKEN="$(gh auth token)" bash scripts/release/cut-local-stable.sh \
    --version-tag v0.23.4 --publish --push-tag
EOF
}

fail() {
  echo "cut-local-stable: $*" >&2
  exit 64
}

require_tool() {
  command -v "$1" >/dev/null 2>&1 || fail "missing required tool: $1"
}

version_tag=""
git_oid=""
out_dir=""
tap_dir=""
proof_report=""
skip_build=0
skip_tag=0
push_tag=0
mode=""
target="aarch64-apple-darwin"

while [ "$#" -gt 0 ]; do
  case "$1" in
    --version-tag) [ "$#" -ge 2 ] || fail "missing value for --version-tag"; version_tag="$2"; shift 2 ;;
    --git-oid) [ "$#" -ge 2 ] || fail "missing value for --git-oid"; git_oid="$2"; shift 2 ;;
    --out-dir) [ "$#" -ge 2 ] || fail "missing value for --out-dir"; out_dir="$2"; shift 2 ;;
    --tap-dir) [ "$#" -ge 2 ] || fail "missing value for --tap-dir"; tap_dir="$2"; shift 2 ;;
    --proof-report) [ "$#" -ge 2 ] || fail "missing value for --proof-report"; proof_report="$2"; shift 2 ;;
    --skip-build) skip_build=1; shift ;;
    --skip-tag) skip_tag=1; shift ;;
    --push-tag) push_tag=1; shift ;;
    --dry-run|--publish)
      [ -z "$mode" ] || fail "choose exactly one of --dry-run or --publish"
      mode="${1#--}"
      shift
      ;;
    --target) [ "$#" -ge 2 ] || fail "missing value for --target"; target="$2"; shift 2 ;;
    --help|-h) usage; exit 0 ;;
    *) fail "unknown argument: $1" ;;
  esac
done

[ -n "$version_tag" ] || fail "--version-tag is required"
[ -n "$mode" ] || fail "choose exactly one of --dry-run or --publish"
case "$version_tag" in
  *-*) fail "stable cut refuses prerelease tag: $version_tag" ;;
esac
[[ "$version_tag" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] ||
  fail "--version-tag must look like vX.Y.Z (got $version_tag)"
version="${version_tag#v}"
out_dir="${out_dir:-/tmp/lastdb-release-${version_tag}}"
proof_report="${proof_report:-/tmp/forge-homebrew-promotion-${version_tag}.json}"

if [ "$mode" = "dry-run" ]; then
  [ -n "$tap_dir" ] || fail "--tap-dir is required for --dry-run"
fi

require_tool git
require_tool cargo
require_tool bash
[ -x "$PACKAGE_SCRIPT" ] || fail "missing package script: $PACKAGE_SCRIPT"
[ -x "$PROMOTE_SCRIPT" ] || fail "missing promote script: $PROMOTE_SCRIPT"

cd "$ROOT"

if [ -n "$(git status --porcelain)" ]; then
  fail "fold checkout is dirty; refuse release cut from a dirty tree"
fi

head_oid="$(git rev-parse HEAD)"
if [ -n "$git_oid" ]; then
  resolved="$(git rev-parse "$git_oid")"
  [ "$resolved" = "$head_oid" ] || fail "--git-oid $git_oid is not HEAD ($head_oid); checkout that SHA first"
else
  git_oid="$head_oid"
fi

echo "cut-local-stable: fold ROOT=$ROOT"
echo "cut-local-stable: version_tag=$version_tag git_oid=$git_oid mode=$mode"

if [ "$skip_tag" -eq 0 ]; then
  if git rev-parse -q --verify "refs/tags/${version_tag}" >/dev/null; then
    existing="$(git rev-parse "refs/tags/${version_tag}^{}")"
    [ "$existing" = "$git_oid" ] || fail "tag $version_tag exists at $existing, not $git_oid"
    echo "cut-local-stable: tag $version_tag already points at $git_oid"
  else
    git tag -a "$version_tag" -m "$version_tag" "$git_oid"
    echo "cut-local-stable: created annotated tag $version_tag"
  fi
  if [ "$push_tag" -eq 1 ]; then
    git push origin "refs/tags/${version_tag}"
    echo "cut-local-stable: pushed $version_tag to origin"
  fi
else
  echo "cut-local-stable: --skip-tag; not creating/pushing tag"
fi

bin_dir="$ROOT/target/release"
if [ "$skip_build" -eq 0 ]; then
  echo "cut-local-stable: cargo build --release -p lastdb_node --bin lastdb --bin lastdbd"
  cargo build --release -p lastdb_node --bin lastdb --bin lastdbd
else
  echo "cut-local-stable: --skip-build; using existing $bin_dir"
fi

[ -x "$bin_dir/lastdb" ] || fail "missing $bin_dir/lastdb"
[ -x "$bin_dir/lastdbd" ] || fail "missing $bin_dir/lastdbd"

lastdb_ver="$("$bin_dir/lastdb" --version 2>/dev/null | head -1 || true)"
lastdbd_ver="$("$bin_dir/lastdbd" --version 2>/dev/null | head -1 || true)"
printf '%s\n' "$lastdb_ver" | grep -q "$version" || fail "lastdb --version '$lastdb_ver' does not contain $version"
printf '%s\n' "$lastdbd_ver" | grep -q "$version" || fail "lastdbd --version '$lastdbd_ver' does not contain $version"
case "$lastdb_ver$lastdbd_ver" in
  *-dirty*) fail "binaries report -dirty; rebuild from a clean tree" ;;
esac

mkdir -p "$out_dir"
echo "cut-local-stable: packaging into $out_dir"
bash "$PACKAGE_SCRIPT" \
  --target "$target" \
  --bin-dir "$bin_dir" \
  --out-dir "$out_dir" \
  --git-oid "$git_oid" \
  --expect-version "$version"

promote_args=(
  --version-tag "$version_tag"
  --artifact-dir "$out_dir"
  --source-git-oid "$git_oid"
  --proof-report "$proof_report"
)
if [ "$mode" = "dry-run" ]; then
  promote_args+=(--dry-run --tap-dir "$tap_dir")
else
  promote_args+=(--publish)
  [ -n "${GH_TOKEN:-}" ] || fail "GH_TOKEN is required for --publish"
fi

echo "cut-local-stable: promote ${mode}"
bash "$PROMOTE_SCRIPT" "${promote_args[@]}"

echo "CUT_LOCAL_STABLE=ok mode=$mode version_tag=$version_tag git_oid=$git_oid out_dir=$out_dir proof=$proof_report"
echo "cut-local-stable: primary Mini on this host is NOT upgraded here — use lastdb-safe-upgrade if desired"
