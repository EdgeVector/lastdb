#!/usr/bin/env bash
# lint-no-build-artifacts.sh
#
# Blocks Cargo/build junk and oversized blobs from entering git history.
#
# Why: in May 2026, kanban checkpoint commits and salvage branches force-added
# nested `exemem_service/lambdas/*/target/` trees (~110 GB of .rlib blobs).
# Those refs kept the local .git at ~17 GB even after main no longer tracked
# any of it. .gitignore alone is not enough — `git add -f` and some agent
# checkpoint paths bypass it.
#
# Modes:
#   (default / --staged)  Inspect the index (pre-commit).
#   --tree [REV]          Inspect the tree at REV (default HEAD; CI).
#   --range A..B          Inspect blobs introduced by commits in the range
#                         (pre-push). Empty/missing A means "all of B".
#
# Failure exit: 1.
#
# Size ceiling: 2 MiB. Largest intentional tracked file on main as of
# 2026-07 is schemaorg JSON-LD (~1.6 MiB). Raise MAX_BYTES only with a
# documented allowlist entry below.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

MAX_BYTES=$((2 * 1024 * 1024))

# Paths that may exceed MAX_BYTES (prefix or exact match). Keep tight.
# Currently empty — schemaorg JSON-LD is under the ceiling. Add a path here
# only with a comment citing why the blob must live in git.
allowlisted_large_path() {
  case "$1" in
    # example: schema_service/crates/core/data/schema_org/*) return 0 ;;
    *) return 1 ;;
  esac
}

# Path patterns that must never be committed (build/cache/deps).
forbidden_path() {
  local p="$1"
  case "$p" in
    target/*|*/target/*) return 0 ;;
    target-dogfood/*|*/target-dogfood/*) return 0 ;;
    node_modules/*|*/node_modules/*) return 0 ;;
    *.rlib|*.rmeta|*.rlib.bc|*.o) return 0 ;;
    *.pyc|__pycache__/*|*/__pycache__/*) return 0 ;;
    .fastembed_cache/*|*/.fastembed_cache/*) return 0 ;;
    .semantic-search-fastembed-cache/*|*/.semantic-search-fastembed-cache/*) return 0 ;;
    .folddb-dev/*|*/.folddb-dev/*) return 0 ;;
  esac
  # CACHEDIR.TAG only matters as a cargo-target marker; still never commit it.
  case "$p" in
    */CACHEDIR.TAG|CACHEDIR.TAG) return 0 ;;
  esac
  return 1
}

MODE="staged"
RANGE=""
TREE_REV="HEAD"

while [ $# -gt 0 ]; do
  case "$1" in
    --staged) MODE="staged"; shift ;;
    --tree)
      MODE="tree"
      if [ $# -ge 2 ] && [[ "$2" != -* ]]; then TREE_REV="$2"; shift 2; else shift; fi
      ;;
    --range)
      MODE="range"
      RANGE="${2:-}"
      if [ -z "$RANGE" ]; then
        echo "lint-no-build-artifacts: --range requires A..B" >&2
        exit 2
      fi
      shift 2
      ;;
    -h|--help)
      sed -n '2,25p' "$0"
      exit 0
      ;;
    *)
      echo "lint-no-build-artifacts: unknown arg: $1" >&2
      exit 2
      ;;
  esac
done

offenders=()

record() {
  offenders+=("$1")
}

check_path_and_size() {
  local path="$1"
  local size="$2" # bytes; empty means "unknown / skip size"
  if forbidden_path "$path"; then
    record "FORBIDDEN_PATH  $path"
    return
  fi
  if [ -n "$size" ] && [ "$size" -gt "$MAX_BYTES" ] 2>/dev/null; then
    if allowlisted_large_path "$path"; then
      return
    fi
    record "OVERSIZE(${size}B > ${MAX_BYTES}B)  $path"
  fi
}

case "$MODE" in
  staged)
    # name-status gives renames; -z is safer but we keep line-based for portability.
    while IFS= read -r line; do
      [ -z "$line" ] && continue
      # format: MODE\tpath  or  MODE\told\tnew for renames
      mode="${line%%$'\t'*}"
      rest="${line#*$'\t'}"
      case "$mode" in
        R*|C*)
          path="${rest##*$'\t'}"
          ;;
        D*)
          continue
          ;;
        *)
          path="$rest"
          ;;
      esac
      [ -z "$path" ] && continue
      # Size from the index blob when possible.
      size=""
      if oid=$(git rev-parse --verify ":$path" 2>/dev/null); then
        size=$(git cat-file -s "$oid" 2>/dev/null || true)
      elif [ -f "$path" ]; then
        size=$(wc -c <"$path" | tr -d ' ')
      fi
      check_path_and_size "$path" "$size"
    done < <(git diff --cached --name-status --diff-filter=ACMR || true)
    ;;
  tree)
    while IFS= read -r line; do
      [ -z "$line" ] && continue
      # mode type oid size\tpath  (ls-tree -l -r)
      size=$(printf '%s' "$line" | awk '{print $4}')
      path=$(printf '%s' "$line" | awk '{print substr($0, index($0,$5))}')
      # skip non-blobs (size is '-' for trees)
      [ "$size" = "-" ] && continue
      check_path_and_size "$path" "$size"
    done < <(git ls-tree -r -l "$TREE_REV")
    ;;
  range)
    # Blobs introduced by the range. For brand-new branches, A may be all-zero.
    local_range="$RANGE"
    if [[ "$local_range" == *..* ]]; then
      a="${local_range%%..*}"
      b="${local_range##*..}"
      if [ -z "$a" ] || [[ "$a" =~ ^0+$ ]]; then
        rev_list_args=("$b")
      else
        rev_list_args=("${a}..${b}")
      fi
    else
      rev_list_args=("$local_range")
    fi
    while IFS= read -r line; do
      [ -z "$line" ] && continue
      # objecttype objectname objectsize rest(path)
      otype=$(printf '%s' "$line" | awk '{print $1}')
      [ "$otype" = "blob" ] || continue
      osize=$(printf '%s' "$line" | awk '{print $3}')
      path=$(printf '%s' "$line" | awk '{print substr($0, index($0,$4))}')
      [ -z "$path" ] && continue
      check_path_and_size "$path" "$osize"
    done < <(
      git rev-list --objects "${rev_list_args[@]}" \
        | git cat-file --batch-check='%(objecttype) %(objectname) %(objectsize) %(rest)'
    )
    ;;
esac

if [ "${#offenders[@]}" -gt 0 ]; then
  echo "lint-no-build-artifacts: blocked ${#offenders[@]} path(s)." >&2
  echo "lint-no-build-artifacts: cargo/build caches and files >${MAX_BYTES} bytes must not enter git." >&2
  echo "lint-no-build-artifacts: (history bloat incident: nested **/target/ ~110GB via kanban checkpoints)" >&2
  n=0
  for o in "${offenders[@]}"; do
    n=$((n + 1))
    if [ "$n" -le 40 ]; then
      echo "  $o" >&2
    fi
  done
  if [ "${#offenders[@]}" -gt 40 ]; then
    echo "  ... and $((${#offenders[@]} - 40)) more" >&2
  fi
  echo "lint-no-build-artifacts: unstage them (\`git restore --staged -- <path>\`)," >&2
  echo "  ensure .gitignore covers them, and never \`git add -f\` a target/ tree." >&2
  exit 1
fi

exit 0
