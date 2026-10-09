#!/usr/bin/env bash
# lint-workspace-fastembed-unification.sh
#
# FAIL CLOSED when cargo feature-unification can paint fastembed/ONNX across a
# full-workspace resolve. That accident is what made post-merge "cold start"
# clippy look like a 60-minute job — it is NOT a normal cold start.
#
# Background (2026-07):
#   cargo clippy --workspace --all-targets
# with packages that depend on schema_service_server_shared features=["fastembed"]
# unifies schema-service FastEmbed/ONNX onto the ENTIRE graph (every workspace test
# target included). Agents repeatedly rationalized multi-tens-of-minutes runs
# as "cold cache." This lint exists so that regression turns CI red with a
# message that says "this is a feature-unification bug, not a cold start."
#
# What it checks:
#   1. Allowlist of packages that may FORCE fastembed into a resolve (hard deps
#      with features=["fastembed"]). New force-enablers must be added here AND
#      excluded from bulk workspace clippy in
#      .github/workflows/ci-required.yml.
#   2. cargo tree: bulk workspace (allowlist excluded) must NOT link fastembed.
#   3. cargo tree: each allowlisted package MUST link fastembed (coverage still
#      exists for the off-lane path).
#   4. cargo tree: bare --workspace (no excludes) MUST link fastembed — proving
#      why the heavy job must exclude the force-packages.
#   5. GitHub workflow hygiene: full_workspace_check timeout ≤45m, bulk clippy
#      excludes the allowlist, off-lane step names each allowlist package, and
#      the heavy job keeps the fastembed force-packages in a separate lane.
#
# Runs on the required GitHub Mini gate (every PR) so this cannot wait for a
# post-merge 60m blowup to be noticed.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

# Packages that force features=["fastembed"] (or equivalent) as a hard dep.
# Keep in sync with .github/workflows/ci-required.yml full_workspace_check
# excludes.
ONNX_FORCE_PACKAGES=(
  generate_validated_schema_org_seeds
  schema_service_worker
)

GITHUB_HEAVY_CI=".github/workflows/ci-required.yml"
# The required GitHub job has a 45-minute deadline, including a cold build.
MAX_HEAVY_TIMEOUT_MINUTES=45

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

note() {
  echo "OK: $*"
}

tree_output() {
  local output
  local exit_code

  set +e
  output="$(cargo tree "$@" 2>&1)"
  exit_code=$?
  set -e

  # cargo tree exits non-zero when -i fastembed matches nothing; that is the
  # "absent" success path for assert_absent.
  # Read via here-string: `printf | grep -q` under `set -o pipefail` false-fails
  # when grep exits on the first match and printf gets SIGPIPE.
  if [ "$exit_code" -ne 0 ] \
    && ! grep -q 'package ID specification `fastembed` did not match any packages' <<<"$output" \
    && ! grep -q 'did not match any packages' <<<"$output"; then
    printf '%s\n' "$output" >&2
    fail "cargo tree failed: cargo tree $*"
  fi

  printf '%s\n' "$output"
}

has_fastembed() {
  grep -q '^fastembed v' <<<"$1"
}

assert_absent() {
  local label="$1"
  shift
  local tree
  tree="$(tree_output "$@")"
  if has_fastembed "$tree"; then
    printf '%s\n' "$tree" >&2
    cat >&2 <<EOF

FAIL: fastembed/ONNX is linked into: ${label}

This is NOT a normal cold start. Cargo feature-unification pulled
schema-service FastEmbed (fastembed → ONNX Runtime) into a bulk workspace
resolve. Cold full-workspace clippy then rebuilds the whole graph with ONNX
and can look like a 30–60 minute "cold cache" job.

Fix:
  - Do not run a single unscoped \`cargo clippy --workspace --all-targets\`.
  - Bulk: exclude packages that force features=["fastembed"]
    (currently: ${ONNX_FORCE_PACKAGES[*]}).
  - Off-lane: clippy those packages alone (\`cargo clippy -p … --bins\`).
  - If you intentionally added a new force-enabler, update ONNX_FORCE_PACKAGES
    in scripts/ci/lint-workspace-fastembed-unification.sh AND the excludes in
    .github/workflows/ci-required.yml full_workspace_check.

EOF
    fail "fastembed present in ${label}"
  fi
  note "fastembed absent from ${label}"
}

assert_present() {
  local label="$1"
  shift
  local tree
  tree="$(tree_output "$@")"
  if ! has_fastembed "$tree"; then
    printf '%s\n' "$tree" >&2
    fail "fastembed missing from ${label} (off-lane ONNX coverage would be a lie)"
  fi
  note "fastembed present in ${label}"
}

# --- 1. Cargo.toml: only the allowlist may force features=["fastembed"] ----

# Matches hard-dep enablement: features = [..., "fastembed", ...]
# Skips:
#   - feature table keys like `fastembed = ["dep:fastembed"]`
#   - `required-features = ["fastembed"]` on [[bin]] / [[test]] (opt-in only)
force_hits="$(
  rg -n --glob '**/Cargo.toml' --glob '!**/target/**' \
    'features\s*=\s*\[[^\]]*["'\'']fastembed["'\'']' \
    "$ROOT" \
    | grep -v 'required-features' \
    || true
)"

while IFS= read -r line; do
  [ -z "$line" ] && continue
  # line format: path:lineno:content
  path="${line%%:*}"
  pkg_name="$(
    awk '
      /^\[package\]/ { in_pkg=1; next }
      /^\[/ { in_pkg=0 }
      in_pkg && /^name[[:space:]]*=/ {
        line=$0
        sub(/^[^=]*=[[:space:]]*/, "", line)
        gsub(/"/, "", line)
        gsub(/[[:space:]]/, "", line)
        print line
        exit
      }
    ' "$path"
  )"
  if [ -z "$pkg_name" ]; then
    fail "could not parse package name from $path (force-fastembed line: $line)"
  fi
  allowed=0
  for a in "${ONNX_FORCE_PACKAGES[@]}"; do
    if [ "$pkg_name" = "$a" ]; then
      allowed=1
      break
    fi
  done
  if [ "$allowed" -ne 1 ]; then
    cat >&2 <<EOF
FAIL: package '${pkg_name}' forces features=["fastembed"] but is not on the
ONNX force-enable allowlist:

  ${line}

Allowlist (scripts/ci/lint-workspace-fastembed-unification.sh):
  ${ONNX_FORCE_PACKAGES[*]}

Adding a new force-enabler WITHOUT excluding it from bulk workspace clippy
will feature-unify ONNX onto the whole monorepo again. Update the allowlist
AND .github/workflows/ci-required.yml full_workspace_check in the same PR.
EOF
    fail "unallowlisted fastembed force-enable in ${pkg_name}"
  fi
done <<< "$force_hits"

note "only allowlisted packages force features=[\"fastembed\"] (${#ONNX_FORCE_PACKAGES[@]} package(s))"

# --- 2–4. cargo tree resolves ----------------------------------------------

exclude_args=()
for p in "${ONNX_FORCE_PACKAGES[@]}"; do
  exclude_args+=(--exclude "$p")
done

assert_absent \
  "bulk workspace resolve (ONNX force-packages excluded)" \
  --workspace "${exclude_args[@]}" -i fastembed

for p in "${ONNX_FORCE_PACKAGES[@]}"; do
  assert_present \
    "off-lane package ${p}" \
    -p "$p" -i fastembed
done

assert_present \
  "bare --workspace resolve (shows why force-packages need exclusion)" \
  --workspace -i fastembed

# --- 5. GitHub workflow must keep the split + job deadline -----------------

[ -f "$GITHUB_HEAVY_CI" ] || fail "missing $GITHUB_HEAVY_CI"

# Extract the full_workspace_check job block (until next top-level job key).
job_block="$(
  awk '
    /^  full_workspace_check:/ { grab=1 }
    grab {
      print
      # next job at indent-2 that is not a step/list continuation
      if (NR > 1 && /^  [a-zA-Z0-9_]+:/ && !/^  full_workspace_check:/) {
        exit
      }
    }
  ' "$GITHUB_HEAVY_CI"
)"

[ -n "$job_block" ] || fail "could not find full_workspace_check job in $GITHUB_HEAVY_CI"

timeout="$(awk '/timeout-minutes:/ { print $2; exit }' <<<"$job_block")"
if [ -z "$timeout" ]; then
  fail "full_workspace_check has no timeout-minutes"
fi
if [ "$timeout" -gt "$MAX_HEAVY_TIMEOUT_MINUTES" ]; then
  fail "full_workspace_check timeout-minutes=${timeout} > ${MAX_HEAVY_TIMEOUT_MINUTES} (required GitHub job deadline exceeded)"
fi
note "full_workspace_check timeout-minutes=${timeout} ≤ ${MAX_HEAVY_TIMEOUT_MINUTES}"

# Bulk step must exclude every allowlisted package.
# Here-string, not `printf | grep -q`: under `set -o pipefail` an early grep
# exit SIGPIPEs printf and the check reports a missing exclude that is present.
# Measured 2026-10-03 on EdgeVector/fold#1431 job 111317154072.
for p in "${ONNX_FORCE_PACKAGES[@]}"; do
  if ! grep -qF -- "--exclude ${p}" <<<"$job_block"; then
    fail "full_workspace_check bulk clippy missing --exclude ${p}"
  fi
done
note "full_workspace_check excludes all ONNX force-packages"

# Off-lane step must name each package with -p.
for p in "${ONNX_FORCE_PACKAGES[@]}"; do
  if ! grep -qE -- "-p[[:space:]]+${p}|-p=${p}" <<<"$job_block"; then
    fail "full_workspace_check off-lane clippy missing -p ${p}"
  fi
done
note "full_workspace_check off-lane step names all ONNX force-packages"

# The separate heavy Clippy coverage lint checks each command and its target flags.

echo
echo "workspace fastembed-unification lint passed"
echo "  (ONNX force-packages: ${ONNX_FORCE_PACKAGES[*]})"
echo "  Reminder: multi-tens-of-minutes 'cold' full-workspace clippy is a bug, not weather."
