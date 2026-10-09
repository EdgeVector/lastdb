#!/usr/bin/env bash
# lint-workspace-edition-inheritance.sh
#
# Workspace member crates must inherit Rust edition from [workspace.package]
# instead of pinning their own literal edition. This keeps the workspace's
# edition policy in one place and prevents quiet per-crate drift.
#
# Failure exit: 1.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

if ! command -v cargo >/dev/null 2>&1; then
    echo "lint-workspace-edition-inheritance: cargo not found in PATH" >&2
    exit 2
fi

if ! command -v python3 >/dev/null 2>&1; then
    echo "lint-workspace-edition-inheritance: python3 not found in PATH" >&2
    exit 2
fi

workspace_manifests=""
if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    cargo_manifests="$(git ls-files 'Cargo.toml')"
else
    cargo_manifests="$(find . -name Cargo.toml -not -path './target/*' | sed 's#^\./##')"
fi

while IFS= read -r manifest; do
    [ -n "$manifest" ] || continue
    if grep -q '^[[:space:]]*\[workspace\]' "$manifest"; then
        workspace_manifests="${workspace_manifests}${manifest}"$'\n'
    fi
done <<< "$cargo_manifests"

if [ -z "$workspace_manifests" ]; then
    echo "lint-workspace-edition-inheritance: no workspace manifests found" >&2
    exit 2
fi

errors=0
checked=0

while IFS= read -r workspace_manifest; do
    [ -n "$workspace_manifest" ] || continue

    metadata_json="$(cargo metadata --no-deps --format-version 1 --manifest-path "$workspace_manifest")"
    member_manifests="$(
        python3 -c '
import json, sys

metadata = json.load(sys.stdin)
members = set(metadata["workspace_members"])
for package in metadata["packages"]:
    if package["id"] in members:
        print(package["manifest_path"])
' <<< "$metadata_json"
    )"

    while IFS= read -r manifest_path; do
        [ -n "$manifest_path" ] || continue
        rel_path="${manifest_path#"$REPO_ROOT"/}"
        checked=$((checked + 1))

        if grep -Eq '^[[:space:]]*edition[[:space:]]*=' "$rel_path"; then
            echo "lint-workspace-edition-inheritance: FAIL" >&2
            echo "  $rel_path pins a literal Rust edition." >&2
            grep -nE '^[[:space:]]*edition[[:space:]]*=' "$rel_path" | sed 's/^/    /' >&2
            errors=$((errors + 1))
        fi

        if ! grep -Eq '^[[:space:]]*edition\.workspace[[:space:]]*=[[:space:]]*true[[:space:]]*$' "$rel_path"; then
            echo "lint-workspace-edition-inheritance: FAIL" >&2
            echo "  $rel_path must declare edition.workspace = true." >&2
            errors=$((errors + 1))
        fi
    done <<< "$member_manifests"
done <<< "$workspace_manifests"

if [ "$errors" -gt 0 ]; then
    cat >&2 <<'EOF'

Rust edition is owned by [workspace.package]. Replace member-crate
literal editions such as:

  edition = "2021"

with:

  edition.workspace = true

EOF
    exit 1
fi

echo "lint-workspace-edition-inheritance: ok - $checked workspace member manifest(s) inherit edition"
