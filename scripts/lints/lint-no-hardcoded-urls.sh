#!/usr/bin/env bash
# lint-no-hardcoded-urls.sh
#
# Enforces the cross-environment URL registry: all gateway hostnames must
# live in folddb_profile/environments.json (the single source of truth —
# moved there from the deleted fold_db_node crate in the Mini-only cutover,
# 2026-07-12). Any hardcoded occurrence elsewhere — Rust, shell, JSON,
# Markdown — is drift waiting to happen. folddb_profile/build.rs generates
# the per-(env, key) constants in OUT_DIR; Rust callers go through
# `folddb_profile::endpoints::*`.
#
# Failure exit: 1.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

# The current API Gateway IDs. Update by editing environments.json
# (which this lint reads back); never edit this list by hand.
#
# `region` is metadata, not a URL. `portal` is excluded too: it is
# registry-owned (Rust reads it via endpoints::portal_url_for) but its
# prod value is the *shared public web host* (exemem.com — also used by
# billing, verify-email, marketing), not a unique opaque gateway. Scanning
# it would wrongly flag every legitimate exemem.com literal across the
# codebase. The drift this lint guards against is gateway-host drift; the
# public host is well-known and stable.
HOSTNAMES=$(
    jq -r '.environments | to_entries[] | .value | to_entries[] | select(.key != "region" and .key != "portal") | .value' \
        folddb_profile/environments.json \
    | sed -E 's|^https?://||; s|/.*$||' \
    | sort -u
)

# Allowlist: where these hostnames are allowed to appear.
#   folddb_profile/environments.json — the registry itself
#   target/              — generated artifacts (cargo build output)
#   scripts/lints/lint-no-hardcoded-urls.sh — this file (allowed because it
#                                        derives the list dynamically; no
#                                        literal hostname appears here)
#   .git/                — git internals
#   .claude/worktrees/   — sibling worktrees of this same repo (other
#                          checkouts of the same files)
#   docs/dogfood/*.md    — historical run reports; immutable record of
#                          past sessions, not live config

ALLOW_PATHS=(
    './folddb_profile/environments.json'
    './target/'
    './scripts/lints/lint-no-hardcoded-urls.sh'
    './.git/'
    './.claude/worktrees/'
    './docs/dogfood/'
    './snapshots/'
    # Frozen run reports: a proof records what was actually hit during a soak
    # or dogfood run, so the literal gateway URL is the evidence and must not
    # be rewritten to an env name. (Added 2026-08-16: proofs/schema-pow-dev-soak.md
    # landed 2026-08-10 without an allowlist entry and has been failing this
    # lint — and therefore CI step 1 and the pre-commit hook — ever since.)
    './proofs/'
    # OpenAPI spec: its `servers:` block deliberately documents the live
    # gateway URLs for API consumers. (Newly in scope 2026-07-12 when this
    # lint moved from the fold_db_node subtree to repo-wide.)
    './schema_service/openapi.yaml'
)

errors=0
for host in $HOSTNAMES; do
    # `git ls-files` so we only scan tracked files, regardless of cwd noise.
    # Fall back to `find` when not in a git repo (e.g. fresh tarball).
    if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
        candidates=$(git ls-files -z | xargs -0 grep -lF "$host" 2>/dev/null || true)
    else
        candidates=$(grep -rlF "$host" . 2>/dev/null \
            | grep -v '^\./target/' \
            | grep -v '^\./.git/' || true)
    fi

    while IFS= read -r f; do
        [ -z "$f" ] && continue
        # Normalize to leading ./ for allow-list matching.
        path="./${f#./}"
        skip=0
        for allow in "${ALLOW_PATHS[@]}"; do
            if [[ "$path" == "$allow"* ]]; then
                skip=1
                break
            fi
        done
        [ "$skip" -eq 1 ] && continue

        echo "lint-no-hardcoded-urls: FAIL" >&2
        echo "  $path contains hardcoded gateway hostname '$host'." >&2
        # Show the offending lines so the fixer doesn't have to grep.
        grep -nF "$host" "$f" | sed 's/^/    /' >&2
        errors=$((errors + 1))
    done <<< "$candidates"
done

if [ "$errors" -gt 0 ]; then
    cat >&2 <<'EOF'

The cross-environment URLs are owned by folddb_profile/environments.json
(the single source of truth). Do not hardcode them anywhere else.

  Rust:  use folddb_profile::endpoints::{schema_service_url, ...}
         or, for env-pinned access, schema_service_url_for(Environment::Dev).
  Docs:  reference the env name (dev/prod), not the literal URL.

If a file genuinely needs the URL inline (e.g. a frozen historical run
report), add its prefix to ALLOW_PATHS in
scripts/lints/lint-no-hardcoded-urls.sh with a comment explaining why.
EOF
    exit 1
fi

echo "lint-no-hardcoded-urls: ok"
