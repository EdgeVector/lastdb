#!/usr/bin/env bash
# lint-redaction.sh
#
# Fail the build if a `tracing` macro emits a sensitive field as a raw value
# instead of through `observability::redact!()` / `observability::redact_id!()`.
#
# Sensitive fields:
#   password, token, api_key, secret, auth_token, email, phone, ssn,
#   user_hash, user_id
#
# `user_hash` / `user_id` are the per-user identifiers FoldDB threads
# through every HTTP request — they're 32-hex-char hashes (`user_hash`)
# or the same hash exposed under a different name (`user_id`).
# Emitting them raw lets anyone with log access correlate every action
# a user took. Wrap with `redact_id!()` so they emit as
# `<id:HHHHHHHH>` (8-hex-char low-32-bit xxhash64) — still
# correlatable across log lines, but no longer the raw identifier.
#
# Scope: pass one or more `--scope=<dir>` flags. Each `<dir>` is either:
#   - a Cargo workspace parent containing `<dir>/<crate>/src/` (multi-crate
#     layout — e.g. `fold_db/crates`, `schema_service/crates`), OR
#   - a single-crate root containing `<dir>/src/` directly (e.g.
#     `fold_db_node`).
# The two cases are detected automatically: if `<scope>/src/` exists it is
# used directly; otherwise the multi-crate glob `<scope>/*/src/` applies.
# Example:
#
#     bash scripts/lints/lint-redaction.sh \
#         --scope=fold_db/crates \
#         --scope=schema_service/crates \
#         --scope=fold_db_node
#
# Tests under `<scope>/<crate>/tests/` (or `<scope>/tests/` for single-crate
# scopes) and FMT-layer self-tests are out of scope at the directory level;
# intentional raw-value test fixtures inside `src/` use the inline override
# below.
#
# Override: add a comment containing the literal `lint:redaction-ok <reason>`
# on the violating line OR on the line immediately above it. The two-line
# window is so the override survives `rustfmt`, which will lift a long
# trailing comment onto its own line. Example:
#
#     // lint:redaction-ok FMT-layer test must emit raw value to verify deny-list
#     tracing::info!(password = "hunter2", "login");
#
# Use overrides sparingly — typically only for unit tests that need to feed
# the raw value to verify the FMT layer's deny-list.
#
# Exit code: 0 if every match is wrapped or overridden, 1 otherwise.

set -euo pipefail

scopes=()
for arg in "$@"; do
    case "$arg" in
        --scope=*)
            scopes+=("${arg#--scope=}")
            ;;
        -h|--help)
            sed -n '2,40p' "$0"
            exit 0
            ;;
        *)
            echo "lint-redaction: unknown argument: $arg" >&2
            echo "usage: bash scripts/lints/lint-redaction.sh --scope=<dir> [--scope=<dir>...]" >&2
            exit 2
            ;;
    esac
done

if [[ ${#scopes[@]} -eq 0 ]]; then
    echo "lint-redaction: at least one --scope=<dir> required" >&2
    echo "usage: bash scripts/lints/lint-redaction.sh --scope=<dir> [--scope=<dir>...]" >&2
    exit 2
fi

PATTERN='tracing::(info|warn|debug|error|trace)!.*?(password|token|api_key|secret|auth_token|email|phone|ssn|user_hash|user_id)\s*=\s*[^,]'

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
# scripts/lints/<script> → repo root is two dirs up.
REPO_ROOT="$( cd "$SCRIPT_DIR/../.." && pwd )"
cd "$REPO_ROOT"

if ! command -v rg >/dev/null 2>&1; then
    echo "lint-redaction: ripgrep (rg) not found in PATH" >&2
    exit 1
fi

# Resolve each scope to its `src` dir(s): either `<scope>/src` (single-crate
# layout) or the set of `<scope>/<crate>/src` dirs (multi-crate layout).
targets=()
for scope in "${scopes[@]}"; do
    if [[ ! -d "$scope" ]]; then
        echo "lint-redaction: scope directory not found: $scope" >&2
        exit 1
    fi
    if [[ -d "$scope/src" ]]; then
        targets+=("$scope/src")
        continue
    fi
    found_any=0
    for d in "$scope"/*/src; do
        if [[ -d "$d" ]]; then
            targets+=("$d")
            found_any=1
        fi
    done
    if [[ $found_any -eq 0 ]]; then
        echo "lint-redaction: no $scope/src or $scope/*/src directories found" >&2
        exit 1
    fi
done

tmp=$(mktemp)
trap 'rm -f "$tmp"' EXIT INT HUP TERM

# rg --pcre2 -n: numbered lines; `|| true` because rg exits 1 when no matches.
rg --pcre2 -n "$PATTERN" "${targets[@]}" > "$tmp" 2>/dev/null || true

failed=0
hits=0

while IFS= read -r match; do
    [[ -z "$match" ]] && continue

    file="${match%%:*}"
    rest="${match#*:}"
    lineno="${rest%%:*}"
    content="${rest#*:}"

    # Override on the violating line itself.
    if printf '%s\n' "$content" | grep -q 'lint:redaction-ok'; then
        continue
    fi

    # Override on the line directly above (so rustfmt is free to lift a
    # trailing comment onto its own line without breaking the override).
    if [[ "$lineno" -gt 1 && -r "$file" ]]; then
        prev_lineno=$((lineno - 1))
        prev=$(sed -n "${prev_lineno}p" "$file" 2>/dev/null || true)
        if printf '%s\n' "$prev" | grep -q 'lint:redaction-ok'; then
            continue
        fi
    fi

    # Extract the right-hand side starting at the sensitive field name and
    # check whether it routes through redact!() / redact_id!() before the
    # next field separator. We accept either `field = %redact!(x)` (the
    # `tracing` `%`-display form) or a bare `redact!(...)` / `redact_id!(...)`.
    rhs=$(printf '%s\n' "$content" | grep -oE '(password|token|api_key|secret|auth_token|email|phone|ssn|user_hash|user_id)[[:space:]]*=[^,]*' | head -1)
    if printf '%s\n' "$rhs" | grep -qE 'redact(_id)?!\('; then
        continue
    fi

    hits=$((hits + 1))
    echo "ERROR: $file:$lineno — sensitive field emitted without redact!() / redact_id!()"
    echo "    $content"
    failed=1
done < "$tmp"

# --- Isolated-payload pattern: query-filter values in log/trace output ------
#
# Query filters and record keys carry record-key PAYLOAD from the queried
# schema. For an app-isolation `Isolated` namespace those must never reach logs
# (invariant I4, app_security_model.md — the logs side channel). The blessed
# idiom routes the value through
# `fold_db::app_isolation::redact_debug_if_isolated(&schema_name, &value)`
# (or `redact_if_isolated` for plain strings), which is
# the identity for every non-isolated schema. This pattern flags the raw
# Debug-emission shapes
#     "... <ident>={:?} ..."     (positional Debug interpolation)
#     <ident> = ?expr            (tracing structured Debug capture)
# for the set of payload-bearing identifiers below — not just the literal
# token `filter`, so a record key emitted as `key_value={:?}` is caught too.
# Keep the list to identifiers that actually carry queried-schema payload (a
# bare `key`/`value` would be too broad); extend it when a new payload-bearing
# field name appears. Override with `lint:redaction-ok <reason>` exactly like
# the field rule.
ISOLATED_IDENTS='filter|range_filter|hash_range_filter|record_key|key_value|payload'
ISOLATED_PATTERN="\\b(${ISOLATED_IDENTS})\\b\\s*=\\s*(\\{:\\?\\}|\\?[A-Za-z_&])"

rg --pcre2 -n "$ISOLATED_PATTERN" "${targets[@]}" > "$tmp" 2>/dev/null || true

while IFS= read -r match; do
    [[ -z "$match" ]] && continue

    file="${match%%:*}"
    rest="${match#*:}"
    lineno="${rest%%:*}"
    content="${rest#*:}"

    if printf '%s\n' "$content" | grep -q 'lint:redaction-ok'; then
        continue
    fi
    if [[ "$lineno" -gt 1 && -r "$file" ]]; then
        prev_lineno=$((lineno - 1))
        prev=$(sed -n "${prev_lineno}p" "$file" 2>/dev/null || true)
        if printf '%s\n' "$prev" | grep -q 'lint:redaction-ok'; then
            continue
        fi
    fi

    hits=$((hits + 1))
    echo "ERROR: $file:$lineno — query-filter value Debug-emitted without isolated-namespace redaction"
    echo "    $content"
    echo "    Route it through fold_db::app_isolation::redact_debug_if_isolated(&schema_name, &filter)"
    failed=1
done < "$tmp"

if [[ "$failed" -ne 0 ]]; then
    cat >&2 <<EOF

Found $hits unredacted sensitive-field site(s) in tracing macros.

Wrap the value in observability::redact!(...) (opaque "<redacted>") or
observability::redact_id!(...) (correlatable hash). Example:

    tracing::info!(
        api_key = %observability::redact!(&api_key),
        user.hash = %observability::redact_id!(&user_hash),
        "request received",
    );

For an intentional exception (e.g. a test feeding the FMT layer a raw value
to verify deny-list redaction), add an inline comment containing
\`lint:redaction-ok <reason>\` on the same line.

See docs/observability/redaction-lint.md for guidance.
EOF
    exit 1
fi

echo "lint-redaction: ok — no unredacted sensitive-field tracing call sites in ${#targets[@]} src tree(s)."
