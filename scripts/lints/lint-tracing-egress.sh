#!/usr/bin/env bash
# lint-tracing-egress.sh
#
# Enforce that every `reqwest::Client` / `reqwest::ClientBuilder` construction
# inside each scoped `src/` tree (either `<scope>/<crate>/src/` for multi-crate
# workspaces or `<scope>/src/` for single-crate roots) carries a
# `// trace-egress: <class>` classifier comment within the 3 lines immediately
# preceding it.
#
# Classes (observability propagation):
#   propagate — call goes to one of our own services; .send() should be wrapped with
#               `observability::propagation::inject_w3c`.
#   loopback  — same as propagate but for internal localhost loopback / test fakes.
#   skip-s3   — presigned-URL S3 calls; injecting headers would corrupt the signature.
#   skip-3p   — third-party (Anthropic, Ollama, OpenRouter, Stripe, etc.) that does not
#               honour traceparent.
#
# Tests under `<scope>/<crate>/tests/` (or `<scope>/tests/` for single-crate
# scopes — top-level integration tests) are out of scope; classification
# matters at runtime, not in test scaffolding outside `src/`.
#
# Scope: pass one or more `--scope=<dir>` flags. Each `<dir>` is either a
# multi-crate workspace parent (`<dir>/<crate>/src/`) or a single-crate root
# (`<dir>/src/`); the layout is detected automatically. Example:
#
#     bash scripts/lints/lint-tracing-egress.sh \
#         --scope=fold_db/crates \
#         --scope=schema_service/crates \
#         --scope=fold_db_node \
#         --strict
#
# Usage:
#   bash scripts/lints/lint-tracing-egress.sh --scope=<dir>            # warn-only
#   bash scripts/lints/lint-tracing-egress.sh --scope=<dir> --strict   # fail (CI)
#
# Default mode is warn-only so a half-finished local edit doesn't block iteration.
# CI runs with `--strict` so unclassified constructions cannot land.

set -euo pipefail

scopes=()
strict=0
for arg in "$@"; do
    case "$arg" in
        --scope=*)
            scopes+=("${arg#--scope=}")
            ;;
        --strict)
            strict=1
            ;;
        -h|--help)
            sed -n '2,37p' "$0"
            exit 0
            ;;
        *)
            echo "lint-tracing-egress: unknown argument: $arg" >&2
            echo "usage: bash scripts/lints/lint-tracing-egress.sh --scope=<dir> [--scope=<dir>...] [--strict]" >&2
            exit 2
            ;;
    esac
done

if [[ ${#scopes[@]} -eq 0 ]]; then
    echo "lint-tracing-egress: at least one --scope=<dir> required" >&2
    echo "usage: bash scripts/lints/lint-tracing-egress.sh --scope=<dir> [--scope=<dir>...] [--strict]" >&2
    exit 2
fi

PATTERN='reqwest::(Client|ClientBuilder)::(new|default|builder)\(\)'

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
REPO_ROOT="$( cd "$SCRIPT_DIR/../.." && pwd )"
cd "$REPO_ROOT"

# Resolve each scope to its `src` dir(s): either `<scope>/src` (single-crate
# layout) or the set of `<scope>/<crate>/src` dirs (multi-crate layout).
targets=()
for scope in "${scopes[@]}"; do
    if [[ ! -d "$scope" ]]; then
        echo "lint-tracing-egress: scope directory not found: $scope" >&2
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
        echo "lint-tracing-egress: no $scope/src or $scope/*/src directories found" >&2
        exit 1
    fi
done

violations=0
total=0

while IFS= read -r match; do
    [[ -z "$match" ]] && continue

    file="${match%%$'\t'*}"
    rest="${match#*$'\t'}"

    if [[ "$file" == "$match" || ! "$rest" =~ ^([0-9]+): ]]; then
        continue
    fi

    total=$((total + 1))

    lineno="${BASH_REMATCH[1]}"

    start=$((lineno - 3))
    [[ $start -lt 1 ]] && start=1
    end=$((lineno - 1))

    preceding=""
    if [[ $end -ge 1 ]]; then
        preceding=$(sed -n "${start},${end}p" "$file")
    fi

    if ! printf '%s\n' "$preceding" | grep -q '// trace-egress:'; then
        if [[ $strict -eq 1 ]]; then
            echo "ERROR: $file:$lineno — reqwest::Client construction without // trace-egress: classifier in preceding 3 lines"
        else
            echo "WARN: $file:$lineno — reqwest::Client construction without // trace-egress: classifier in preceding 3 lines"
        fi
        violations=$((violations + 1))
    fi
done < <(
    while IFS= read -r -d '' source_file; do
        while IFS= read -r hit; do
            printf '%s\t%s\n' "$source_file" "$hit"
        done < <(grep -nIE "$PATTERN" "$source_file" 2>/dev/null || true)
    done < <(
        find "${targets[@]}" \
            \( -type d \( -name target -o -name binaries \) -prune \) \
            -o -type f -print0
    )
)

if [[ $violations -ne 0 ]]; then
    cat >&2 <<'EOF'

Add a comment like '// trace-egress: <propagate|loopback|skip-s3|skip-3p>' on
one of the 3 lines immediately preceding each reqwest::Client construction.
See docs/observability/tracing-egress-lint.md for guidance.
EOF
    if [[ $strict -eq 1 ]]; then
        exit 1
    fi
    echo "lint-tracing-egress: warn — $violations of $total reqwest construction sites are unclassified (run with --strict to fail)."
    exit 0
fi

echo "lint-tracing-egress: ok — all $total reqwest construction sites in ${#targets[@]} src tree(s) are classified."
