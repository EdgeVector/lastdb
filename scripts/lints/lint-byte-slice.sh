#!/usr/bin/env bash
# lint-byte-slice.sh
#
# Fail the build on byte-index slicing of a `&str` that does not snap
# to a UTF-8 char boundary. The two patterns this lint guards:
#
#   1. `&<expr>[..<expr>.len().min(<N>)]` — the direct panic class.
#      Crashes the moment the byte at offset `<N>` lands inside a
#      multi-byte char (em dash, accented letter, emoji, CJK).
#   2. `&<expr>[..<N>]` paired with an `if … .len() > <N>` guard a few
#      lines up — defensive against length but not against multi-byte
#      boundaries. Same panic class.
#
# Both should be replaced with
# `observability::truncate::truncate_on_char_boundary(s, N)`, which
# snaps the cut down to the nearest char boundary and never panics.
#
# Scope: pass one or more `--scope=<dir>` flags. Each `<dir>` is either
# a multi-crate workspace parent (`<dir>/<crate>/src/`) or a
# single-crate root (`<dir>/src/`); detected automatically. Tests under
# `<scope>/<crate>/tests/` are out of scope at the directory level;
# intentional in-`src` test fixtures use the inline override below.
#
# Override: add a comment containing the literal `lint:byte-slice-ok
# <reason>` on the violating line OR on the line immediately above it.
# Use sparingly — typically only for slicing a byte buffer (`&[u8]`)
# that happens to live behind a `&str`-like identifier in a test.
#
# Exit code: 0 if every match is overridden, 1 otherwise.

set -euo pipefail

scopes=()
for arg in "$@"; do
    case "$arg" in
        --scope=*)
            scopes+=("${arg#--scope=}")
            ;;
        -h|--help)
            sed -n '2,28p' "$0"
            exit 0
            ;;
        *)
            echo "lint-byte-slice: unknown argument: $arg" >&2
            echo "usage: bash scripts/lints/lint-byte-slice.sh --scope=<dir> [--scope=<dir>...]" >&2
            exit 2
            ;;
    esac
done

if [[ ${#scopes[@]} -eq 0 ]]; then
    echo "lint-byte-slice: at least one --scope=<dir> required" >&2
    echo "usage: bash scripts/lints/lint-byte-slice.sh --scope=<dir> [--scope=<dir>...]" >&2
    exit 2
fi

# Anchored on the `.len().min(<int>)` shape because that is the exact
# pattern that crashed the actix worker. The `&` and the leading bracket
# are matched to avoid hitting unrelated `.len().min(N)` arithmetic.
PATTERN='\[\.\.[^]]*\.len\(\)\.min\([0-9]+\)\]'

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
REPO_ROOT="$( cd "$SCRIPT_DIR/../.." && pwd )"
cd "$REPO_ROOT"

if ! command -v rg >/dev/null 2>&1; then
    echo "lint-byte-slice: ripgrep (rg) not found in PATH" >&2
    exit 1
fi

targets=()
for scope in "${scopes[@]}"; do
    if [[ ! -d "$scope" ]]; then
        echo "lint-byte-slice: scope directory not found: $scope" >&2
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
        echo "lint-byte-slice: no $scope/src or $scope/*/src directories found" >&2
        exit 1
    fi
done

tmp=$(mktemp)
trap 'rm -f "$tmp"' EXIT INT HUP TERM

rg --pcre2 -n "$PATTERN" "${targets[@]}" > "$tmp" 2>/dev/null || true

failed=0
hits=0

while IFS= read -r match; do
    [[ -z "$match" ]] && continue

    file="${match%%:*}"
    rest="${match#*:}"
    lineno="${rest%%:*}"
    content="${rest#*:}"

    if printf '%s\n' "$content" | grep -q 'lint:byte-slice-ok'; then
        continue
    fi

    if [[ "$lineno" -gt 1 && -r "$file" ]]; then
        prev_lineno=$((lineno - 1))
        prev=$(sed -n "${prev_lineno}p" "$file" 2>/dev/null || true)
        if printf '%s\n' "$prev" | grep -q 'lint:byte-slice-ok'; then
            continue
        fi
    fi

    hits=$((hits + 1))
    echo "ERROR: $file:$lineno — UTF-8-unsafe byte-index slice on a string"
    echo "    $content"
    failed=1
done < "$tmp"

if [[ "$failed" -ne 0 ]]; then
    cat >&2 <<EOF

Found $hits UTF-8-unsafe byte-index slice site(s).

Plain \`&s[..s.len().min(N)]\` panics when byte N lands inside a
multi-byte UTF-8 character — em dash (3 bytes), accented Latin letter
(2 bytes), emoji (4 bytes), CJK ideograph (3 bytes). On an actix
worker the panic kills the worker thread and the client sees
\`Empty reply from server\`.

Use the shared char-boundary-safe helper instead:

    use observability::truncate::truncate_on_char_boundary;
    truncate_on_char_boundary(&s, 200)

For an intentional exception (e.g. slicing a \`&[u8]\` buffer that
happens to live behind a string-like identifier), add an inline
comment containing \`lint:byte-slice-ok <reason>\` on the same line
or directly above.
EOF
    exit 1
fi

echo "lint-byte-slice: ok — no UTF-8-unsafe byte-index slice sites in ${#targets[@]} src tree(s)."
