#!/usr/bin/env bash
# lint-spawn-instrument.sh
#
# Enforce that every `tokio::spawn(async ... { ... })` site inside each
# scoped `src/` tree (either `<scope>/<crate>/src/` for multi-crate
# workspaces or `<scope>/src/` for single-crate roots) either:
#   - chains `.instrument(tracing::Span::current())`
#   - chains `.in_current_span()`
#   - carries an explicit `// lint:spawn-bare-ok <reason>` override on the
#     spawn line, the line immediately preceding it, or anywhere inside
#     the spawn call's parenthesised body.
#
# The "window" for the check is the spawn call's own parenthesised body —
# from `tokio::spawn(` through its matching `)` — found by balancing
# parens forward from the spawn line. This avoids fragile fixed-line
# heuristics that miss `.instrument(...)` parked just past the body.
#
# Why this matters (observability):
#   Without `.instrument(...)`, the spawned future starts with an empty
#   span stack — the parent's `trace_id` / `user.hash` / `schema.name`
#   never propagate across the spawn boundary, so child logs can't be
#   stitched back to the originating request in the trace.
#
# `Span::current()` is always safe: when there is no enclosing span it
# resolves to a disabled root that the OTel layer skips. The override
# comment exists for spawns that genuinely have no parent context
# (perpetual workers started at `init_*`, `#[cfg(test)]` scaffolding).
#
# Scope: pass one or more `--scope=<dir>` flags. Each `<dir>` is either
# a multi-crate workspace parent (`<dir>/<crate>/src/`) or a single-crate
# root (`<dir>/src/`); the layout is detected automatically. Example:
#
#     bash scripts/lints/lint-spawn-instrument.sh \
#         --scope=schema_service/crates \
#         --scope=fold_db_node
#
# Top-level integration tests under `<scope>/<crate>/tests/` (or
# `<scope>/tests/` for single-crate scopes) are out of scope at the
# directory level — instrumentation is a runtime correctness invariant
# that matters at request-path call sites, not in test scaffolding
# outside `src/`.
#
# Exit code: 0 if every match is instrumented or marked, 1 otherwise.
#
# See docs/observability/spawn-instrument-lint.md for the canonical
# pattern and override syntax.

set -euo pipefail

scopes=()
for arg in "$@"; do
    case "$arg" in
        --scope=*)
            scopes+=("${arg#--scope=}")
            ;;
        -h|--help)
            sed -n '2,46p' "$0"
            exit 0
            ;;
        *)
            echo "lint-spawn-instrument: unknown argument: $arg" >&2
            echo "usage: bash scripts/lints/lint-spawn-instrument.sh --scope=<dir> [--scope=<dir>...]" >&2
            exit 2
            ;;
    esac
done

if [[ ${#scopes[@]} -eq 0 ]]; then
    echo "lint-spawn-instrument: at least one --scope=<dir> required" >&2
    echo "usage: bash scripts/lints/lint-spawn-instrument.sh --scope=<dir> [--scope=<dir>...]" >&2
    exit 2
fi

# Maximum forward-line scan when balancing the spawn's parens. Large
# enough to cover any sane body, small enough to avoid runaway scans on
# malformed input.
MAX_FORWARD_LINES=200

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
REPO_ROOT="$( cd "$SCRIPT_DIR/../.." && pwd )"
cd "$REPO_ROOT"

# Resolve each scope to its `src` dir(s): either `<scope>/src` (single-crate
# layout) or the set of `<scope>/<crate>/src` dirs (multi-crate layout).
targets=()
for scope in "${scopes[@]}"; do
    if [[ ! -d "$scope" ]]; then
        echo "lint-spawn-instrument: scope directory not found: $scope" >&2
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
        echo "lint-spawn-instrument: no $scope/src or $scope/*/src directories found" >&2
        exit 1
    fi
done

# Print the full text of the tokio::spawn(...) call, beginning at $2
# (1-based line number) of $1, by balancing parentheses character-by-
# character. Strings, char literals, line comments, and block comments
# are skipped so their parens don't poison the depth count.
spawn_call_text() {
    local file="$1"
    local start_line="$2"
    awk -v start="$start_line" -v max="$MAX_FORWARD_LINES" '
        BEGIN {
            depth = 0
            opened = 0
            in_block_comment = 0
            collected = ""
        }
        NR >= start {
            line = $0
            if (collected == "") {
                collected = line
            } else {
                collected = collected "\n" line
            }
            i = 1
            n = length(line)
            in_string = 0
            string_char = ""
            in_line_comment = 0
            while (i <= n) {
                ch = substr(line, i, 1)
                next_ch = (i < n) ? substr(line, i + 1, 1) : ""

                if (in_line_comment) {
                    i++
                    continue
                }
                if (in_block_comment) {
                    if (ch == "*" && next_ch == "/") {
                        in_block_comment = 0
                        i += 2
                        continue
                    }
                    i++
                    continue
                }
                if (in_string) {
                    if (ch == "\\") {
                        i += 2
                        continue
                    }
                    if (ch == string_char) {
                        in_string = 0
                    }
                    i++
                    continue
                }

                if (ch == "/" && next_ch == "/") {
                    in_line_comment = 1
                    i += 2
                    continue
                }
                if (ch == "/" && next_ch == "*") {
                    in_block_comment = 1
                    i += 2
                    continue
                }
                if (ch == "\"" || ch == "\x27") {
                    in_string = 1
                    string_char = ch
                    i++
                    continue
                }
                if (ch == "(") {
                    depth++
                    opened = 1
                } else if (ch == ")") {
                    depth--
                    if (opened && depth == 0) {
                        print collected
                        exit
                    }
                }
                i++
            }
            if (NR - start + 1 >= max) {
                print collected
                exit
            }
        }
    ' "$file"
}

failed=0
total=0

trim_left() {
    local s="$1"
    s="${s#"${s%%[![:space:]]*}"}"
    printf '%s' "$s"
}

while IFS= read -r match; do
    [[ -z "$match" ]] && continue

    file="${match%%:*}"
    rest="${match#*:}"
    lineno="${rest%%:*}"

    spawn_line=$(sed -n "${lineno}p" "$file")

    # Skip doc comments and comment-block lines (the rg target is real
    # spawn calls, not occurrences of `tokio::spawn(` inside docstrings).
    trimmed=$(trim_left "$spawn_line")
    case "$trimmed" in
        //*|\**|/\**)
            continue
            ;;
    esac

    # Confirm this is a spawn of an async block, not `tokio::spawn(some_future)`.
    # The `async` keyword may sit on the spawn line itself or the next 1-2
    # lines (we have both styles in-tree).
    async_end=$((lineno + 2))
    async_window=$(sed -n "${lineno},${async_end}p" "$file")
    if ! grep -qE 'tokio::spawn\(\s*async|^[[:space:]]*async[[:space:]]+(move[[:space:]]+)?\{|^[[:space:]]*async[[:space:]]*\{' <<<"$async_window"; then
        continue
    fi

    total=$((total + 1))

    body=$(spawn_call_text "$file" "$lineno")

    # Allow override on the line just preceding the spawn too — a common
    # place reviewers expect the rationale to live.
    prev_line=""
    if [[ $lineno -gt 1 ]]; then
        prev_line=$(sed -n "$((lineno - 1))p" "$file")
    fi

    if grep -qE '\.instrument\(|\.in_current_span\(\)|// lint:spawn-bare-ok' <<<"$body"; then
        continue
    fi
    if grep -qE '// lint:spawn-bare-ok' <<<"$prev_line"; then
        continue
    fi

    echo "ERROR: $file:$lineno — tokio::spawn(async ...) without .instrument() / .in_current_span() / override inside the spawn call"
    failed=1
done < <(grep -rnE 'tokio::spawn\(' "${targets[@]}" 2>/dev/null || true)

if [[ $failed -ne 0 ]]; then
    cat >&2 <<'EOF'

Each tokio::spawn(async ...) must either:
  - chain .instrument(tracing::Span::current()) on the future, or
  - chain .in_current_span() on the future, or
  - carry a `// lint:spawn-bare-ok <reason>` comment on the spawn line,
    the line immediately preceding it, or anywhere inside the spawn body.

See docs/observability/spawn-instrument-lint.md for guidance and the
list of pre-approved bare-spawn rationales.
EOF
    exit 1
fi

echo "lint-spawn-instrument: ok — all $total tokio::spawn(async) sites in ${#targets[@]} src tree(s) are instrumented or explicitly marked."
