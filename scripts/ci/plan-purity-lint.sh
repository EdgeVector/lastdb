#!/usr/bin/env bash
# Fail if the mutation-manager plan module names a service type or an effect.
#
# `fold_db_core/mutation_manager/plan/` holds the write decisions as pure
# functions: model values and a gathered snapshot in, a plan value out. The
# caller gathers the inputs with the existing batch reads and commits the plan
# on the unchanged commit path. This gate keeps the decisions free of service
# handles and effects, so a decision cannot read storage, take a lock, read the
# clock or environment, mint an id, or log.
#
# Comment lines are skipped so the module docs can name what it excludes.
# Pure bash and ripgrep. Usage: bash scripts/ci/plan-purity-lint.sh

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
scope="$repo_root/fold_db/crates/core/src/fold_db_core/mutation_manager/plan"

if [[ ! -d "$scope" ]]; then
  echo "FAIL: plan module not found at $scope" >&2
  exit 1
fi

if ! rg --files --glob '*.rs' "$scope" | rg -q .; then
  echo "FAIL: plan module at $scope has no Rust files" >&2
  exit 1
fi

# name|pattern. A pattern is a ripgrep regex.
checks=(
  'service type|\b(MutationManager|AtomStore|PackRuntime|FoldDB|DbOperations|SchemaStore|SchemaCore)\b'
  'tokio|\btokio\b'
  'async|\basync\b'
  'await|\.await\b'
  'block_on|\bblock_on\b'
  'lock|\b(Mutex|RwLock|Condvar|Semaphore|Notify|Barrier|OnceLock|LazyLock|OnceCell|ArcSwap)\b'
  'atomic|\bAtomic[A-Za-z0-9]*\b'
  'task_local|\btask_local\b'
  'thread_local|\bthread_local\b'
  'global|\blazy_static\b|\bstatic\s+mut\b'
  'clock|\b(SystemTime|Instant|Utc|Local)::now\b|\bclock::unix_'
  'ids|\bnew_v4\b|\bthread_rng\b|\brand::|\bOsRng\b'
  'env|\bstd::env\b|\benv::var|\benv_flag::'
  'fs|\bstd::fs\b|\btokio::fs\b|\bFile::(open|create)\b|\bOpenOptions\b'
  'io|\bstd::io\b|\breqwest::'
  'spawn|\bspawn(_blocking)?\s*\('
  'logging|\btracing::|\blog::|\b(trace|debug|info|warn|error)!\s*\('
)

failures=0
for check in "${checks[@]}"; do
  name="${check%%|*}"
  pattern="${check#*|}"
  # Drop matches on comment lines: path:line:   // ...
  hits="$(rg -n --glob '*.rs' -e "$pattern" "$scope" | rg -v '^[^:]+:[0-9]+:[[:space:]]*//' || true)"
  if [[ -n "$hits" ]]; then
    echo "FAIL: plan module names a forbidden ${name}:" >&2
    printf '%s\n' "$hits" >&2
    failures=$((failures + 1))
  fi
done

if (( failures > 0 )); then
  echo "FAIL: ${failures} plan purity check(s) failed." >&2
  echo "Gather inputs and commit effects in the caller; keep decisions in plan/ pure." >&2
  exit 1
fi

echo "OK: mutation_manager/plan names no service type and no effect marker"
