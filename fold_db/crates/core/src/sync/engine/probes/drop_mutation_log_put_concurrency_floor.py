#!/usr/bin/env python3
"""Drop the mutation-log catch-up PUT fan-out floor.

Used by last-stack-mutation-probe. The guard test
`mutation_log_backlog_cycle_puts_in_parallel_under_interactive_busy`
must go RED on PUT-PARALLELISM.
"""
from pathlib import Path

path = Path("fold_db/crates/core/src/sync/engine/pin_log.rs")
text = path.read_text()
old = """    if let Some(explicit) = env_override {
        return explicit.clamp(1, MUTATION_LOG_PUT_CONCURRENCY_MAX);
    }
    let policy = policy_concurrency.max(1);
    if objects_in_cycle > MUTATION_LOG_CATCH_UP_OBJECTS {
        policy.max(MUTATION_LOG_CATCH_UP_PUT_CONCURRENCY)
    } else {
        policy
    }
"""
new = """    if let Some(explicit) = env_override {
        return explicit.clamp(1, MUTATION_LOG_PUT_CONCURRENCY_MAX);
    }
    let _ = objects_in_cycle;
    policy_concurrency.max(1)
"""
count = text.count(old)
assert count == 1, f"anchor count={count} path={path}"
path.write_text(text.replace(old, new, 1))
