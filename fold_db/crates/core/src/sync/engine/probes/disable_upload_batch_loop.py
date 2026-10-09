#!/usr/bin/env python3
"""Force the catch-up upload loop to stop after one batch.

Used by last-stack-mutation-probe. The guard test
`mutation_log_upload_pass_runs_more_than_one_batch_while_backlog_remains`
must go RED on this patch.
"""
from pathlib import Path

path = Path("fold_db/crates/core/src/sync/engine/cycle.rs")
text = path.read_text()
old = """    segments_uploaded > 0
        && wake_threshold_ns > 0
        && upload_backlog_after >= wake_threshold_ns
        && !budget.is_zero()
        && elapsed < budget
"""
new = """    let _ = (
        segments_uploaded,
        upload_backlog_after,
        wake_threshold_ns,
        elapsed,
        budget,
    );
    false
"""
count = text.count(old)
assert count == 1, f"anchor count={count} path={path}"
path.write_text(text.replace(old, new, 1))
