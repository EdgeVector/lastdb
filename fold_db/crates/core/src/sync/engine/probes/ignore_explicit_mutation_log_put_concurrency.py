#!/usr/bin/env python3
"""Ignore the explicit mutation-log PUT fan-out at the S3 stream.

Used by last-stack-mutation-probe. The guard test
`mutation_log_backlog_cycle_puts_in_parallel_under_interactive_busy`
must go RED on PUT-PARALLELISM when the floor is computed but never
plumbed into `buffer_unordered`.
"""
from pathlib import Path

path = Path("fold_db/crates/core/src/sync/engine/transfer/s3_io/upload.rs")
text = path.read_text()
old = """            let cap = match put_concurrency {
                Some(explicit) => explicit.max(1),
                None => self.active_upload_caps().await.concurrency.max(1),
            };
"""
new = """            let cap = self.active_upload_caps().await.concurrency.max(1);
            let _ = put_concurrency;
"""
count = text.count(old)
assert count == 1, f"anchor count={count} path={path}"
path.write_text(text.replace(old, new, 1))
