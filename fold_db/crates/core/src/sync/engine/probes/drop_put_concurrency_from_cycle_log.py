#!/usr/bin/env python3
"""Drop put_concurrency from the continuous mutation-log cycle log line.

Used by last-stack-mutation-probe. The guard test
`catch_up_cycle_log_includes_put_concurrency_and_phase_timings`
must go RED on 'cycle log must name the catch-up fan-out'.
"""
from pathlib import Path

path = Path("fold_db/crates/core/src/sync/engine/pin_log.rs")
text = path.read_text()
old = """                published_f = report.published_frontier_after,
                backlog = report.upload_backlog_after,
                put_concurrency = report.put_concurrency,
                scan_ms,
"""
new = """                published_f = report.published_frontier_after,
                backlog = report.upload_backlog_after,
                scan_ms,
"""
count = text.count(old)
assert count == 1, f"anchor count={count} path={path}"
path.write_text(text.replace(old, new, 1))
