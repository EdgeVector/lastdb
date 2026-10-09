# Atom GC Tip-Fold Gate

`scripts/lastdbd/atom-gc-tip-fold-gate.sh` is the CoW-only gate for proving
atom GC before any primary-facing workflow uses it on fkanban card data.

The gate clones `PRIMARY_HOME`, disables inherited cloud sync state in the clone,
runs `lastdb_local_maintain atom-gc-audit`, runs a dry-run reap, then executes
`atom-gc-reap` against the clone only. It writes `proof.json` with the command
paths, home path type, candidate/delete counts, schema coverage, and
read-after-GC verdicts for:

- `Card`
- `BoardCards`
- `MilestoneCards`

`PASS` means the clone stayed on the at-rest seam, used partition-prefixed atom
keys, and every exercised schema still has a survivor verified by the reaper
after redundant atom copies were deleted. If a schema is missing from the
post-GC affected set while deletes happened, the gate reports
`read-after-gc-fold-agreement` and exits nonzero.

Example:

```bash
cargo build -p lastdb_node --bin lastdb_local_maintain
MAINTAIN=./target/debug/lastdb_local_maintain \
  ./scripts/lastdbd/atom-gc-tip-fold-gate.sh "$(date -u +%Y%m%dT%H%M%SZ)"
```

The script deliberately refuses `WORK_HOME` under `~/.lastdb`, `~/.folddb`, or
`PRIMARY_HOME`. Primary execution remains a separate supervised decision.
