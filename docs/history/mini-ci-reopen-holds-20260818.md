# Investigation: Fold Mini CI anti-reopen holds on current main

**Date:** 2026-08-19
**Card:** `hga-invest-fold-mini-ci-reopen-root-cause-20260818`
**Parents:** `teardown-sync-pin-log-unbounded-chain-walk`, `papercut-lastdb-query-400-actionable-errors`, `lastdb-remove-transform-schema-serde-residue`, `lastdb-resident-models-molecule-key-set`, `lastdb-projection-is-a-filter-row-drop`
**Investigator:** routine `last-stack-fkanban-pickup-w3` run `2026-08-19T21-20-32-779Z`

## Question

Is the historic agent anti-churn `needs_human` hold (HGA 2026-08-08, waiting `hga-invest-fold-mini-clippy-reopen-root-cause`) still warranted on current `main`, or can each parent be closed / re-driven as a thin slice?

Sibling investigation `docs/history/mini-clippy-reopen-root-cause-cloud-sync-20260817.md` covered different parents. This note is the 2026-08-18 five-parent set against `main` at `c1323893f`.

## What Mini actually gates (current main)

Forge job `fmt + clippy (lastdb_node Mini lane)` on `.forgejo/workflows/ci.yml` still includes rustfmt, `cargo clippy -p lastdb_node --lib --bins -- -D warnings`, and required `fold_db` / `lastdb_node` tests (including `cargo test -p fold_db (mutation provenance contracts)`). Reaper text of "fmt+clippy Mini" can hide a later test/OOM step.

Infrastructure that landed **after** the 2026-08-05/06 reaps:

| Fix | PR | On `c1323893f` |
|-----|----|----------------|
| Mini OOM backoff `CARGO_BUILD_JOBS=2` | **#1345** `bceb891d9` | yes |
| Mini-lane clippy pre-push parity | **#1336** `8aa555ec7` | yes |
| Mini-gate step excerpts + failure-detail status | `ci.yml` failure-diagnostics block | present |

**Live focused gate (no stale-PR reopen):** `GET /repos/EdgeVector/fold/commits/main/status` at 2026-08-19T21:28Z → combined `state=success`. Mini `fmt + clippy (lastdb_node Mini lane)` on push of #1655: **Successful in 6m52s** (run 5735). `ci-required` **Successful**. Heavy clippy **Successful in 2m31s** (run 5736). Recent PR Mini runs in the same window are mostly green; one unrelated backup-drain PR failure (run 5733) is not this hold class.

**Conclusion:** the Mini *infrastructure* that produced reopen-churn in early August is obsolete on current main. Remaining holds must be decided per parent against whether the **product work already merged**.

## Parent evidence

### 1. `teardown-sync-pin-log-unbounded-chain-walk`

Historic kill: Forge **#1238** / **#1258** closed unmerged; Mini fmt+clippy RED; reaper `flagged=reopen-churn`. Preserved tip `21edb5cf2`.

| Fact | Source |
|------|--------|
| Product landed as Forgejo **#1442** "fix(sync): checkpoint pin-log materialization frontier" | `merged_at=2026-08-10T21:33:56-07:00`, merge `6692becd6` |
| Merge is ancestor of current main | `git merge-base --is-ancestor 6692becd6 HEAD` → true |
| Replay is frontier-bounded | `replay_pin_log_for_target` reads `materialized_frontier` then `read_pin_log_records_for_target_after` (`scan_range` when bounded) |
| Publish uses `log_from` | `read_pin_log_records_for_target_after(&target, Some(desc.log_from))` |
| O(1) no-change replay test | `pin_log.rs` assertion: "the durable materialized frontier must make a no-change replay O(1)" |

**Recommendation 2026-08-18: (B) no remaining Mini repair. Close parent `done` against #1442.** Do not reopen #1238/#1258.

### 2. `papercut-lastdb-query-400-actionable-errors`

Historic kill: Forge **#1304** reaped three times (Mini fmt+clippy ~1m10s). Goal was structured 400s for malformed `POST /api/query`.

| Fact | Source |
|------|--------|
| Typed rejection + query wiring landed as Forgejo **#1400** | `merged_at=2026-08-08T15:42:05-07:00`, merge `608ee6d43` |
| Merge is ancestor of current main | `git merge-base --is-ancestor 608ee6d43 HEAD` → true |
| `execute_query_route` | `require_keys(&obj, QUERY_REQUIRED_KEYS)` then `Reject::new(RejectKind::InvalidValue)` — no bare `Bad Request` on this route |
| Leftover bare 400s | ratchet in `exec.rs` allows **11** `content_free(400, "Bad Request")` sites on **other** routes (batch mutations, get-schema, molecule/atom/protein, search/native-index). Owned by `papercut-lastdb-ten-owner-socket-routes-still-answer-the-bare-11-byte-bad-request`, **not** this parent |

**Recommendation 2026-08-18: (B) no remaining Mini repair for `/api/query`. Close parent `done` against #1400.** Do not reopen #1304. Remaining owner-socket bare 400s are a different card.

### 3. `lastdb-remove-transform-schema-serde-residue`

Historic kill: Forge **#1303** / **#1315** reopen-churn; Mini fmt+clippy RED ~1m. Preserved branch `kanban/lastdb-remove-transform-schema-serde-residue` @ `321f6a86f`.

| Fact | Source |
|------|--------|
| `transform_fields` still on current main | `fold_db/.../declarative_schemas/mod.rs` and `schema_types.rs`; legacy read test still present |
| `pub struct Transform` product type | not found on main (already gone); residue is the serde/metadata map |
| Mini on main | green (see live gate above) |

**Recommendation 2026-08-18: (A) re-drive as a fresh thin slice on current `origin/main`.** Do **not** reopen #1303/#1315 and do **not** force-push the preserved monopr (27 files, +62/−299). Slice 1 only: drop `transform_fields` from `schema_types` / declarative schema product types + keep legacy JSON accept-and-ignore if the legacy read test still requires it. Mini gate to go green: current `fmt + clippy (lastdb_node Mini lane)` including `cargo fmt --all -- --check` and `cargo clippy -p lastdb_node --lib --bins -- -D warnings`. Host slices (node call-sites, fmt-only follow-up) as later PRs.

### 4. `lastdb-resident-models-molecule-key-set`

Historic kill: **#1318** / **#1325** / **#1347** Mini RED (`lastdb_node --lib` ~292s near-pass / OOM class). Hold-cleared once after #1345, then reaped again.

| Fact | Source |
|------|--------|
| Product landed as Forgejo **#1465** "Model a resident molecule key set" | `merged_at=2026-08-11T19:10:45-07:00`, merge `d6af113b2` |
| Merge is ancestor of current main | `git merge-base --is-ancestor d6af113b2 HEAD` → true |
| Code on main | `ResidentGraph::resident_key_set_range`, completeness/demote, metrics, tests `key_set_range_walk_is_ordered_and_complete_hits` |

**Recommendation 2026-08-18: (B) no remaining Mini repair. Close parent `done` against #1465.** Do not reopen #1318/#1325/#1347.

### 5. `lastdb-projection-is-a-filter-row-drop`

Historic kill: **#1333** / **#1341** Mini `cargo test -p fold_db (mutation provenance contracts)` ~165–176s with no step excerpt (OOM/load). Reaper reopen-churn.

| Fact | Source |
|------|--------|
| Product landed as Forgejo **#1446** "Fix sparse projection row selection" | `merged_at=2026-08-10T23:43:39-07:00`, merge `7574103c0` (includes `3fccfaabf` keep-sparse-on-key-spine) |
| Merge is ancestor of current main | `git merge-base --is-ancestor 7574103c0 HEAD` → true |
| `cokey_primary_field` | schema key field whenever present — projection is columns, not the row set |
| Regression | `sparse_projection_keeps_the_key_field_row_set` |
| Mini OOM class | addressed in-tree by #1345 `CARGO_BUILD_JOBS=2`; mutation-provenance step still wrapped in `mini-gate-step.sh` |

**Recommendation 2026-08-18: (B) no remaining Mini repair. Close parent `done` against #1446.** Do not reopen #1333/#1341.

## Root cause (durable)

1. **Primary (historical):** Mini-lane host OOM / coarse "fmt+clippy Mini" labels plus 1h reaper SLA produced reopen-churn of the **same red heads**.
2. **Infrastructure:** #1345 + #1336 + Mini-gate excerpts made that class obsolete on current main (live Mini 6m52s green).
3. **Board drift:** four of five parents **already merged** under later PRs while cards stayed `backlog`/`needs_human` waiting this investigation.
4. **One real leftover:** transform-schema serde residue was never landed; re-drive as a **new** thin slice, not a reopen.

## Invest card closeout

Durable proof also lives at `~/.last-stack/feature-proofs/hga-invest-fold-mini-ci-reopen-root-cause-20260818.md`. This history note is the merged `Kind: pr` artifact for the investigation child.
