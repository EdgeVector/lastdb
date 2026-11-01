# Schema-root data attribution delivery plan

> **SUPERSEDED.** The record counter is the only authority for the life of a record body
> (`docs/lastdb-verify-ref.md`). This plan was retired as a path to deleting records,
> because no delete uses an attribution walk. It stays as history.

| Field | Value |
|---|---|
| Status | Superseded — kept as history |
| North Star | `north-star-lastdb-schema-root-data-attribution` |
| Milestone | `lastdb-schema-root-data-attribution` (retired; see banner above) |
| Terminal proof | `lastdb-schema-root-data-attribution-proof` (deleted from board; see banner above) |
| Repository | `EdgeVector/fold` |
| Design | `docs/designs/schema-root-data-attribution-and-safe-reclaim.md` (also superseded) |

## Goal

LastDB can classify every object in an isolated database copy from schema,
system, retention, and validated durable-reference roots. It removes only
proven residue from that copy. A successful future write has durable
attribution source evidence before the API returns.

## Chosen architecture

The schema catalog starts the user-data walk. The walker follows declared
field keys, key slots, tips, records, blobs, alternate addresses, history, and valid
reference edges. System and retention roots run in the same epoch.

The ledger stores fixed-size object rows, bounded path rows, cursor rows, and
an ordered event source. It never stores an unbounded schema list in a record.
It shares the existing per-field-key counters for logical size. It reports
inclusive logical size by schema and unique physical size by object.

The first exact-copy operation uses a short owner maintenance write gate. The
gate covers only: event-frontier check, durable flush, snapshot receipt, and
copy identity. The long walk stays online. A future atomic snapshot API may
replace the gate only when it returns the same frontier and copy identity.

## Complexity and capacity

Let `F` be declared field keys, `T` live tip slots, `E` durable edges,
`N` inspected objects, and `A` materialized root-to-object paths.

| Operation | Time | Durable space | Process memory |
|---|---:|---:|---:|
| Catalog and graph walk | O(F + T + E + N) | O(N + A) epoch rows | O(page size) |
| Event catch-up | O(events + changed edges) | O(events) until receipt expiry | O(page size) |
| New write source record | O(1 + changed references) | O(1 + changed references) | O(changed references) |
| Copy scrub | O(residue objects + affected compact shards) | O(delete batches) | O(page size) |
| Final report | O(number of report groups) after rows exist | O(1) extra | O(page size) |

The database reserves one logical-copy capacity. A copy-on-write file system
can start small, but scrub and compaction can break sharing. No normal write
stores an object-wide or schema-wide list.

## Work slices

### 1. Complete the source and epoch boundary

- Finish the public owner status and inventory contract.
- Expose pending scopes, event frontier, catalog generation, and epoch state.
- Make new writes fail closed when the durable source event cannot persist.
- Keep the current pending-marker recovery path as `unknown` until audit or
  retry resolves it.

**Verify:** crash at each source-boundary phase. A successful mutation has one
event. A failed or ambiguous mutation leaves a visible blocker.

### 2. Build the bounded graph projector

- Add resumable pages for schema catalogs, field keys, tips, alternate addresses, records,
  blobs, history, and system roots.
- Write object rows and bounded proof paths in the attribution ledger.
- Apply source events after the base walk. Re-read canonical rows from each
  locator. Never replay an event into an earlier copy.
- Classify unsupported or unreadable evidence as `unknown`.

**Required product evidence:** real operational output covers shared records, alternate addresses, record cycles, blobs,
history roots, system roots, interrupted cursors, and a write during the walk.

### 3. Finish inline size attribution

- Use field key counters as the source for logical value, structure, and
  retained-history size.
- Mark reports incomplete when a counter has no complete source state.
- Show inclusive schema bytes and node-unique physical bytes separately.
- Bootstrap a fresh home without a manual liveness action.

**Verify:** a new home reports complete logical size after its first write.
Shared records change inclusive size without double-counting unique bytes.

### 4. Create and verify an exact isolated copy

- Add the short owner maintenance write gate and snapshot receipt.
- Bind the copy ID and the exact event frontier to the epoch.
- Refuse copy work when the walker, event catch-up, or unknown count is not
  complete.
- Restore the copy in a new temporary home and compare its receipt.

**Verify:** a source write at the cutover boundary is either in the copy and
the frontier, or after both. No mixed state passes verification.

### 5. Scrub and compact only the isolated copy

- Make `copy-scrub` accept one verified copy ID and a complete epoch only.
- Delete only `unattributed-residue` in bounded, idempotent batches.
- Write a receipt before compaction and preserve it after restore.
- Keep source-home operations read-only. Do not add a source-delete command.

**Verify:** a second scrub makes no extra delete. A failed batch resumes.
System, retention, attributed, and unknown rows never delete.

### 6. Ship the owner interface and product proof

- Add `lastdb db inventory` proof and size sections.
- Add attribution status, schema report, copy verification, and dry-run
  promotion-plan commands.
- Use CI product commands on fresh generated data. Keep tests and fixture harnesses retired.
- Document the runbook, capacity need, rollback, and no-source-delete rule.

**Verify:** the terminal proof below exits zero and prints `PASS`.

## Dependencies and order

```text
source boundary ─┬─> graph projector ─> exact copy ─> copy scrub ─> proof
                 └─> inline sizes ────> inventory ───┘
```

The North Star driver creates the milestone and the first work card. The
milestone driver creates later cards only after the dependency frontier clears.

## Terminal verification

This plan requires product evidence from CI on fresh generated data.
Do not start a local node, copy the primary, or create a fixture harness.
Source inspection alone does not prove these results.

1. Use two schemas with independent values, named alternate keys, a blob,
   retained field history, and the actual system roots from product operations.
2. Start attribution. Write one record while the projector runs. Finish the
   event catch-up and take the exact isolated copy.
3. Run the copy scrub and compaction. Restore the copy in a fresh home.
4. Assert all of these facts:
   - source digest did not change;
   - only data with complete authority for removal was deleted;
   - the copy reports zero `unattributed-residue` user objects;
   - the copy reports zero `unknown` user objects;
   - canonical field reuse leaves schema values independent and alternate keys resolve the correct primary records;
   - history and system roots remain;
   - the concurrent write has one source event and one attribution path;
   - a later write receives a durable source event and inline size before its
     response returns.

The CI summary must link the source revisions, commands, and retained operational artifacts for each claimed result.

## Open questions resolved by this plan

- **Does app ownership decide reclaim?** No. Schema and other durable roots
  decide attribution. App data only adds a display label.
- **Does age decide reclaim?** No. Age is not an attribution signal.
- **Does no schema path alone permit delete?** No. System, retention, and
  durable-reference roots must also be complete.
- **Does the normal database delete this residue?** No. The first destructive
  operation is copy-only. Source cutover needs a later decision and design.
- **How does the walker operate during writes?** It uses a snapshot frontier,
  durable source events, bounded cursors, and a short final snapshot gate.
- **Can normal writes leave unseen user data?** No. They persist a source event
  before acknowledgement. Projector delay remains visible and blocks a clean
  copy proof for that scope.
