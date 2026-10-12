# Resident set convergence: one cache, one ordering gate, one persist path

Status: design notes. Nothing here is implemented except the removal of the
never-populated `rehydrate_flights` map. Written from reading the code in
`fold_db/crates/core/src/resident/`, `storage/laststore/logical_path/` and
`fold_db_core/mutation_manager/`. It has not been run against real data.

## Goal

The product law (`docs/lastdb-canonical-model.md` section 5) names one resident
graph. The code has three caches over the same ladder objects. The goal is one
cache, one mutation ordering gate and one persist path, with no on-disk or wire
change.

## Fixed constraints

- A write is acknowledged after it is applied in memory, before it is durable.
  Disk follows through the persist lanes, cloud follows disk. A change must not
  delay the acknowledgment to meet a latency bar.
- A read after an acknowledgment sees the write.
- A purge never drops a record whose write has not been persisted.
- No tests are added. The proof is the two operational bars,
  `scripts/lastdbd/resident-latency-bar.sh` and
  `scripts/lastdbd/read-after-ack-bar.sh`, run on a copy of real data, and then
  on the primary. A cloud sandbox cannot run them.

## Today

| Cache | Where | Bound | Mode gated |
|---|---|---|---|
| `LogicalResidentSet` | store layer, `storage/laststore/logical_path/` | 10000 records (`RESIDENT_KEY_CAP`), LRU, skips held and dirty records | no, always on |
| `ResidentGraph` | `DbOperations`, one per database | byte budget (`LASTDB_RESIDENT_BYTES`) | resident-first reads and acknowledge-on-apply depend on the mode; the graph itself is built and written in every mode |
| Hash-group warm set | store layer, non-logical collections | `LASTDB_HASH_GROUP_WARM_BYTES` | no |

The hash-group warm set is a storage-layer page cache for indexes and metadata.
It is out of scope.

## What the graph does that the logical set does not

1. Per-slot `resident_revision` and `durable_revision`. These are the mutation
   concurrency gate (`mutation_manager/molecules/apply_gate.rs`) and the persist
   turn order (`wait_for_persist_turn`, `complete_persist_turn`).
2. Acknowledge-on-apply in `write` mode, with a deferred persist.
3. Range coverage (`coverage.rs`, `page_coverage.rs`) and the Skip-Delete
   tombstone overlay.
4. A second copy of tips, atoms and schemas.

Item 4 is the duplicate. Items 1 to 3 are real behavior that must survive.

## Target

- The logical set is the only cache of tips and atoms.
- Slot revisions, persist turns, dirty tracking and the deferred acknowledgment
  live in the mutation path and persist lanes, not in a cache.
- Range coverage and the Skip-Delete tombstones move to the logical set.
- The graph's tip, atom and schema maps, its byte ledger,
  `LASTDB_RESIDENT_BYTES` and the resident-first read branch are deleted.

## Why slot revisions cannot move first

An earlier plan moved slot revisions out of the graph as step 2. Reading the
code shows that order is wrong.

The `slots` map holds the revision counters and the cache state in one value,
and one lock covers it. Three things depend on that single lock:

- `install_tip` compares the observed revisions with the current ones and then
  inserts into the tip map in one critical section. This rejects a stale cold
  rehydrate. Splitting the revisions into another structure turns it into a
  check across two locks.
- Eviction and purge refuse to drop a tip whose `resident_revision` differs from
  its `durable_revision`, and bump both under the same lock as the tip removal.
- A cold read holds a `SlotRead` so that eviction cannot recycle a revision
  identity while the disk read is in flight.

Those three exist because the graph is a cache. They disappear when the graph's
cache role is deleted. What remains is the ordering gate and the persist turn,
which need only the two counters.

## Revised order

Each step is its own PR, carries a bar receipt, and is not done until the
receipt also comes from the primary.

1. Remove dead code that sits on the same locks. Done for `rehydrate_flights`.
2. Measure. Run both bars with `LASTDB_RESIDENT_MODE` set to `off`, `read` and
   `write` on a copy of real data, and keep the numbers.
3. Teach the logical set the missing behavior: range coverage, the Skip-Delete
   overlay, and acked-but-unflushed tips that survive purge until the persist
   lane reports them durable. Keep the graph in place and verify the logical set
   gives identical answers.
4. Switch reads from the graph to the logical set. This removes the read mode.
5. Move the slot revisions and the persist turn to the mutation path as plain
   counters, now that no cache shares their lock.
6. Delete the graph's maps, ledger and the remaining env vars.

## Baseline (step 2), 2026-10-12

Measured by the owner on a copy-on-write copy of the real data, with a release
`lastdbd` built from `main` at 817b7d55. Bars: point p50 at most 50 ms, write
p50 at most 10 ms. Every run was RED.

| Mode | Point p50 | Point p95 | Write p50 | Write p95 |
|---|---|---|---|---|
| off | 30 ms | 55 ms | 641 ms | 1248 ms |
| read | 28 ms | 61 ms | 688 ms | 1254 ms |
| write | 30 ms | 60 ms | 46 ms | 111 ms |

What this means:

- Point reads pass in all three modes. The always-on logical set already serves
  them, so the graph's resident-first read branch buys nothing measurable. Step 4
  should not cost read latency.
- `read` mode is indistinguishable from `off`.
- `write` mode is the only mode that matters, and it matters for writes only:
  about 14 times faster at p50. It still misses the 10 ms bar, so these numbers
  are the figures to hold, not a target already met. A step must not move the
  write p50 above 46 ms or the point p50 above 30 ms.

Read-after-ack, `write` mode, 20 reps per cell: create by point-get and update
by point-get pass. Create by page, update by page and create by count-signal are
stale in all 20 reps. The defer window reported `NOT_OPEN`.

These results are not yet interpreted, for three reasons found by reading
`scripts/lastdbd/read-after-ack-bar.sh`:

- The run had no `off` baseline, so a write-mode gap cannot be told from a bar
  problem. The script now takes `--mode`.
- The count-signal cell only checks that the response has `.ok`, `.total_count`
  or `.data`. It cannot detect stale data.
- A freshness race would fail some reps. Failing every rep, while the point-get
  on the same key passes, points to something deterministic: either the page
  query does not surface the `default` key within its first 500 rows, or page
  reads do not overlay acknowledged but unflushed tips in `write` mode. The
  second would be a real defect and step 3 must not make it worse.
- `NOT_OPEN` was read from real metrics, not a failed lookup. `persist_enqueued`
  counts newly dirty keys and every rep writes one key, so it can stay flat while
  deferral works. The raw before and after values decide it.
- Delete cells are not implemented, so Skip-Delete read-after-ack is untested.

To settle it, run the same cells with `--mode off`, and look at the raw
`sample_response` lines the script now prints for each stale cell.

## Open questions to verify before step 3

- Does a lane persist job report durability back so that the logical set can
  clear a dirty record? The logical set has a `DurabilityToken` for this. Whether
  the persist lane feeds it today was not checked.
- Which readers still need a resident answer for a key the logical set evicted?
  The graph's coverage tracking answers "is this key set complete" for range
  reads. The logical set has only a negative barrier cache.
- Whether the page and count-signal stale reads are a write-mode defect or a bar
  problem. See the baseline section.

## Review hazards

- This is a live write path with no test suite. A wrong lock order or a missed
  revision bump shows up as a lost or reordered write, not a compile error.
- As written, the bars read and update one Board field. They do not appear to
  exercise protein sibling folds or Skip Deletes, so step 3 needs a manual check
  of those two paths.
