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

## Open questions to verify before step 3

- Does a lane persist job report durability back so that the logical set can
  clear a dirty record? The logical set has a `DurabilityToken` for this. Whether
  the persist lane feeds it today was not checked.
- Which readers still need a resident answer for a key the logical set evicted?
  The graph's coverage tracking answers "is this key set complete" for range
  reads. The logical set has only a negative barrier cache.
- What the live node's `write` mode buys. Step 2 answers this. The
  acknowledgment rule above holds whatever the answer is.

## Review hazards

- This is a live write path with no test suite. A wrong lock order or a missed
  revision bump shows up as a lost or reordered write, not a compile error.
- As written, the bars read and update one Board field. They do not appear to
  exercise protein sibling folds or Skip Deletes, so step 3 needs a manual check
  of those two paths.
