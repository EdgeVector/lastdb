# Current core memory and historical convergence record

Current summary, 2026-10-26: disk format v2 is the sole native engine.
The core model owns current schemas, molecules, keys, tips, confirmed absence, and atom values.
The recency list selects logical records for purge without a second set of values.
The old serving graph, logical cache, mode controls, and alternate write branch are removed.

Every native write waits for the complete disk commit and clean core publication before the ack.
The first accepted request starts the worker immediately. Callers ahead of that worker in the FIFO can share one commit.
The private disk-output queue retains bytes and resource owners until completion; ordinary reads cannot inspect it.
Captured core values keep their memory charges until their calls release them.
There is no 500 ms collection timer. Legacy Cloud Sync is removed.

Use the [current native summary](lastdb-disk-native-node.md) and [memory and disk routes](lastdb-rehydrate.md) for current responsibilities.
The [October work plan](lastdb-logical-warm-set-plan.md) records the earlier implementation sequence.
The [cutover results](https://github.com/EdgeVector/lastdb/blob/0b0a79d4c10949e2f0570e82d9bc4fdb9513a285/docs/proofs/core-clarity-20261018/RESULTS.md) record fresh-data API results and their exact source identities.
The [earlier V6 benchmark](https://github.com/EdgeVector/lastdb/blob/0b0a79d4c10949e2f0570e82d9bc4fdb9513a285/docs/proofs/core-owned-writes-20261018/RESULTS.md) measures its earlier source, rather than this final cleanup.
No primary operation forms part of this cutover proof.

## Historical plan and receipts, 2026-10-12 to 2026-10-14

CAUTION: All remaining text records an older design and its historical measurements.
The named modes, cache classes, bar scripts, and incomplete steps do not describe current operation.
The old bar scripts are retired. Do not execute this historical plan or use it to change the primary.
The dated results and links remain as evidence of that earlier work.

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
  `scripts/lastdbd/read-after-ack-bar.sh`. A cloud sandbox cannot run them.
- The bars never copy `~/.lastdb` (Tom, 2026-10-12: the copy costs disk the
  machine does not have). Each bar boots `lastdbd` on a fresh empty home under
  `/private/tmp` and loads synthetic data: the schema
  `validation/MiniFreshInstall`, HashRange(bucket, id). Numbers measured on
  synthetic data are not comparable with the owner run on real data below. The
  baseline for steps 3 to 6 is the synthetic baseline in "Step 2b findings".

## Today

| Cache | Where | Bound | Mode gated |
|---|---|---|---|
| `LogicalResidentSet` | store layer, [`storage/laststore/logical_path/`](https://github.com/EdgeVector/lastdb/tree/011b29f3a74eac3eb3a977678630c40345157ed4/fold_db/crates/core/src/storage/laststore/logical_path) (removed) | 10000 records (`RESIDENT_KEY_CAP`), LRU, skips held and dirty records | no, always on |
| `ResidentGraph` (removed) | `DbOperations`, one per database | byte budget (`LASTDB_RESIDENT_BYTES`, removed) | resident-first reads and acknowledge-on-apply depend on the mode; the graph itself is built and written in every mode |
| Hash-group warm set (removed with disk format v1) | store layer, non-logical collections | `LASTDB_HASH_GROUP_WARM_BYTES` (no longer read) | no |

The hash-group warm set was a storage-layer page cache for indexes and metadata.
Disk format v2 has no hash groups, so it no longer exists.

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
   `write` on a fresh empty home with synthetic data, and keep the numbers.
3. Teach the logical set the missing behavior: range coverage, the Skip-Delete
   overlay, and acked-but-unflushed tips that survive purge until the persist
   lane reports them durable. Keep the graph in place and verify the logical set
   gives identical answers.
4. Switch reads from the graph to the logical set. This removes the read mode.
5. Move the slot revisions and the persist turn to the mutation path as plain
   counters, now that no cache shares their lock.
6. Delete the graph's maps, ledger and the remaining env vars.

## Owner baseline on a copy of real data (step 2), 2026-10-12

This section is history. The bars no longer copy the primary's home, and the
read-after-ack cells below were wrong. See "Step 2b findings". Measured by the
owner on a copy-on-write copy of the real data, with a release
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

Read-after-ack, 20 reps per cell, run by the owner on a copy of the real data
with a script copy that took `--mode` (release `lastdbd`, 817b7d55). Both modes
returned the same matrix:

| Cell | `off` | `write` |
|---|---|---|
| create by point-get | green | green |
| update by point-get | green | green |
| create by page | stale 20/20 | stale 20/20 |
| update by page | stale 20/20 | stale 20/20 |
| create by count-signal | stale 20/20 | stale 20/20 |

`persist_enqueued` went 0 to 282 in `off` (window OPEN) and stayed 0 in `write`
(NOT_OPEN). `deferred_persist_failed` stayed 0 in both. No delete cells exist.

Interpretation:

- The stale page and count cells are not a resident-mode defect. In `off` the
  acknowledgment waits for the persist lane, so the write is durable before the
  read, and the cells are still stale 20/20. Either the page query or the bar is
  wrong, or page reads have a defect that is independent of this work. The
  count-signal cell only checks the response shape and cannot detect staleness.
  None of the three cells can gate a step until the cause is known.
- Point-get read-after-ack is green in both modes. That is the one proven
  invariant, and every step must keep it.
- `write` mode did not mark anything dirty in this run, and the cause is
  unknown. In `off`, 100 mutations produced 282 new dirty keys, about 2.8 per
  mutation, because every mutation creates a new atom. A deferred path must
  therefore raise the counter. A flat 0 means one of: the mutations took the
  durable-wait branch (`LASTDB_MUTATION_SYNC_FLUSH`, or `force_durable`), they
  took the write-through branch (defer window disabled, or
  `should_write_through`), or the status snapshot was not refreshed. The bar
  mirrors the live node's `LASTDB_*` environment, so a flag in the live plist
  would explain it.
- The latency bar still shows `write` at 46 ms against 641 ms. So the 14 times
  gain may not come from deferral at all. What `write` mode actually does on the
  live configuration must be established before step 3, because the design
  assumes acknowledge-before-durable is what runs.

## Step 2b findings (2026-10-12)

Measured with a release `lastdbd` built from `main` at f86da329, on fresh
empty homes with synthetic data (rule above). The live LaunchAgent environment
is mirrored. `LASTDB_RESIDENT_MODE=write` is set in the plist. The plist does
not set `LASTDB_MUTATION_SYNC_FLUSH` or `LASTDB_DEFER_WRITE_THROUGH_BYTES`.

### Why persist_enqueued read 0 in write mode

It was a measurement error, not a missing deferral.

- `/api/status` serves the resident counters from the self-metrics sampler.
  The sampler publishes once per `LASTDB_SELF_METRICS_SAMPLE_SECS` (default 60).
  A write-mode bar run ends in a few seconds, so the counters had not moved. An
  `off` run lasts over a minute, so they had.
- With the sampler set to 1 s and one wait after the run, write mode reports
  `persist_enqueued` 2123 to 2159 and `deferred_persist_failed` 0 for 20 reps
  of 8 cells. `off` reports 3120 for the same work. Write mode counts fewer
  because it re-dirties a key that is already dirty without counting it again.
- The bar now sets the sampler to 1 s and waits for a fresh sample.

One env change on a write-mode node, six updates each:

| Change | Ack (resident_stages_us.ack) | Defers |
|---|---|---|
| none | 6 to 9 ms (first call 144 ms) | yes |
| `LASTDB_MUTATION_SYNC_FLUSH=1` | 580 to 670 ms | no |
| `LASTDB_DEFER_WRITE_THROUGH_BYTES=1` | 440 to 600 ms, `deferred_write_throughs` 6 | no |

Code path: `reserve_persist_capacity` sets `acks_resident` only for
`write` mode with no storage prefix and no durable request. Then it takes
`write_through` when the defer window is disabled or the batch is at least
`LASTDB_DEFER_WRITE_THROUGH_BYTES`. With neither, it takes a defer reservation
and `wait_for_persist_lane` is false, so the ack returns after the resident
apply. The decision for this work is therefore: write mode defers on the live
configuration, and the sequence goes on.

### What produces the write p50

An acknowledged write in `write` mode returns after the resident apply, 6 to 9
ms in the node. The bar adds the curl and socket cost (the status floor is 12
ms), so the bar reads about 38 ms. In `off` and `read` mode the ack waits for
the persist lane envelope to be durable, which costs 440 to 670 ms in the node,
so the bar reads about 300 ms. The 14 times gain from `write` mode is the
deferral itself.

### Why the page and count cells were stale in off mode

The bar was wrong, not the product. A `Page` filter on a product schema is an
unfiltered schema scan, and the node refuses it with
`full_schema_scan_not_allowed` (HTTP body `ok: false`). The bar tested the
error body for the new title, so every page cell failed in every mode. The
count cell sent `include_total_count`, which is not in the request grammar. A
keyed read also returns `total_count` as `null`.

The bar now uses keyed reads only: a `HashRangeKey` point read, a `HashKey`
range list on a HashRange schema, and `returned_count` of that range list
before and after for the count cells. Every rep uses a fresh hash key. Delete
cells exist: a `delete` mutation with the key in `key_value` and `{}` as the
fields.

### Synthetic baseline (median of 3 runs, release build of f86da329)

Latency bar, 200 reads and 100 writes per run, milliseconds. The status floor
is 12 ms in every run.

| Mode | Point p50 | Point p95 | Write p50 | Write p95 |
|---|---|---|---|---|
| off | 14 | 46 | 312 | 788 |
| read | 13 | 14 | 290 | 830 |
| write | 15 | 54 | 38 | 83 |

Read-after-ack, 20 reps per cell per run, 8 cells, 3 runs per mode:

| Mode | Stale cells |
|---|---|
| off | none, every cell GREEN in all 3 runs |
| read | none, every cell GREEN in all 3 runs |
| write | `create x point-get` 20/20 and `update x point-get` 20/20 in all 3 runs; the other 6 cells GREEN (fixed in step 2c) |

`deferred_persist_failed` stayed 0 in all 9 runs.

### Product defect found and fixed in step 2c: reads after an ack missed in write mode

In `write` mode a `HashRangeKey` point read (and `HashRangeKeys`,
`HashRangePrefix`, `HashRangeRange`) that followed a create or an update
acknowledgment returned no record. The same key showed up on the `HashKey`
range list, and the other filters became correct after about 300 ms. `off` and
`read` mode were correct at once. The old bar could not see this, because the
Board schema is a Hash schema. This broke the second fixed constraint (a read
after an acknowledgment sees the write) for HashRange reads in the mode that
runs on the primary.

Cause, found by logging the molecule keys in `collect_matches`:
`resident_overlay_for_read` (`schema/types/field/variant/read/refresh.rs`)
joins the resident dirty keys to the molecule that the read built. It converted
every dirty key to storage form (blind hash, OPE range). The durable load of a
HashRange field builds that molecule with the API hash for every filter, and
with the API range for a point or multi-get filter (the load remaps it) and the
storage range for any other filter. So a dirty key never matched the filter
until the persist lane made it durable and the overlay went away. `HashKey`
worked because it takes the resident fast path in `resolve_hash_key_fast`.

Fix (step 2c): the overlay now uses the same key form as the molecule it joins.
It does not change any other behavior.

## Answers to the open questions, and what step 3 needs (2026-10-12)

Read from the code at 4bcd264a, with no change made.

- Does a lane persist job report durability back so that the logical set can
  clear a dirty record? Not directly. The logical set clears a dirty record
  when the store flush seals its `DurabilityToken` (`apply_durable_through`,
  called after a store flush). The lane's job writes through the store, so a
  record becomes dirty in the logical set when the lane writes it and clean on
  the next seal. Between the acknowledgment and the lane write, the record is in
  the graph only: the logical set does not know it. That window is the real
  "acked but unflushed" gap, and it is what the graph's dirty set covers today.
- Which readers still need a resident answer? Thirty-five files outside
  `resident/` name the graph. The read side is `schema/types/field/variant/`
  (`read.rs`, `read/resolve.rs`, `read/refresh.rs`, `read/collect.rs`,
  `partition_window.rs`), `fold_db_core/query/` (`hash_range_query/count.rs`,
  `secondary.rs`, `query_executor.rs`), `record_molecule/` and `purge/`. The
  write side is `mutation_manager/` (the apply gate, `restore.rs`, the CAS
  state readers, the persist jobs, hard erasure and delete winners). Each read
  site has its own rule for when the graph's answer is authoritative (key-set
  completeness, partition and page coverage tickets, `molecule_has_dirty`).
- The graph holds API-form keys and the logical set holds raw store rows. The
  same record has two spellings, and step 2c was a bug at that seam.

What this means for steps 3 to 6:

- Step 3 as written adds three behaviors to the logical set (range coverage, the
  Skip-Delete overlay, and records that are acknowledged but not yet in the
  store). Nothing reads them until step 4. They cannot be verified by the two
  bars alone, because the bars do not exercise coverage tickets, protein sibling
  folds or Skip Deletes. A shadow comparison (the logical set answers every
  graph read in parallel and a counter in `/api/status` counts disagreements) is
  the proof that fits the no-tests rule, and it must be built first.
- Step 5 changes the optimistic-concurrency gate (`PreparedSlotRevision`,
  `PersistSlotRevision`, the persist turn). A mistake there loses or reorders a
  write, and none of the bars can see a reorder.
- The shorter route may be to make the ack follow a store write instead of a
  graph apply. The store already acknowledges before it flushes and tracks
  dirty records with a durability token. That removes the graph's deferred
  apply and the window above, but it changes what the lane batches and
  orders, and it is a durability design change. It needs Tom's decision.

Step 3 is not started. The work stops here with 2b and 2c merged, because the
rules do not decide between the doc's order and the store-ack route, and
building the doc's step 3 without the shadow comparison cannot be verified.

## Review hazards

- This is a live write path with no test suite. A wrong lock order or a missed
  revision bump shows up as a lost or reordered write, not a compile error.
- As written, the bars read and update one Board field. They do not appear to
  exercise protein sibling folds or Skip Deletes, so step 3 needs a manual check
  of those two paths.

## Status log

- 2026-10-12, step 1: removed the never-populated `rehydrate_flights` map (#100).
- 2026-10-12, step 2: owner baseline on a copy of real data (#105).
- 2026-10-12, step 2b: findings above; both bars run on synthetic data with a
  driver, `scripts/lastdbd/resident-bars-matrix.sh`. Merged-build proof: run the bars on the
  merged main build (see the rule on copies above); no primary upgrade yet.
- 2026-10-12, step 2c: fixed the dirty-overlay key form in
  `resident_overlay_for_read`, so a HashRange read after a write-mode ack sees
  the write. Matrix on the fixed build, median of 3: off point 13/15 write
  284/669 ms, read 13/17 and 275/519, write 13/17 and 24/35. All 8
  read-after-ack cells GREEN in all 3 modes, 3 runs each; `deferred_persist_failed`
  0. This is the baseline for steps 3 to 6. Merged-build proof: main at 4bcd264a, one
pass on a fresh synthetic home: off point 13/52 write 246/447, read 13/14 and
355/462, write 13/55 and 26/86 ms; all 24 cells GREEN; `deferred_persist_failed`
0. No primary upgrade was made.
- 2026-10-12, step 3: not started. See the assessment above.
- 2026-10-12, warm set design: `docs/lastdb-warm-set-design.md` written and open for
  Tom's decisions. No code change.
- 2026-10-13, designs: `docs/lastdb-blind-write-design.md` and
  `docs/lastdb-write-ahead-journal-design.md` (since deleted) written. Built since the warm set
  design: store fsync gauges (#160), fewer groups per write (#167), GC reaper
  re-arm and hardening (#169, #170), persist cut for the manual GC verb (#171).
  (The atom GC reaper and its verb were removed by the counter lifecycle,
  `docs/lastdb-atom-counter.md`.)
- 2026-10-14, sidecar flush rule: the key sidecar (`keys-v1.idx`) keeps its write
  aside and rename. The forced disk write before the rename now happens only for a
  group of half the cold-load cap or more (1 GiB by default). Proof on fresh
  synthetic homes in write mode: main at 2d0ffbf0 against the same commit plus the
  change, 3 interleaved rounds of 200 creates. The machine load average was 16 to
  45, so the times are noisy and the counts are exact. Sidecar fsyncs 1554, 1548,
  1551 became 0, 0, 0. Data fsyncs did not change (4445, 4412, 4423 against 4443,
  4477, 4421). Fsync time 105, 88, 84 s became 66, 56, 71 s. Time until the lane was
  empty 202, 130, 170 s became 119, 90, 145 s. Drill: 30 hashes, a kill -9 and a
  graceful stop, each followed by a restart and a range list of every hash. The id
  sets match the baseline exactly in both cases, the node still reads sidecars after
  the restart, and the logs have no panic or checksum line. Bars on the new binary,
  3 modes, 2 runs each: every read-after-ack cell GREEN, `deferred_persist_failed`
  0, write p50 30 ms. NOT proven: power loss. No drill can show it. Open points:
  whether APFS can persist the rename before the file bytes (a bad file fails the
  checksum and the node loads the group), and whether the old sidecar flush also
  made a new plain-mode group directory entry durable (plain mode does not sync new
  group directories). A group that is under the floor at its last sidecar write and
  later grows past it can lose that sidecar in a power cut.
