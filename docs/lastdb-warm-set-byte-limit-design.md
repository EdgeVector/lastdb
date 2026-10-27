# Warm set byte limit design

Status (2026-10-20): stages 1–3 are implemented. Tom approved D1–D5.
The current implementation uses a byte limit and admits each read.
See [the current memory and disk contract](lastdb-rehydrate.md) for the implemented rules.

The original design below describes main `fadd60ab` before those changes. No measurement informed that original design.
Numbers marked "guess" have no measurement.

This design replaces the record-count limit of the warm set.
That limit comes from decision `decision-2026-10-02-warm-set-exact-logical-key-budget`
and the warm set rules in the harness instructions ("the key budget is 10000 logical records").
It also replaces the capacity rule in
[the logical warm set design](lastdb-logical-warm-set-design.md#capacity-and-purge).

## Answer

Limit the warm set by bytes, not by a record count.

- Each path pays a fixed cost. Each atom object pays its own calculated memory size.
- A shared atom pays one time. Its charge stays until the last holder drops it.
- The purge removes the least recently used clean paths until the new charge fits.
- A new read always enters the warm set. When the warm set is full, the purge removes the least recently used clean records to make room (Tom, 2026-10-19).
- Unflushed writes use at most a fixed share of the limit. Above that share, a write goes to disk before its ack, which is the current fallback.
- Status reports the charged bytes next to the measured process memory, so an error in the estimate is visible.

The charge is an estimate with a small, bounded error. It is rounded up, so the error is on the safe side.
A record count has no bound on its error: 10,000 records can use 10 MB or 2.5 GiB.

## State at design creation

| Part | Code | Limit today |
|---|---|---|
| Warm set paths, tips, atom bodies | `resident/core_set.rs`, `core_set/admission.rs`, `core_set/lifecycle.rs` | 10,000 paths (`RESIDENT_KEY_CAP`, `resident/key_cap.rs`). No byte limit on atom bodies. |
| Definition cache | `resident/core_set/memory.rs` | 10,000 entries and 16 MiB |
| Complete keys | `resident/core_keys.rs`, `storage/disk_pack/schema_keys.rs` | 128 MiB (`LASTDB_SCHEMA_KEY_BYTES`) |
| Write model metadata | `resident/core_set/model.rs` | 16 MiB |
| Written values | `resident/core_set/model.rs` and `model/prepare.rs` (`CoreModelSlot.owner`) | A `DeferReservation` from the deferred write allowance (`memory_budget/deferred_gauge.rs`), derived from process headroom, 64–128 MiB |
| Process reserve for atom retention | `memory_budget/process_budget.rs` (`resident_graph_bytes` input) | 2 GiB (`LASTDB_RESIDENT_BYTES`) |

Facts this design depends on:

1. `CorePath` is the record identity: scope, schema, field, molecule, hash, range.
2. Several paths can own one slot (`slot_owners`). Several slots can share one atom object (`SharedAtom`, an `Arc`).
3. Paths are reserved through `CoreCallHolds::hold` → `reserve_paths`. Some read routes reserve before the disk fetch (`logical_paths/core_filter.rs`).
   Field admission reserves the path and admits the atom in one lock section after the fetch (`admit_field_path` → `CoreWarmSet::admit`).
   In both cases the atom size is known only at `admit`.
4. A write reserves its paths in `logical_paths/protection.rs`. If the core refuses, the write finishes on disk before success.
5. `make_room` removes least recently used paths that are not held and not dirty. If it cannot find enough, it refuses (`CoreCapacity`).
6. A read can keep an atom after the purge removes its path. `CoreCallHolds` keeps the `CoreModelSlot` objects (`retained_values`), and a selection returns an `Arc<Atom>` (`CoreWarmSelection`).
   A call does not keep the `SharedAtom` itself.
7. The atom content is a decoded JSON tree (`serde_json::Value`, `atom/atom_def.rs`). The `bytes` value on a `CoreWarmSlot` is the encoded JSON length (`logical_paths/body.rs`), not the memory size.
   Without `preserve_order`, a JSON object is a `BTreeMap<String, Value>`.
8. `CoreKeyBudget` (`resident/core_keys/budget.rs`) already gives byte reservations that release on drop.
9. The global allocator is mimalloc (`lastdb_node/src/allocator.rs`).
10. A written value keeps its `DeferReservation` through `CoreModelSlot.owner`. The owner stays while the slot is current, also after the flush, until an overwrite or a purge drops the slot.
    So today a written value that stays warm keeps a charge in the deferred write allowance, and a value admitted by a read has no byte charge.

## Goals

- One byte limit for the warm set: paths, tips, metadata, and atom objects.
- A charge that is accurate to a small, bounded error and rounded up.
- Every read enters the warm set. No read error because the warm set is full.
- No change to the write ack rule, the flush delay, or durability.
- Status that shows the charge, the measured memory, and the difference.

## Non-goals

- The definition cache, complete keys, and write metadata keep their own limits in this design. A later step can put them under one total (see "Later step").
- No change to file blobs. File contents stay outside the warm set.
- No change to the read capture rules, the publication lock order, or the source checks.

## The charge

The charge for the warm set has two kinds of items.

### Path charge (fixed cost)

One charge for each `CorePath` in `paths`:

```
path_charge = PATH_BASE
            + 3 × capacity of scope, schema, field, molecule, hash, range
                (copies in `paths`, `slot_owners`, and `order`)
            + slot key copies (scope, molecule, hash, range) in `molecules`,
              `owned_slots`, and `slot_atoms`, charged to the first owner of the slot
            + tip entry and key metadata sizes (when the path knows a tip)
```

`PATH_BASE` covers the map entries, `PathState`, and the `order` entry. Start value (guess): 512 bytes.
The string sizes come from `String::capacity()`, as `allocation_bound` in `core_set/memory.rs` does today.

### Atom charge (calculated memory size)

One charge for each `SharedAtom`:

```
atom_charge = ATOM_BASE
            + capacity of uuid, source_schema_name, source_file_name
            + metadata map: for each entry, MAP_ENTRY + key capacity + value capacity
            + json_heap_bytes(content)
```

`json_heap_bytes` walks the JSON tree one time, at admission:

| JSON node | Charge |
|---|---|
| null, bool, number | `VALUE_SIZE` (the size of `serde_json::Value`, 32 bytes on 64-bit targets) |
| string | `VALUE_SIZE` + string capacity |
| array | `VALUE_SIZE` + vector capacity × `VALUE_SIZE` + children |
| object | `VALUE_SIZE` + for each entry: `MAP_ENTRY` + key capacity + child |

`MAP_ENTRY` covers the B-tree node space for one entry. Start value (guess): 64 bytes.
The walk has the same cost order as the encoded length that the code calculates today. It replaces that calculation for the charge.

### Rounding

- Round each atom charge and each path charge up to a multiple of 16 bytes.
- Multiply the total by `SAFETY_FACTOR`. Start value (guess): 1.10. This covers allocator size classes and B-tree node slack.
- After calibration, the constants change. The rule "round up" stays.

### Why the error is bounded

- The string, vector, and map sizes are calculated, not guessed.
- Only the per-object constants are estimates. Each one is a fixed number of bytes per object.
- So the total error is at most (number of objects × constant error) plus the allocator rounding. It does not grow with the size of the values.

## The budget

- One `CoreWarmBudget` for each native runtime, with the same reservation type as `CoreKeyBudget`: atomic `used` and `peak`, and a reservation that releases its bytes on drop.
- The limit comes from `LASTDB_CORE_WARM_BYTES`. Start value (guess): 256 MiB. The daemon refuses to start on a value that does not parse, or on zero.
- `LASTDB_RESIDENT_KEY_CAP` and `RESIDENT_KEY_CAP` are removed (see decision D3).

### Where the charges live

- **Path charge:** `PathState` owns one reservation. `drop_path` drops it.
- **Atom charge:** one `Arc<WarmCharge>` for each atom object. `SharedAtom`, `CoreWarmSlot`, and `CoreWarmSelection` each carry a clone of it, next to their `Arc<Atom>`.
  The reservation drops when the last clone drops. A running call keeps a `CoreModelSlot` or a selection, so it keeps the charge too.
  A value that a call still uses stays charged. That is correct, because the memory is still in use.
  The charge must not live on `SharedAtom` alone: a call does not keep `SharedAtom`, so the charge would drop while the call still uses the atom.
- **Dirty marker:** a dirty path carries no extra charge. Its path and atom charges are already in the budget. The dirty share counts these charges (see "Unflushed writes").

## Admission

Admission has two steps, because the atom size is not known before the fetch.

### Step 1: reserve the path (before the fetch)

`reserve_paths` reserves the path charge for each new path, through `make_room`.
This step needs only the key strings, so the size is known.

### Step 2: admit the atom (after the fetch)

`admit` calculates the atom charge and reserves it, through `make_room`.

- If the atom object is already in memory (same UUID and equal value, `link_atom`), no new charge is made.
- Otherwise the purge removes least recently used clean records until the charge fits. Then the atom enters.
- The atom always enters. There is no tip-only fallback and no read that skips the warm set.

No large-atom rule is needed. An atom body is at most 1 MiB (`ABSOLUTE_MAX_ATOM_CONTENT_BYTES`), which is a small part of the limit.

### When the purge cannot make enough room

The purge can remove only records that no call holds and that are flushed.
The dirty share keeps unflushed writes at or under 25% of the limit. So the purge fails only when running calls hold most of the rest.

In that case the new record still enters, and the charge goes above the limit:

- The amount over the limit is at most the bytes that running calls hold. Those bytes are in use, so the charge stays honest.
- Each later admission runs the purge first. When the calls end, their records become clean and the purge can remove them, so the charge returns under the limit.
- Status counts each admission over the limit and the highest charge (`warm_over_limit_admissions`, `warm_peak_bytes`).

This replaces the current error "all core warm path slots are held or unflushed".

## Purge

`make_room(needed_bytes)` replaces the count calculation:

1. Try the reservation. If it succeeds, stop.
2. Take the least recently used path from `order` that is not in the request, has `holds == 0`, and is not dirty.
3. Drop the path. This drops its path reservation, and it can drop the last `Arc` of its atom.
4. Go to step 1. Stop when the charge fits or when no candidate remains. If no candidate remains, the record enters over the limit (see "When the purge cannot make enough room").

The purge measures what it freed from the budget itself, not from a nominal size.
This handles shared atoms correctly: a path whose atom another path still uses frees only its path charge.
The definition cache uses the same loop today (`CoreMemory::prepare`).

The purge runs only on admission, as today. No timer runs a purge.

## Unflushed writes

Unflushed writes cannot be purged. Without a rule, a burst of writes could push out every clean record.

Today a written value is charged to the deferred write allowance (`DeferReservation`), and that charge stays while the value stays warm (fact 10).
If the warm budget also charged the same value, the bytes would count two times. So the charge for a written value moves:

1. **Before the ack:** the write reserves its path and atom charges in the warm budget, inside the dirty share. The dirty share is `DIRTY_FRACTION` × limit. Start value (guess): 25%.
   The check is in `logical_paths/protection.rs`, before publication.
2. **If the dirty share is full:** the core refuses. The write finishes on disk before success.
   This is the current fallback for a memory refusal (`docs/lastdb-rehydrate.md`, "A memory refusal also forces disk completion before success").
3. **After the flush:** `complete_flush` moves the charges from `dirty_bytes` to clean. The bytes stay in the warm budget, and the purge can remove them.
4. **The deferred write allowance** stops charging the in-memory value objects. It keeps its other jobs: the count cap for in-flight work and the footprint latch.
   Queued disk bytes are reserved in the write queue (`disk_pack/queue.rs`, `reserve_capacity`), not in either budget.

Result: one budget charges every value in the warm set, from a read or from a write. The dirty share keeps writes from taking all of it.

## Status

Add these fields to the native core status (`storage/disk_pack/view.rs`, `CoreWarmStats`):

| Field | Meaning |
|---|---|
| `warm_limit_bytes` | The configured limit |
| `warm_used_bytes` | The current charge |
| `warm_peak_bytes` | The highest charge since start |
| `warm_held_bytes` | The charge of atoms that only a running call keeps, outside any path |
| `warm_dirty_bytes` | The charge of unflushed writes |
| `warm_dirty_limit_bytes` | The dirty share limit |
| `warm_paths` | The number of paths (the old count, kept for reading) |
| `warm_over_limit_admissions` | Admissions that entered with the charge above the limit |
| `warm_purged_bytes` | Bytes that the purge freed since start |

The self-metrics sampler already records the process footprint. Its log line adds `warm_used_bytes` next to the footprint.
The ratio of the two is the check on the estimate.

`ProcessMemoryBudget` (`memory_budget/process_budget.rs`) charged `resident_graph_bytes` from the 2 GiB `LASTDB_RESIDENT_BYTES` reserve.
The code comment said that this reserve stays only "while clean native atom retention has a record cap rather than a global byte cap".
Done in PR 570 (2026-10-27): the projection charges only the warm set byte limit, and `LASTDB_RESIDENT_BYTES` is removed.

## Calibration

CAUTION: Do not copy `~/.lastdb` and do not run a node on a copy of it (Situation `no-copies-of-primary-database-20261012`). Calibrate on synthetic data and on read-only live status only.

1. Build a synthetic home with three record shapes: small (about 200 bytes of JSON in about 10 fields), medium (about 8 KB), and large (about 200 KB).
2. For each shape, fill the warm set to its limit. Read `warm_used_bytes` and the process footprint before and after the fill.
3. Set each constant so that the charge is at or above the measured growth, within 10%.
4. On the primary, after the change ships, compare `warm_used_bytes` with the footprint growth in the self-metrics log. Look only at the live signal; do not copy data.

This is a measurement, not a test. No test suite is written.

## Code changes

| File | Change |
|---|---|
| `resident/key_cap.rs` | Replace the record cap with `CoreWarmBudget`, `LASTDB_CORE_WARM_BYTES`, and the boot parse |
| `resident/core_set.rs` | `PathState` owns the path reservation. `SharedAtom`, `CoreWarmSlot`, and `CoreWarmSelection` carry `Arc<WarmCharge>`. New counters for held, dirty, over-limit admissions, purged bytes |
| `resident/core_set/model.rs`, `model/prepare.rs` | Written values take their charge from the dirty share, not from `DeferReservation` |
| `resident/core_set/charge.rs` (new) | `path_charge`, `atom_charge`, `json_heap_bytes`, the constants, the rounding |
| `resident/core_set/lifecycle.rs` | `make_room` by bytes. `complete_flush` moves dirty charges |
| `resident/core_set/admission.rs` | Two-step admission. Purge first, then always admit. Over-limit admission when no candidate remains |
| `storage/disk_pack/read/core_holds.rs` | A path reservation no longer fails on capacity |
| `storage/disk_pack/logical_paths/read.rs` | Calculate the atom charge before the core lock. Pass it to `admit` |
| `storage/disk_pack/logical_paths/protection.rs` | Check the dirty share before publication |
| `storage/disk_pack/view.rs` and `lastdb_node/src/ops/self_metrics/` | New status fields and the log line |
| `memory_budget/process_budget.rs` | Use `warm_limit_bytes` in place of the 2 GiB reserve input |

The charge on `CoreWarmSlot.bytes` (encoded length) stays for the read-call accounting in `account_memory_record`. It is not the warm set charge.

## Stages

Each stage is one PR. The proof for each stage is the live signal named here, not a test.

1. **Gauge only.** Calculate the charges and report the status fields. Keep the count limit as the enforced rule.
   Proof: status on a synthetic home shows `warm_used_bytes`, and the ratio to the footprint growth is inside 0.9 to 1.3.
2. **Calibrate.** Set the constants from the synthetic fills (see "Calibration").
   Proof: the ratio is inside 1.0 to 1.1 for the three shapes.
3. **Enforce bytes for reads.** DONE (2026-10-19, see "Stage 3 result"). Switch `make_room` to bytes. Every read enters, and the purge makes room. Remove the count limit.
   Proof: a fill above the limit on a synthetic home keeps `warm_used_bytes` at or under `warm_limit_bytes` when no call holds records, with zero read errors.
4. **Dirty share.** Add the dirty counter and the write check.
   Proof: a synthetic write burst keeps `warm_dirty_bytes` at or under its limit. Writes above the share finish on disk, and every write gets its ack.
5. **Process budget.** Replace the 2 GiB reserve input with `warm_limit_bytes`.
   Proof: the boot log shows the new projection.

## Stage 1 result (2026-10-19)

The process footprint is not a usable comparison. It grew 170–270 MB in every run from activity outside the warm set.
The proof uses the allocator in-use bytes (`malloc_bytes_in_use`) in an A/B pair instead.
Each shape runs once with the normal cap and once with `LASTDB_RESIDENT_KEY_CAP=800`. The difference is what the warm set keeps.

The first gauge charged only paths and atoms. It read 0.14–0.33 of the real growth.
A heap profile (system allocator build, `malloc_history`, 9,000 warm paths) showed that the gap is bookkeeping, not data:

| Part | Per path |
|---|---|
| Source links (the invalidation index) | about 6.7 KB |
| Copies of the slot key strings across about 6 maps | about 1.8 KB |
| Tip and molecule entries, recency map share | about 1.8 KB |
| The path table | about 1.2 KB |
| Atom body (small records) | about 0.3 KB |

The gauge now charges source links (`link_charge`) and slot entries (`slot_charge`). Result for 8,200 extra paths on a synthetic throwaway node:

| Shape | Gauge growth | Allocator growth | Ratio |
|---|---|---|---|
| small (3,000 × 200 B) | 93.5 MB | 87.7 MB | 1.066 |
| medium (3,000 × 8 KB) | 117.2 MB | 109.3 MB | 1.073 |

Next step outside the stages: shrink the bookkeeping. Store each slot key and source key once, refer to them by small ids, and use small lists in place of many small hash sets.
At 10,000 paths the bookkeeping is about 100 MB, while the small-record data is about 1 MB.

## Bookkeeping shrink result (2026-10-19)

The change (proof: [`docs/proofs/warm-set-intern-keys-20261019/RESULTS.md`](https://github.com/EdgeVector/lastdb/blob/0b0a79d4c10949e2f0570e82d9bc4fdb9513a285/docs/proofs/warm-set-intern-keys-20261019/RESULTS.md)):

- The slot table stores each slot key once. Paths, owner lists, and source links use a 32-bit slot id.
- The source table stores each source key once, in a packed form. Each lowercase hex run of 16 or more characters becomes raw bytes. Both sides of a source link hold only ids.
- One `Arc<CorePath>` serves the path map, the recency map, and the owner list.
- The gauge constants match the new layout (`PATH_BASE`, `SLOT_BASE`, `LINK_BASE`, `SOURCE_BASE`, `SLOT_KEY_COPIES`).

Result for 8,200 extra paths, with the same A/B method:

| Shape | Allocator growth per path, before | After | Gauge ratio, after |
|---|---|---|---|
| small (3,000 × 200 B) | 11.4 KB | 4.2 KB | 1.040 |
| medium (3,000 × 8 KB) | 14.2 KB | 6.9 KB | 1.064 |

The goal was about 1–2 KB for each path. About 4.2 KB is left:

| Part | Per path |
|---|---|
| Packed source keys and their index | about 1.3 KB |
| Molecule tip and key metadata | about 0.74 KB |
| The owned copy of the tip of a clean read | about 0.64 KB |
| The path and its strings | about 0.64 KB |
| Slot and source table entries | about 0.8 KB |

Each part needs its own change. Source keys need a shared prefix table or keys that the core derives from the slot. The owned copy of the tip is what a call that holds the slot keeps, so a change to it changes hold behavior.

## Second bookkeeping shrink (2026-10-19)

The change (proof: [`docs/proofs/warm-set-shrink-path-20261019/RESULTS.md`](https://github.com/EdgeVector/lastdb/blob/0b0a79d4c10949e2f0570e82d9bc4fdb9513a285/docs/proofs/warm-set-shrink-path-20261019/RESULTS.md)):

- A clean read keeps no owned copy of the tip. A call that holds the path gets the slot atom, so the call keeps the same atom alive as before. A prepared value keeps its owned value and its reservation owner.
- A warm path is its slot id plus interned schema and field name ids. The path index, the recency map, and the slot owner lists hold a 32-bit path id.
- The slot index and the source index hold only ids. A lookup by borrowed strings makes no temporary key.
- A delete-barrier key is stored as its packed `mk:` key and shares that allocation with the `mk:` source.
- The gauge constants match the new layout.

Result for 8,800 extra paths (normal cap against a cap of 1,200; 4 molecules per record on current main):

| Shape | Allocator growth per path, before | After | Gauge ratio, after |
|---|---|---|---|
| small (3,000 × 200 B) | 3.5 KB | 2.2 KB | 1.030 |
| medium (3,000 × 8 KB) | 5.4 KB | 4.0 KB | 1.067 |

The warm set core part falls from about 2.9 KB to about 1.25 KB for each path. The molecule tip and key metadata (about 0.74 KB) are outside the warm set code and did not change.

## Stage 3 result (2026-10-19)

What changed:

- `make_room` purges least recently used clean, unheld paths until the incoming path charges fit `LASTDB_CORE_WARM_BYTES`. It never refuses. If only held or unflushed paths remain, the new paths enter over the limit and `over_limit_admissions` counts it.
- `admit` trims back to the limit after the atom charge, because the atom size is known only at admission.
- `complete_flush` also trims. Flushed paths become purgeable then, and without this trim a quiet node stayed over the limit until its next admission (measured: 17.6 MiB against an 8 MiB limit after an update pass).
- Every check starts with an O(1) comparison, so a warm set under its limit pays no extra allocation.
- The 10,000-path count is removed (D3). `LASTDB_RESIDENT_KEY_CAP` is ignored with a warning. The status fields `warm_record_limit` and `resident_key_budget` stay on the wire for installed clients: the first reads 0, the second is "not served".
- New status fields: `purged_bytes` and `over_limit_admissions`, and a "Warm set bytes" line in `lastdb status`.
- Two stage 1 review fixes that did not reach main with PR 224 are included: the status line, and a peak recorded only after a sweep.

Proof on synthetic throwaway nodes, with an update check that rewrites every 7th record twice and reads every record back after each acknowledgement:

| Case | Charge after the reads and after the flush | Reads | Reads after updates |
|---|---|---|---|
| Small records, 8 MiB limit | 7.9 MiB in every round and after the flush | 9,000 correct | 6,000 correct |
| 16 parallel clients, 8 MiB limit | 7.9 MiB | 9,000 correct | 6,000 correct |
| Medium records, 16 MiB limit | 15.9 MiB | 9,000 correct | 6,000 correct |
| Small records, default 256 MiB limit | 18.5 MiB (everything fits) | 9,000 correct | 6,000 correct |

The 16-client case at 8 MiB failed before this change with "all core warm path slots are held or unflushed". Latency at the default limit is unchanged (p50 3.3–4.0 ms against 3.4–3.5 ms on main, the same warm round times).

Known limit: a read that admits a record while an unflushed write exists marks that record unflushed until the flush, so a read burst during writes can go over the limit for up to the flush delay (500 ms). Stage 4 (the dirty share) bounds this.

## Later step

The same reservation type can carry the definition cache (16 MiB), the complete keys (128 MiB), and the write metadata (16 MiB).
A later design can put all four under one total, with a share for each. That gives one number for "memory used by data".
This design does not do that, so that each step stays small.

## Risks

| Risk | Effect | Control |
|---|---|---|
| The constants are wrong | The charge is too low, and memory grows past the limit | Round up, `SAFETY_FACTOR`, status ratio, calibration stage before enforcement |
| The walk costs time on large atoms | Slower admission of large values | One walk for each admission, the same order as the encoded length today. An atom body is at most 1 MiB |
| A small limit | More disk reads, lower hit rate | Report the hit rate with the byte fields. Change the limit from measurement |
| Held records go above the limit | The charge can pass the limit while long calls run | The overshoot is at most the bytes that calls hold. The purge brings the charge back when the calls end. `warm_held_bytes` and `warm_over_limit_admissions` show it |
| One large call | One call can read up to 256 MiB (`ReadBudget`). With a 256 MiB limit, one call can push out every other record | This is the least recently used rule working as designed. Set the limit above the per-call read budget, or lower that budget |
| A write burst | Writes above the share go to disk first and have a slower ack | This is the current fallback. `warm_dirty_bytes` shows it |
| One lock | The walk runs under the core lock if it is placed in `admit` | Calculate the charge before the lock is taken. `admit` only reserves it |

## Decisions (Tom, 2026-10-19)

- **D1. Byte limit in place of the record count.** DECIDED: yes. This replaces decision `decision-2026-10-02-warm-set-exact-logical-key-budget` and the harness rule "the key budget is 10000 logical records".
- **D2. Default limit.** DECIDED: 256 MiB to start (guess), changed after stage 2.
- **D3. The count limit.** DECIDED: remove it in stage 3. The path charge covers the map growth that a count would protect.
- **D4. Dirty share.** DECIDED: 25% of the limit, above which a write goes to disk before its ack. Written values move from the deferred write allowance to the warm budget.
- **D5. A full warm set on a read.** DECIDED (Tom, 2026-10-19): every new read enters the warm set. The purge removes least recently used clean records to make room. If held and unflushed records leave no room, the record enters over the limit, and the purge corrects it after the calls end.
