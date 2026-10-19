# Warm set byte limit design

Status (2026-10-19): design for Tom's review. Nothing here is implemented.
The design was written from a read of the code at main `fadd60ab`. Nothing was run or measured.
Numbers marked "guess" have no measurement.

If Tom approves this design, it replaces the record-count limit of the warm set.
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

## Current state

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

`ProcessMemoryBudget` (`memory_budget/process_budget.rs`) charges `resident_graph_bytes` from the 2 GiB `LASTDB_RESIDENT_BYTES` reserve today.
The code comment says that this reserve stays only "while clean native atom retention has a record cap rather than a global byte cap".
After this change, that input becomes `warm_limit_bytes`.

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
3. **Enforce bytes for reads.** Switch `make_room` to bytes. Every read enters, and the purge makes room. Remove the count limit.
   Proof: a fill above the limit on a synthetic home keeps `warm_used_bytes` at or under `warm_limit_bytes` when no call holds records, with zero read errors.
4. **Dirty share.** Add the dirty counter and the write check.
   Proof: a synthetic write burst keeps `warm_dirty_bytes` at or under its limit. Writes above the share finish on disk, and every write gets its ack.
5. **Process budget.** Replace the 2 GiB reserve input with `warm_limit_bytes`.
   Proof: the boot log shows the new projection.

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

## Decisions for Tom

- **D1. Byte limit in place of the record count.** This replaces decision `decision-2026-10-02-warm-set-exact-logical-key-budget` and the harness rule "the key budget is 10000 logical records". Recommendation: yes.
- **D2. Default limit.** Recommendation: 256 MiB (guess), changed after stage 2.
- **D3. The count limit.** Recommendation: remove it. The path charge covers the map growth that a count would protect. Alternative: keep `LASTDB_RESIDENT_KEY_CAP` as an optional lower limit, off by default.
- **D4. Dirty share.** Recommendation: 25% of the limit, above which a write goes to disk before its ack. Written values move from the deferred write allowance to the warm budget.
  Alternative: keep written values in the deferred write allowance, as today, and charge only read-admitted values to the warm budget. That keeps two pools for the same kind of object.
- **D5. A full warm set on a read.** DECIDED (Tom, 2026-10-19): every new read enters the warm set. The purge removes least recently used clean records to make room. If held and unflushed records leave no room, the record enters over the limit, and the purge corrects it after the calls end.
