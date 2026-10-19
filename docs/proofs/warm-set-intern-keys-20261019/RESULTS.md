# Warm set bookkeeping shrink: results (2026-10-19)

Design: `docs/lastdb-warm-set-byte-limit-design.md` ("Bookkeeping shrink result").
All data is synthetic. Each run uses a fresh throwaway node home under `$TMPDIR`. Each run stops its node and deletes its home.

## Change

1. Each slot key is stored once in a slot table. Paths, owner lists, and source links use a 32-bit slot id.
2. Each source key is stored once in a source table, in a packed form. Both sides of a source link hold only ids.
3. Paths use one shared `Arc<CorePath>` for the path map, the recency map, and the owner list.
4. The packed form stores each lowercase hex run of 16 or more characters as raw bytes. A delete-barrier key spells its molecule key in hex, so its packed form is about half the size.
5. The gauge constants match the new layout.

## Method

`warm_ab.py` runs each shape on two fresh nodes: the normal cap, and `LASTDB_RESIDENT_KEY_CAP=800`.
Each node writes 3,000 records, flushes, restarts with an empty warm set, and reads every record two times (`HashRangeKeys`, 256 keys per call).
The driver checks every value against the generated value. The growth is the difference between a sample before the reads and a sample after the reads.
The difference between the two caps is what the warm set keeps for 8,200 extra paths.
Allocator bytes come from `malloc_bytes_in_use` in the self-metrics log (`LASTDB_SELF_METRICS_SAMPLE_SECS=5`). Gauge bytes come from `native_disk.core_warm_bytes.used_bytes`.

Binaries: `lastdbd` dev builds with the shipped allocator (mimalloc).
Before: base commit `1dc87fd0` (the PR 224 head, the same code as `adbec20` on main), sha256 `2fbc7b72…`. After: this branch, sha256 `d3e8a8e2…`.

## Allocator growth per warm path

| Shape | Before | After | Change |
|---|---|---|---|
| small (3,000 × 200 B) | 11,448 B | 4,176 B | −64% |
| medium (3,000 × 8 KB) | 14,213 B | 6,924 B | −51% |

The medium shape includes about 2.7 KB of atom body for each path. The atom body did not change.

Confirmation on the rebased branch (main `43f9bd07` plus this change, sha256 `5f7b5d21…`, `ab-after-rebased.json`):

| Shape | Allocator growth per path | Gauge ratio |
|---|---|---|
| small | 3,476 B | 1.038 |
| medium | 6,215 B | 1.067 |

The rebased numbers are lower because main `43f9bd07` (#228) uses exact molecule ids, so some source keys are shorter. The before and after rows above use the same base, so they are the fair comparison for this change.

## Gauge ratio (gauge growth ÷ allocator growth)

| Shape | Before | After |
|---|---|---|
| small | 1.073 | 1.040 |
| medium | 1.077 | 1.064 |

## Values

Every run read 6,000 records and every value matched (`values_checked` in `ab-*.json`). No call failed.
The dev build also runs a `debug_assert` in `SourceTable::intern`: it unpacks each new packed source key and compares it with the input key.

## Heap profile (9,000 warm paths, small shape)

`warm_profile.py` runs a system-allocator build with `MallocStackLogging=1` and `malloc_history -callTree -invert`.
`heap_attr.py` charges each live allocation to the first warm set or molecule frame.

| | Before | After |
|---|---|---|
| Traced live heap | 127 MB | 49.4 MB |
| Traced live allocations | 868,935 | 280,933 |

| Part (per path) | Before | After |
|---|---|---|
| Source links and source keys | 7.65 KB | 1.28 KB |
| Slot and source tables (entries, index, slot key strings) | 1.15 KB (key clones) | 0.80 KB |
| Path, path map, recency map | 2.45 KB | 0.64 KB |
| Molecule tip and key metadata | 0.74 KB | 0.74 KB |
| Owned copy of the tip (`CoreModelSlot::clean`) | 0.64 KB | 0.64 KB |
| Atom links | 0.51 KB | 0.18 KB |

The full lists are in `heap-before.txt` and `heap-after.txt`.

## What is left

The goal was about 1–2 KB for each path. The result is about 4.2 KB. These parts are left:

- Source keys: about 1.3 KB. Each slot links about 4.7 distinct keys, and a packed key is about 190 bytes. A shared prefix table, or source keys that the core can derive from the slot, can remove most of it.
- Molecule tips and the owned copy of the tip: about 1.4 KB. The owned copy of a clean read keeps the tip entry two times. A call that holds the slot keeps that copy, so a change to it changes hold behavior. It needs its own review.
- The path copy: about 0.6 KB. A path key of a slot id plus interned schema and field names can remove the string copies.
