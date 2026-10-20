# Warm set path bookkeeping shrink: results (2026-10-19)

Design: `docs/lastdb-warm-set-byte-limit-design.md` ("Second bookkeeping shrink").
Previous step: `docs/proofs/warm-set-intern-keys-20261019/RESULTS.md` (PR 230).
All data is synthetic. Each run uses a fresh throwaway node home under `$TMPDIR`. Each run stops its node and deletes its home.

## Change

1. A clean read keeps no owned copy of the tip. The tip is in the molecule, and the atom is in the slot. A call that holds the path gets the slot atom (`Arc<Atom>`) from `retained_values`, so the call keeps the same atom alive as before. A prepared (dirty) value still keeps its owned value, its reservation owner, and its metadata charge.
2. A warm path is its slot id plus an interned schema name id and field name id (`path_table.rs`). The path index, the recency map, and the slot owner lists hold a 32-bit path id. The core rebuilds a `CorePath` only for `paths_for_sources`.
3. The slot index and the source index hold only ids (`hashbrown::HashTable`). A lookup by borrowed strings makes no temporary key.
4. A delete-barrier key (`rdel:v2:`) spells its `mk:` key in hex two times. The source table stores it as the packed `mk:` key and shares that allocation with the `mk:` source. The core uses this form only when `delete_barrier_key(mk)` gives the exact key again. A `debug_assert` checks the round trip of each new source.
5. The gauge constants match the new layout (`PATH_BASE` 112, `SLOT_BASE` 1170, `SOURCE_BASE` 72).

The invariants do not change: a slot exists exactly while it has an owner, a path exists only while its slot exists, the core removes the links of a slot before it frees the slot id, and the core checks the 64-link limit before it adds a link.

## Method

`warm_ab.py` (in this folder) is the PR 230 driver with one change: the low cap is a parameter (`--low-cap`, default 1,200).
On current main each record has 4 molecules. One read call of 256 keys holds 1,024 paths, so a cap of 800 refuses the call ("all core warm path slots are held or unflushed"). The baseline refused it in the same way. A cap of 1,200 holds one call.

Each shape runs on two fresh nodes: the normal cap (10,000 paths) and `LASTDB_RESIDENT_KEY_CAP=1200`.
Each node writes 3,000 records, flushes, restarts with an empty warm set, and reads every record two times (`HashRangeKeys`, 256 keys per call).
The driver checks every value against the generated value.
The difference between the two caps is the growth for 8,800 extra paths.
Allocator bytes come from `malloc_bytes_in_use` in the self-metrics log. Gauge bytes come from `native_disk.core_warm_bytes.used_bytes`.

Binaries: `lastdbd` dev builds with the shipped allocator (mimalloc).

| Binary | Code | sha256 |
|---|---|---|
| before | main `b585e66a` plus PR 230 rebased (`ade26fb6`) | `d21658c1…` |
| owned and sources | before plus changes 1, 3 (sources only), 4 | `19e5c29a…` |
| after | this branch (`7ffce026` code) | `02a2c636…` |

## Allocator growth per warm path

| Shape | Before | Owned copy and sources | After | Change |
|---|---|---|---|---|
| small (3,000 × 200 B) | 3,536 B | 2,594 B | 2,155 B | −39% |
| medium (3,000 × 8 KB) | 5,407 B | 4,498 B | 4,001 B | −26% |

Both shapes save about 1.4 KB for each path. The medium shape includes about 1.9 KB of atom body for each path. The atom body did not change.

## Gauge ratio (gauge growth ÷ allocator growth)

| Shape | Before | After, old constants | After, new constants |
|---|---|---|---|
| small | 1.025 | 1.459 | 1.030 |
| medium | 1.049 | 1.292 | 1.067 |

Files: `ab-before.*`, `ab-owned-and-sources.*`, `ab-after-precalibration.*` (the after code with the old constants), `ab-after.*`.

## Values

Every run read 6,000 records and every value matched (`values_checked` in `ab-*.json`). No call failed.

## Heap profile (10,000 warm paths, small shape)

`warm_profile.py` runs a system-allocator build (`--no-default-features --features sentry-telemetry`) with `MallocStackLogging=1` and `malloc_history -callTree -invert`.
`heap_attr.py` charges each live allocation to the first frame that matches `fold_db::resident::|fold_db::atom::molecule_hash_range`.
Binaries: before `1050680e…`, after `cb352114…` (the same code as the rows above).

| | Before | After |
|---|---|---|
| Traced live heap | 47.4 MB | 31.8 MB |
| Traced live allocations | 294,451 | 184,493 |

Live bytes by frame, 10,000 paths (MB; divide by 10 for bytes per path in KB):

| Frame | Before | After |
|---|---|---|
| `SourceTable::intern`, `shared_mk`, `IdSet::insert` (source keys and index) | 7.18 | 2.66 |
| `CoreModelSlot::clean` and the `admit` clone closure (owned copy of the tip) | 6.40 | 0 |
| `insert_path`, `PathTable::insert`, `touch` (path strings, path map, recency map) | 6.30 | 0.38 |
| `tables::slab_insert` (entry vectors of the slot, source, and path tables) | 3.84 | 5.30 |
| `SlotTable::insert` (slot index) | 1.22 | 0.10 |
| `CorePath::slot_key`, `admit_tip`, `link_atom`, `track_atom` (unchanged parts) | 4.11 | 4.11 |
| Warm set core total | 29.05 | 12.55 |
| Molecule tip and key metadata (`molecule_hash_range`) | 7.30 | 7.38 |

The core total falls from about 2.9 KB to about 1.25 KB for each path.

The full lists are in `heap-before.txt` and `heap-after.txt`. The warm set status at the time of each profile is in `heap-*-status.txt`.

## Checks

`validation/`: `cargo clippy -p fold_db -p lastdb_node --all-targets -- -D warnings`, `cargo fmt --check`, and `scripts/lints/lint-diff-{file-size,fn-size,new-allows}.py --base origin/main`. All pass.

## What is left

The small shape is now about 2.2 KB for each path, near the 1–2 KB goal. These parts are left:

- The molecule tip and key metadata: about 0.74 KB. The molecule map keeps its own `String` copies of the hash and the range. That code is in `atom/molecule_hash_range`, outside the warm set.
- The entry vectors of the slot, source, and path tables: about 0.53 KB. Most of it is spare capacity after a doubling.
- The packed `mk:` and atom source keys: about 0.27 KB. The core can derive the `mk:` key from the slot key with the molecule key codec.
- Atom links and atom tracking: about 0.28 KB.
