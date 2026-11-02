# Warm set byte limit design

Status: implemented. Tom approved D1–D5 on 2026-10-19.
This page describes the warm set of a format 8 home.
See [the memory and disk routes](lastdb-rehydrate.md) for the read and write rules.

## Answer

Limit the warm set by bytes, not by a record count.

- The warm set unit is one decoded record at one record key (decision D2 of the record model).
- Each warm record pays its own calculated memory size.
- A new read always enters the warm set. When the warm set is full, the purge removes the least recently used clean records (Tom, 2026-10-19).
- Status reports the charged bytes next to the measured process memory, so an error in the estimate is visible.

The charge is an estimate with a small, bounded error. It is rounded up, so the error is on the safe side.
A record count has no bound on its error: 10,000 records can use 10 MB or 2.5 GiB.

## The charge

Code: `fold_db/crates/core/src/resident/core_set/charge.rs`.
One charge for each warm record:

```
record_charge = RECORD_BASE + Arc header + size of StoredRecord
              + capacity of the key coordinate strings
              + fields: for each entry, JSON_MAP_ENTRY + name capacity + json_heap_bytes(value)
              + stamps: for each entry, JSON_MAP_ENTRY + name capacity + stamp strings
              + record metadata: source file name, metadata map entries, stamp strings
              + the delete floor stamp of a tombstone
```

`json_heap_bytes` walks the JSON tree one time, at admission:

| JSON node | Charge |
|---|---|
| null, bool, number | `VALUE_SIZE` (the size of `serde_json::Value`) |
| string | `VALUE_SIZE` + string capacity |
| array | `VALUE_SIZE` + vector capacity × `VALUE_SIZE` + children |
| object | `VALUE_SIZE` + for each entry: `JSON_MAP_ENTRY` + key capacity + child |

The charge rounds up:

- Round each record charge up to a multiple of 16 bytes.
- Multiply by `SAFETY_PERCENT` (110). This covers allocator size classes and B-tree node slack.

The string, vector, and map sizes are calculated, not guessed.
Only the per-object constants (`RECORD_BASE`, `JSON_MAP_ENTRY`) are estimates.
So the total error is at most the number of objects times the constant error, plus the allocator rounding.
It does not grow with the size of the values.

## The limit

Code: `fold_db/crates/core/src/resident/warm_bytes.rs`.

- `LASTDB_CORE_WARM_BYTES` sets the limit. The default is 256 MiB (D2).
- The daemon refuses to start on a value that does not parse, or on zero.
- The byte limit is the only warm set limit. There is no record count limit (D3).

## Admission and purge

A read fetches a record from disk, then admits it.
The record size is known only at admission, so admission calculates the charge after the fetch.

1. The record enters as the most recent entry, and its charge joins the total.
2. If the total is over the limit, the purge removes the least recently used records that no call holds.
3. The purge measures what it freed from the charge itself, not from a nominal size.
4. No read skips the warm set, and no read fails because the warm set is full.

If only held records remain, the charge stays over the limit.
The amount over the limit is at most the bytes that running calls hold. Those bytes are in use, so the charge stays honest.
Each later admission runs the purge again. When the calls end, the purge returns the charge under the limit.
Status counts each admission over the limit (`over_limit_admissions`).

Every check starts with an O(1) comparison, so a warm set under its limit pays no extra allocation.
Records enter after a read and after a flush, and the purge runs only then. No timer runs a purge.

## Writes

A native write commits to disk before it publishes clean records to the warm set (Tom, 2026-10-30).
So the warm set holds no unflushed record, and `dirty_bytes` reads 0.
A write request stages its written records up to the dirty share of the limit: 25% (D4, `CORE_WARM_DIRTY_PERCENT`).
Those records enter the warm set after the flush. The rest enter on a later read.
Queued disk bytes are reserved in the write queue, not in the warm set charge.

## Status

The native core status reports `CoreWarmByteStats` (`resident/core_set/bytes.rs`):

| Field | Meaning |
|---|---|
| `limit_bytes` | The configured limit |
| `used_bytes` | The current charge |
| `peak_bytes` | The highest charge at a status read since start |
| `record_bytes` | The charge of the decoded records and their keys |
| `dirty_limit_bytes` | The dirty share of the limit |
| `purged_bytes` | Bytes that the purge freed since start |
| `over_limit_admissions` | Admissions that left the charge above the limit |

The fields `path_bytes`, `slot_bytes`, `link_bytes`, `held_bytes` and `dirty_bytes` stay on the wire for installed clients. They read 0.
`lastdb status` prints a "Warm set bytes" line.
The self-metrics sampler records the process footprint next to the charge. The ratio of the two is the check on the estimate.
The process memory projection charges only the warm set byte limit.

## Calibration

CAUTION: Do not copy `~/.lastdb` and do not run a node on a copy of it (Situation `no-copies-of-primary-database-20261012`). Calibrate on synthetic data and on read-only live status only.

1. Build a synthetic home with three record shapes: small (about 200 bytes of JSON in about 10 fields), medium (about 8 KB), and large (about 60 KB, under the 64 KiB record cap).
2. For each shape, fill the warm set to its limit. Read `used_bytes` and the allocator in-use bytes before and after the fill.
3. Set each constant so that the charge is at or above the measured growth, within 10%.
4. On the primary, compare `used_bytes` with the footprint growth in the self-metrics log. Look only at the live signal. Do not copy data.

This is a measurement, not a test. No test suite is written.

## Risks

| Risk | Effect | Control |
|---|---|---|
| The constants are wrong | The charge is too low, and memory grows past the limit | Round up, `SAFETY_PERCENT`, the status ratio, calibration |
| A small limit | More disk reads, lower hit rate | Report the hit rate with the byte fields. Change the limit from measurement |
| Held records go above the limit | The charge can pass the limit while long calls run | The overshoot is at most the bytes that calls hold. The purge brings the charge back when the calls end |
| One large call | One call can read up to 256 MiB (`ReadBudget`). With a 256 MiB limit, one call can push out every other record | This is the least recently used rule. Set the limit above the read budget of a call, or lower that budget |

## Later step

The same reservation type can carry the definition cache and the write metadata.
A later design can put them under one total, with a share for each. That gives one number for "memory used by data".

## Decisions (Tom, 2026-10-19)

- **D1. Byte limit in place of the record count.** DECIDED: yes.
- **D2. Default limit.** DECIDED: 256 MiB to start, changed after calibration.
- **D3. The count limit.** DECIDED: remove it. The byte charge covers the growth that a count would protect.
- **D4. Dirty share.** DECIDED: 25% of the limit for staged written records.
- **D5. A full warm set on a read.** DECIDED: every new read enters the warm set. The purge removes least recently used clean records to make room. If held records leave no room, the record enters over the limit, and the purge corrects it after the calls end.
