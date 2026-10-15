# Disk-pack v1: first direct implementation

The read measurements merged in LastDB PR 174.
This change implements the first Stage 2 unit from the [pack plan](https://chatgpt.com/space/page_dac3cffa86f08191b9fe60c47d7477a9).
It adds a directory reader and a separate artifact builder.
It does not enable a LastStore home writer or change LastDB query routes.

The implementation uses the byte layouts in [the format document](lastdb-disk-pack-format-v1.md).
It supports these objects:

- A fixed 4,096-byte CURRENT with a complete checksum and explicit codec identities.
- Immutable 16 KiB directory, locator, overflow, and block-map pages.
- Ordered trees with at most eight levels, 53 leaf entries, and 89 children per internal page.
- Full keys of at most 65,535 bytes, with at most five overflow pages per key.
- Independent 4 KiB payload blocks in physical parts of at most 8 MiB.
- Plain frames with CRC32, independent zstd frames, and a bounded decoder window.
- Full record digests, exact keys, collection digests, mutation versions, and versioned tombstones.

The reader follows exact file positions.
It does not reconstruct a hash group or read a complete footer.
One call shares metadata pages, part headers, and payload blocks.
The call releases those objects at its end.
A missing referenced file, unknown format, or damaged object returns an error.
Only a valid absent directory entry, absent locator entry, or tombstone returns absence.

The call requires positive page, block, byte, and key budgets.
The artifact reader permits at most 4,096 pages, 16,384 blocks, 256 MiB, and 10,000 keys per call.
Budget exhaustion returns `ReadBudgetExceeded`.
The byte budget counts file bytes, decoded blocks, record assembly, and full-key materialization.
These artifact limits do not select the future production admission policy.

The builder consumes ordered keys with bounded buffers.
It keeps at most 53 leaf entries and 177 child descriptors at each tree level.
It streams record bodies through a 64 KiB buffer.
It preserves stored body bytes and supplied physical mutation versions.
It writes file barriers before the initial CURRENT, then uses a rename and a directory barrier.
An error or an unfinished group prevents publication.

CAUTION: An artifact is not a LastStore home.
The builder requires a new path and creates an artifact marker.
The legacy LastStore open paths refuse that marker or a disk-pack CURRENT.
The command cannot replace an existing home or activate an online writer.
All artifact files remain in place until the owner removes the artifact.
The caller must retain the selected generation for the complete read call.

## Product command

`lastdb-pack` accepts explicit paths.
It has no primary-home default and no network dependency.
The command supports one named collection and group per artifact import.
The library supports more groups in exact encoded directory-key order.

```sh
lastdb-pack build --artifact NEW_PATH --input records.jsonl \
  --collection COLLECTION --group GROUP --frontier FRONTIER
lastdb-pack read --artifact PATH --input keys.jsonl \
  --collection COLLECTION --group GROUP \
  --pages 512 --blocks 1024 --bytes 16777216 --keys 256
```

A build input line contains `key`, `batch`, `operation`, and `body_hex`.
A tombstone uses `deleted: true` and omits `body_hex`.
Keys must use strict byte order.
Each input line is at most 2,200,000 bytes in this command.
The streaming library retains the legacy body ceiling of `u32::MAX`.
A read input line contains one JSON string with an exact storage key.
The result preserves request order, repeated keys, empty bodies, and valid absence.

The codec arguments default to identifier 1 for separate artifact use.
These values do not allocate a production home codec or feature bit.
The future LastDB integration must obtain both identifiers and the group from the held home descriptor.
The reader checks that CURRENT contains the expected identifiers.

## Operational receipts

The [receipt report](proofs/disk-pack-reader-20261015/report.md) records the generated inputs and observed results.
The commands use newly generated files in an owned temporary path.
They exercise the pack command, with no primary access or primary copy.
They do not establish LastDB app latency, protected write admission, or a recovery result.

## Remaining dependency order

1. Map every store write and memory ack to a complete recovery-journal batch.
2. Add bounded journal recovery, immutable tree updates, and the atomic root publisher.
3. Add the compatible bridge reader and allocate the home fence.
4. Connect exact, batch, and hash-range LastDB reads, with protected warm-set admission.
5. Add backup retention, source-free restore, and concurrent conversion.
6. Collect fresh-node LastDB receipts, then prepare the primary upgrade for Tom's separate decision.

Encrypted physical frames, cloud-only entries, and bridge values return explicit errors in this unit.
The owner atom encryption bytes remain unchanged.
The 10,000-record logical warm set uses its current path.
The new reader does not enter it yet.
The factory, reset stores, primary, and archive are outside this work.
