# Native backup resource policy

The canonical source is [`lastdb_storage_policy/src/lib.rs`](../../lastdb_storage_policy/src/lib.rs).
The native store, cloud protocol, capture code, and restore code consume this dependency-light crate.
This change adds no runtime memory configuration or format migration.

## Fixed native format

`native_v1` defines the existing native file format.
Its values fix page addresses, record lengths, and compatible readers.
They are not environment settings or optional memory targets.

| Item | Native V1 value |
| --- | ---: |
| Page | 16 KiB |
| Decoded block | 4 KiB |
| Pages per physical part | 512 |
| Physical part | 8 MiB |
| Native record body | At most `u32::MAX` bytes |
| Native backup files | At most 65,552 |

A record body can span many native parts.
An eight MiB part does not limit a user file to eight MiB.
A change to these format values requires a new format and compatible reader and publication rules.

## Editable source resource values

`BACKUP_RESOURCES`, `native_read`, and `backup_decode` define source resource policy.
A source change and the normal build and review gates can change these values.
They do not change the native V1 file format.

| Resource | Current value |
| --- | ---: |
| Cloud work allocations | 96 MiB |
| Decoded values within that allowance | 64 MiB |
| Stream buffer | 1 MiB |
| Concurrent source streams | 4 |
| Tracked metadata or temporary object cache | 1 GiB |
| Native copy run | 16 blocks |
| Native payload file handles | 32 |
| Restore external pointer and kind identities | 43,690 within 8 MiB |

Memory values control buffers and bounded decoded metadata, not a complete user file.
The cloud work allowance does not cap the whole LastDB process.
The native copy derives cumulative I/O work from the exact located record and releases caches after each run.
Capture and restore use private disk files for larger ciphertext, JSON, and envelope stages.
Filesystem capacity checks admit those payload and scratch bytes separately from the metadata allowance.
Capture retains one complete active external ciphertext until all of its part receipts reach disk.

`backup_memory` defines separate proof and upload phase allowances.
The proof phase allowance is 88 MiB within the current 96 MiB policy.
The upload phase allowance is 40 MiB, with six bounded buffers for each of four streams.
Those buffers include the HTTP backend, the queue, and the file reader.
The worker completes each upload batch before the next proof or manifest phase.
The two phase allowances do not overlap.
Restore and manifest assembly share the receipt and identity memory charges in `backup_decode`.

## Existing cloud and local limits

The cloud wire still admits objects of at most 64 MiB.
One external ciphertext uses at most 512 ordered receipt extents and at most 32 GiB, subject to complete manifest page bytes.
Manifest and reference pages remain at most 256 KiB with at most 512 entries.
The complete referenced backup remains subject to its one TiB policy and the other count and byte limits.
The first reached bound controls admission.
These limits do not promise every maximum at once.

The normal local file API keeps its separate owner-configurable 16 MiB default.
Backup authentication does not use that default as a file-size guard.
The disk-stream path preserves existing encryption formats, keys, and exact ciphertext.
It requires the complete tag, plaintext identity, exact length, and EOF before a successful proof.

Cloud sync and automatic deletion remain off.
Format and source checks do not establish runtime memory, provider speed, or successful cloud recovery.
