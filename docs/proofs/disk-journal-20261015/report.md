# Complete journal batch operational receipt

Date: October 15, 2026.
This receipt covers separate artifact construction and recovery.
It does not establish LastDB node recovery or activate an online writer.
The primary, archive, factory, and reset stores were not used.

Source: `28b03e26434ec4b2bcf8bfe21d7d02286edcb820`.
The programs used a release build on macOS with Rust 1.96.1.
The host had 14 logical CPUs and an observed load average near 5.
The filesystem-page state is unknown.

| Program | SHA-256 |
|---|---|
| `lastdb-journal` | `a665db06c511fc78e492026e17285aaba33dbc9fcac7dbfe5d69ea087d850bea` |
| `lastdb-pack` | `c061da6597900ccab7230e16a5bc4fa8c731952dd7d89dab933372d1d3237cf2` |

The inputs came from generated data in an owned temporary path.
Each artifact contained one committed pack value before the journal commands.
The journal command then created complete batches and exited.
A separate recovery command reopened the durable files.
It verified each batch and the final captured TAIL.

The normal input contained 96 operations across three collections and named groups.
It included puts, deletes, repeated coordinates, and empty bodies.
The command used six batches of 16 operations.
The accepted time was the supplied constant 1792074000000.
Recovery preserved the time, sequence, operation number, collection, group, storage-id digest, body size, and body digest.

Other inputs exercised these normal format dimensions:

- Eight keys of exactly 65,535 bytes, with 128-byte bodies.
- Ten generated bodies of 1,048,576 bytes, across physical data parts.
- A 4,160-byte logical batch with COMMIT across a 4 KiB boundary.
- A 4,377-byte logical batch with an operation header across a 4 KiB boundary.

The [verification receipt](verification.json) records true value-and-order comparisons for all 117 operations.
Every recovery ended with `complete: true`.
No input came from an existing user database.
The group values are explicit artifact coordinates; production placement integration remains open.

| Input | Batches | Operations | Decoded bytes | Recovery frame bytes | File opens | Time, ms |
|---|---:|---:|---:|---:|---:|---:|
| Normal | 6 | 96 | 32,379 | 14,653 | 27 | 2.669958 |
| Long keys | 1 | 8 | 1,051,163 | 27,545 | 262 | 17.760416 |
| Large bodies | 1 | 10 | 10,488,251 | 10,693,041 | 2,570 | 134.442125 |
| COMMIT boundary | 1 | 1 | 4,160 | 475 | 7 | 2.679666 |
| Operation boundary | 1 | 2 | 4,377 | 664 | 7 | 1.255166 |

Times are single command observations, not percentiles.
The recovery command time includes its inspection output.
The largest instrumented temporary byte-buffer peak was 1,126,642 bytes.
The count excludes returned mutations, parsed metadata, input JSON, and allocator overhead.
It is not process memory size.

All recovery receipts reported zero group opens, legacy replay bytes, footer bytes, and directory walks.
Explicit recovery correctly reported nonzero `journal_replay_bytes`.
The ordinary read still returned the committed pack value and reported zero journal replay bytes.
That read did not inspect the unpublished journal suffix.
The future publisher and local write state must provide online read-after-ack behavior.

The append receipts record the actual durable data and map bytes.
The suffix reserve uses the uncompressed worst case, with frame, part, and map overhead.
It does not use compression to admit more work.
No file damage, crash injection, tests, or fixture harness formed part of these commands.

Package clippy, full workspace clippy, release builds, format, and source-size checks passed.
The branch secret check passed without a rule override.
The inspection field uses `storage_id_digest` to distinguish a stored-record identity from an encryption key.
The code retains the existing platform data and directory barriers.

Remaining work includes immutable directory updates, atomic root publication, online admission, token coverage, and the home bridge.
Range integration, backups, source-free restore, conversion, and primary preparation remain open.
This unit is a dependency for those results.

The owner removed the generated temporary data after the receipts and PR evidence were preserved.
No ephemeral node ran in this artifact-only unit.
The source worktree and release programs remain available for review.
