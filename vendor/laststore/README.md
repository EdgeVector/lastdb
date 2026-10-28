# laststore

`laststore` is the disk engine of [LastDB](https://thelastdb.com).
LastDB uses disk format v2, which is the module `laststore::disk_run`.
License: MIT OR Apache-2.0.

The [V2 compatibility contract](../../docs/lastdb-disk-format-v2.md) records the accepted file versions, encryption, limits, and crash rules.

## Disk format v2 (`src/disk_run/`)

A run is one immutable file of records in strict ascending key order.
Each record is a key and a value, or a key and a tombstone.
A run file has data blocks, index blocks with a bloom filter, a top block, and a 64-byte footer.
Each block is one `frame`: zstd when it makes the block smaller, then AES-256-GCM.
The block key comes from the home key and the run id.
A reader reads the footer and the top block, then one index block and one data block for each lookup.
A reader keeps nothing after its call.

An ordinary open reads at most 512 tail bytes first.
When those bytes contain the complete top frame, the footer and top need one file read.
Larger top frames use a separate bounded read.
The footer, frame authentication, run identity, counter, and size checks still execute.
Bounded backup readers keep their exact footer read and existing memory admission.

Ordinary encrypted payloads of at least 4096 bytes use the existing OpenSSL AES-256-GCM backend.
Smaller payloads and bounded readers retain the prepared Rust cipher path.
The native context ends after each frame. Plaintext stays private until the full tag check succeeds.
The native path clears its plaintext on failure. The derived key clears when its reader ends.
This changes no key derivation, nonce, authenticated header, frame format, or compression policy.
Authentication time includes native context setup. Counter receipts also report native frame bytes and metadata read bytes.
Read contexts share one immutable parent list. A timer does not allocate or copy that list for each frame.
Each scope still contributes its measurements to every distinct parent once.

A home (`disk_run::home::RunHome`) is a directory:

```text
<root>/laststore-run-home-v2   64 B clear descriptor: magic, home id, CRC
<root>/LOCK                    exclusive owner lock (flock)
<root>/CURRENT                 sealed list of runs + frontier
<root>/runs/<run id>.ldrun     immutable run files
```

- A commit writes one new run, makes it durable, then publishes a new CURRENT by rename.
- A read holds one CURRENT for its full call, so it sees complete commits only.
- `merge_step` merges eight runs of one level into one run, and moves old runs into the base.
- `plan.rs` states the size bound of the home.
- Automatic base compaction keeps the 1 MiB tiered-byte floor. Deleted and replaced records use a 64 KiB floor with the existing 1/8 stale-byte share.
- At open, the owner deletes temporary files and runs that CURRENT does not name. Nothing replays.

## Proofs

The examples `disk_run_proof` and `run_home_proof` are CI-only proofs.
`.github/workflows/disk-run-proof.yml` runs them.

## Old engines

The old LastStore segment engine (`store`) and the disk format v1 module (`disk_pack`) are deleted (decision D1, 2026-10-26).
This crate holds only disk format v2 and the frame, space, and read-receipt code that it uses.
