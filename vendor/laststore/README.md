# laststore

`laststore` is the disk engine of [LastDB](https://thelastdb.com).
LastDB uses disk format v2, which is the module `laststore::disk_run`.
License: MIT OR Apache-2.0.

## Disk format v2 (`src/disk_run/`)

A run is one immutable file of records in strict ascending key order.
Each record is a key and a value, or a key and a tombstone.
A run file has data blocks, index blocks with a bloom filter, a top block, and a 64-byte footer.
Each block is one `frame`: zstd when it makes the block smaller, then AES-256-GCM.
The block key comes from the home key and the run id.
A reader reads the footer and the top block, then one index block and one data block for each lookup.
A read call keeps no index block, data block, or record after it ends.
A home keeps at most 32 opened readers of live runs between calls, least recently used first.
Each kept reader holds a file descriptor, the cipher, the footer, and the decoded top block.
A merge that replaces a run drops its reader.

An ordinary open reads at most 512 tail bytes first.
When those bytes contain the complete top frame, the footer and top need one file read.
Larger top frames use a separate bounded read.
The footer, frame authentication, run identity, counter, and size checks still execute.

A home (`disk_run::home::RunHome`) is a directory:

```text
<root>/laststore-run-home-v2   64 B clear descriptor: magic, home id, CRC
<root>/LOCK                    exclusive owner lock (flock)
<root>/CURRENT                 sealed list of runs + frontier
<root>/runs/<run id>.ldrun     immutable run files
```

- A commit writes one new run, makes it durable, then publishes a new CURRENT by rename.
- A read holds one CURRENT for its full call, so it sees complete commits only.
- `merge_step_with_space` merges eight runs of one level into one run, and moves old runs into the base.
- `plan.rs` states the size bound of the home.
- Automatic base compaction keeps the 1 MiB tiered-byte floor. Deleted and replaced records use a 64 KiB floor with the existing 1/8 stale-byte share.
- At open, the owner deletes temporary files and runs that CURRENT does not name. Nothing replays.

## Scope

This crate holds disk format v2 and the frame, space, and read-receipt code that it uses.
