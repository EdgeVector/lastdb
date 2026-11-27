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
<root>/commit-log/<seq>.log    encrypted commit-log frames (when opened with open_logged)
```

- `RunHome::open_logged` (`disk-run`): each commit appends one encrypted frame to `commit-log/<seq>.log` with one ordered flush (`F_BARRIERFSYNC` on macOS, `fdatasync` elsewhere) and records the commit in the memory layer. Background sealing, explicit cleanup, snapshots, and `close_log` write logged commits into a sorted `.ldrun` and publish `CURRENT` by rename.
- `RunHome::open` (`disk-cas`): each commit writes one sorted `.ldrun`, makes it durable, and publishes `CURRENT` by rename without a commit log. If a prior logged owner left unsealed frames, `open` seals them and removes `commit-log/`.
- A read holds one published root (`CURRENT` plus any unsealed memory layer) for its full call, so it sees complete commits only.
- `merge_step_with_space` seals the commit log when ready, merges eight runs of one level into one run, and moves old runs into the base.
- `space.rs` enforces free-space headroom, replacement reservations, and bounded file inventory across the home.
- Automatic base compaction keeps the 1 MiB tiered-byte floor. Deleted and replaced records use a 64 KiB floor with the 1/8 stale-byte share.
- At open, the owner deletes temporary files and runs that `CURRENT` does not name, replays any commit-log frames above `CURRENT`'s frontier into memory, and prunes retired `.log` files already covered by `CURRENT`.

## Scope

This crate holds disk format v2 and the frame, space, and read-budget code that it uses.
