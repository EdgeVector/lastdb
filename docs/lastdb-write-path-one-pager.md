# LastDB write path and warm set: one page

Status (2026-10-14). This page summarizes `docs/lastdb-resident-convergence.md`,
`docs/lastdb-warm-set-design.md`, `docs/lastdb-blind-write-design.md` and
`docs/lastdb-write-ahead-journal-design.md`. Read those for the evidence.

## Contract

- The node acks a write after it applies the write in memory. A read after the ack sees the write.
- Disk and cloud come after the ack. The loss window is the write lane backlog.
- The mutation log keeps ack order, for multi-device cloud sync.
- The warm set holds 10000 exact records and purges the least recently used. The purge never drops a record that is not yet flushed.

## Flow of one write

1. Stamp the write, apply it to memory, and send the ack.
2. The schema's write lane writes atoms, tips and molecules to the store in the background.
3. Each protein sibling is a separate background write at its own key. Stale siblings, and a crash between the ack and a sibling write, are accepted.
4. The flush to disk runs every 500 ms. A create touches 22 hash groups. Each group costs one forced disk write (`fsync`).
5. After the store flushes a written group, drops a group from memory, or shuts down, it writes the group's sidecar after the segment files (see Words).

## Current state

- Two memory structures exist: the ResidentGraph (database layer) and the warm set (store layer). The primary runs in `write` mode.
- Before #176 a create cost 28.7 forced disk writes: 22.2 for data and 6.5 for sidecars. The lane drained about 4.6 new-key creates/s.

## Built and merged

- #109 synthetic-data bars. They never copy `~/.lastdb`.
- #112 fixes reads after an ack in `write` mode.
- #160 fsync gauges.
- #167 fewer groups per write.
- #169, #170 and #171 harden the atom GC reaper.
- #172 the blind-write and journal designs.
- #176 the sidecar flush rule. A group under 1 GiB no longer pays the sidecar forced write. The A/B showed 0 sidecar fsyncs against about 1550 per round.

## Designed, not built

- **One warm set.** The ResidentGraph shrinks to a write queue. Order: gauges, bug fixes, hash view, log order, store limits, shutdown drain, then the graph change in three parts. Stop after the hash view if live gauges say 10000 records cannot serve the real reads.
- **Blind write.** The ack needs no store read. The lane reads the durable tip in one batch and keeps the winner by clock. Compare-and-set, org homes and delete keep their read for now. Five gates come first.
- **Group reduction.** Layout merges cut 22 groups to about 14 to 18 (a guess). The change is one-way.
- **Journal.** Not built. It cuts latency but not the count of forced writes. Build it only if the numbers still ask for it.

## Words

- **Hash group.** The set of files on disk that holds the records of one hash bucket. A hash group never enters the warm set.
- **Sidecar.** The file `keys-v1.idx` beside a hash group's files. It lists the record ids in the group and the live and dead byte counts. It is a cache with a checksum. Any doubt makes the reader ignore it and load the group.
  - Key lists, GC probes and dead-byte counts read it, so they need not parse the whole group (about 5 MB for an `atoms` group on the primary).
  - `drop_hash_group_dir` needs it to prove which ids a group too large to load holds.
  - Code: `vendor/laststore/src/keysidecar.rs`.
- **Forced disk write.** `fsync` (`F_FULLFSYNC` on macOS). It costs 5 to 12 ms.

## Not proven

- Power loss, for the sidecar rule (#176).
- Whether 10000 records serve the real reads on the primary. No live gauge exists yet.

## Decisions for Tom

1. The 1 GiB sidecar floor.
2. Make parallel flush the default from 4 groups. This needs a blob GC order review.
3. The header checkpoint.
4. The change feed append.
5. The one-way layout merges.
6. Build the journal or not, after the first trims.
