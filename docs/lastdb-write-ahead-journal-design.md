# Write-ahead journal for the store: design and the decision not to build it yet

Policy update, 2026-10-20: the memory-ack policy below is superseded.
Every successful local mutation request now waits for its native disk sequence before the ack.
Earlier measurements below describe their recorded sources and remain unchanged.
See the [current memory and disk contract](lastdb-rehydrate.md#write-and-flush).

Status (2026-10-13): design for Tom's review. Nothing here is built. The result
of the design work is a decision order: do the group reduction first, run
Experiment 0, and build the journal only if the numbers still ask for it. Made by
four readers (store write path and recovery, the lane and mutation path, the
consumers of write order, cost and risk), one designer, two skeptics and a
revision pass. The agents read the code and ran nothing.

Update (2026-10-14): the sidecar flush rule is built (it replaces the B6 rule
below). The key sidecar keeps its write aside and rename. The forced disk write
before the rename happens only for a group of half the cold-load cap or more
(1 GiB by default; `LastStore::sidecar_flush_floor`). A smaller group is not
flushed. The counts "6.4 sidecar fsyncs for each create" below are the numbers
before this rule. The journal design itself is unchanged and still not built.

Why it was designed. A new-key create syncs 22 hash groups, about 150 ms of
fsync (measured below). A journal would pay one sequential fsync for a batch and
update the hash groups lazily. The design shows that it moves the fsyncs from
the lane to the 500 ms tick but does not remove them, because almost no group
repeats inside one tick when each create uses new keys. So the journal mainly
cuts lane latency and the durable-ack wait. Group reduction multiplies both the
lane and the tick.

## Experiment 0: what one create costs (measured 2026-10-13)

Current main with PR 160, 167, 169, 170 and 171 merged. Fresh synthetic home,
write mode, a 4-field schema, new keys. The trace is a throw-away build of
`sync_open` (observation only, not committed).

Groups synced per new-key create (30 creates, 22.0 syncs per create):

| Group family | Syncs | Time |
|---|---|---|
| tips (4 tips and 4 headers) | 7.9 | 46 ms |
| atom reference edges v2 | 4.0 | 29 ms |
| atoms | 4.0 | 24 ms |
| change feed | 2.0 | 22 ms |
| schema index | 1.2 | 7 ms |
| idempotency | 1.0 | 6 ms |
| atom locators | 1.0 | 6 ms |
| metadata (author clock) | 0.9 | 11 ms |

Drain of 100 new-key creates:

| Setting | Creates per second | Data and sidecar fsyncs per create |
|---|---|---|
| main | 4.6 | 22.3 and 6.4 |
| parallel flush at 4 groups, 8 workers, whole-store parallel | 6.8 | the same count |
| zero-fsync lane (`LASTDB_TXN_DEFERRED_FLUSH=1`, never on the primary) | 23 | 7.6 and 0.7 (the tick had not covered the rest) |

With no fsync the lane still needs about 43 ms per create, so about 23 creates
per second is the ceiling for the lane compute under that load (load average 8 to
9). Earlier, `LASTDB_TXN_DEFERRED_FLUSH=1` followed by a kill -9 right after the
drain lost 92 of 150 acked writes, 82 after 1 s, 18 after 3 s and none after 8 s:
the tick walks groups serially and needs seconds to cover a burst. Deferring the
barrier alone widens the crash window.

One `F_FULLFSYNC` costs about 5 ms unloaded and up to 12 ms under load. Sixteen in
parallel cost 2.1 ms each.


## Answer

Do the group reduction first, because the journal moves the fsyncs off the lane and does not remove them (about 29 F_FULLFSYNC per create stay). Step 0 is a measure-only prototype on a fresh synthetic home: a trace switch that lists the 22 hash groups of one create, plus the knob baseline. If the numbers still ask for a journal, build it inside the vendored laststore at the one function every put and delete passes, and write each record with pwrite under one lock. The tick then syncs the hash groups in parallel with no order rule, and recovery cuts each named group back to its synced length and appends the journal tail. After a kill -9 no write is lost whose store call returned, but the client ack still comes before any store write, so the loss window stays the lane backlog.

## Where the journal lives

DECISION: if the journal is built, build it inside the vendored laststore, not above it. Nothing is built before Experiment 0 and the group reduction (see staged_plan).

Path codes: S = vendor/laststore/src. P = fold_db/crates/core/src. Both are under the worktree root. Tags such as [A1] or [B2] point to the skeptic items (see the disposition list in comparison_with_alternatives).

Location on disk: <root>/journal/, next to data/ and laststore-layout-v1. It is not inside data/. The layout walkers, the backup manifest and the group walkers read only data/, so none sees the journal. The journal is not in a backup. After a clean stop the journal holds no records, so a stopped copy of data/ is complete.

Files:
- journal/LOCK: an exclusive flock, held while the store is open.
- journal/<seq>.wal: about 64 MiB each, zero-filled ahead, sealed at the roll.
- The first file is created at the first journaled write, not at open [B13].
- The newest file is never unlinked. Its header holds the LSN floor [A11].
- One journal serves the whole store: all collections, all lanes. The node runs one LastStore.

Open modes [B14]. Every opener names one mode:
- Writer: takes the lock, replays, may write. The daemon uses it.
- ReadOnly: no lock, no replay, no write. It sees group files only. Inspection tools and migration sources use it.
- Restore: journal off. A fresh destination home is written without a journal [A5].
The card lists every opener and its mode: lastdb-maintenance, db_response_cmds, version_retention_cmds, cloud purge (a second LastStore in the daemon process), backup, restore, cloud-purge adoption. A tool that needs the full state asks the daemon for a checkpoint, or opens a stopped home as Writer.

Scope of the first version: hash-group layout, plain packaging, sorted_segments=false. The product uses exactly that today (S/options.rs:292-303, P/fold_db_core/factory/local.rs:399-451). open_resolved refuses the journal for every other descriptor (sorted, frame-AEAD, segment_log). Those homes keep the barrier path [A10]. Each Shard also holds an Arc to the journal, set at construction (load_shard included). A Shard without it is a bug, and the wal_violations counter reports it.

Why laststore. Each reason was read in code:
- Every store write ends in append_with_threshold (S/store/append.rs:202). It sees the collection, the id, the exact record bytes and the op. Its callers are point put, delete, compare-and-swap, transaction, append_for_resident, version retention and restore.
- The key stripe and the shard lock are held there. An LSN taken under them, with the pwrite in the same journal lock section, gives a log order equal to the memory order of each key and to the read-from order across lanes. A journal above the store needs one global mutex around every store call to get the same result.
- Spill, purge, eviction, snapshot, compaction and the pin reap are internal. The WAL rule (a group file may hold a record only if the journal record is durable) must sit in spill_open. Only the store can put it there.
- Open and tail repair live in the store. About 20 offline tools open the home through LastStore::open and gain replay with no change.
- On a plain home the journal bytes equal the group bytes. Atom bodies are sealed above the store. The journal discloses nothing new.

Rejected places: a KvStore wrapper (it misses direct LastStore users and cannot gate spill), the EncryptingKvStore layer (the bytes are plaintext before sealing), and the cloud-sync mutation log (see the comparison). A wrapper is useful only as the throw-away prototype in Experiment 0e.

Cost of the choice: a vendored change ships through lastdb-safe-upgrade. A new on-disk file class needs Tom's decision.

## Record format

Unit: one journal record is one store put or one store delete. There is no batch frame in this version.

Why not one record for each lane batch. The LSN must be taken in the same lock section that changes memory. A batch frame written after the memory writes lets a dependent write in another lane reach the journal before its parent (a shared atom, a count record, a sibling tip). Per-op records keep the order. No path has batch atomicity today, and callers accept any crash prefix (P/storage/traits.rs:368-374). The journal keeps that contract and makes the prefix strict.

File header (64 bytes):
- magic LSJ1, format version u16, packaging u8
- epoch id (16 bytes, new at each open)
- file seq u64, first LSN u64
- prev_end_lsn u64: the last LSN of the previous file that counts. A file that was poisoned ends there.
- CRC of the header. An all-zero header means an unused pre-created file [A11].

Record: magic u8 (0xA7) | kind u8 | flags u8 | payload_len u32 | lsn u64 | payload | crc u32.
The CRC is CRC-32/IEEE (crc32fast, already a dependency; S/sorted_file.rs uses crc32fast::hash). It is not CRC32C. The file header names the polynomial [A12]. The CRC covers kind, flags, length, lsn, payload and the epoch id.

Kinds:
- PUT (1) and DEL (2). Payload: collection_len u16, collection, shard u16, group u32, line. The line is the exact output of segfmt::encode_put or encode_del. The group file later receives the same bytes. Flag GBASE (bit 0) means: this record opens a dirty period of its group. It adds base_seg u32 and base_len u64, the segment and the synced length of the group at that moment.
- GSYNC (3). Payload: collection, shard, group, seg u32, synced_len u64, applied_lsn u64. It is written after a group fsync succeeds and before the Shard is marked clean. It closes the dirty period. It is mandatory [A9, B10].
- CKPT (4). Payload: t_lsn u64, tail_lsn u64. Every record with an LSN below t_lsn is in a synced group file.
- DROP (5). Payload: a scope (a collection, or collection plus shard plus group). It is a redo record: recovery completes the drop. Destructive admin ops write it and commit it before they act [A4].
- END (6). Marks the end of a sealed file.

Size: about 19 bytes of framing, 10 bytes of routing, 15 to 25 bytes of collection name, and 12 more bytes of GBASE on the first record of a period. That is 45 to 60 bytes for each record. A create writes 22 to 40 records, so the overhead is 1 to 2 KB for each create, plus one GSYNC (about 60 bytes) for each group sync.

Sequence numbers. The LSN is a u64, up by 1 for each record, and it is the DurabilityToken. The same value goes into WriteAck.token. put, delete and transaction also get one (today only append_for_resident does, and next_durability_token restarts at 0 at each open, S/store/open.rs). At open the LSN continues from the end of the replayed journal. The floor is the first LSN in the header of the newest file [A11].

Torn-write handling:
- The file is created zero-filled, so the end of data is a zero magic byte.
- A half-written record fails the CRC. Recovery stops at the first invalid record. The LSN must equal the previous LSN plus 1. A gap ends the scan.
- A bad record in the last file is the torn tail only when its expected LSN is above the tail_lsn of the last CKPT. Records at or below that LSN were committed before the CKPT. A fault there is damage to durable data, so the open fails and names the file and the LSN. Only an operator command may skip it, and it logs the lost range [A9, B10].
- A bad record in a sealed file always fails the open. The roll fsyncs the old file first.
- Residual risk: a record that was acked durable after the last CKPT cannot be told from an unsynced one if the media lies. The window is one checkpoint interval (about 500 ms).

Encryption. Plain home: the record bytes equal the group bytes. Values are sealed above the store and the catalogs are plaintext by policy. The collection name and the key id are visible in the group files today, so the journal adds no exposure. Frame-AEAD homes are out of scope and keep the barrier path. A later version wraps each payload with frame::encode_frame (S/frame.rs:98), with a new chunk uuid for each journal file, the counter as nonce and a new file at each open (S/store/encrypted_load.rs:34). zstd stays off on the commit path.

Large records. This version journals every record, including a 16 MiB file blob. The cost is a double write, and the lock section copies the whole record. A size bypass is an open decision.

## Write path and fsync count

Write path for one put or delete (plain home), inside the key stripe and the shard lock:
1. Do every step that can fail first: encode the line, read the previous body, create_dir_all, tag_backup_excluded_collection, and the segment roll with its sync_open. Memory and journal are still untouched [A7].
2. journal.append, as ONE section of the journal lock [A1, B8]:
   - take the next LSN and the file offset;
   - if the group goes from clean to dirty, set the GBASE flag with (segment, synced length) and add the group to the dirty registry with first_unsynced_lsn = this LSN;
   - pwrite the record into the preallocated file;
   - advance the LSN and the offset only after the pwrite returned.
   No LSN can exist without its bytes in the kernel, so the file has no hole. The registry update is in the same section, so a checkpoint that reads the tail and the registry together never misses a group. For a small record the pwrite is a copy into the page cache (microseconds). When it returns, the bytes survive kill -9 and a panic.
3. Change memory: open_buf, the index, the residue (code unchanged). Store the last LSN of the group in the Shard.
4. Return. There is no fsync here.
In journal mode the inline threshold sync in append_with_threshold (S/store/append.rs:289-293) is removed. The checkpoint owns it [A7].

Error rules:
- pwrite fails (ENOSPC, EIO): the write fails before memory changes. The LSN was not consumed. The journal opens a new file whose header has prev_end_lsn = the last good LSN, so a partial record at the old tail is ignored. It retries once. If that fails, the node refuses writes with a retryable error and marks the lane unhealthy (PersistLaneFull::Unhealthy).
- A commit error (fsync of the journal fails) is fail-stop. The file is poisoned. The journal keeps the unsynced tail bytes in memory (cap 64 MiB; appends wait at the cap). It opens a new file and re-appends that tail with the same LSNs, then fsyncs. If that fails too, the node refuses writes. It never writes group files ahead of the journal [A2].
- A failed commit means the outcome is unknown, as a failed flush does today. apply_batch must not undo memory for this error class (P/storage/laststore/logical_path/batch.rs:49-55), because the records are already in the journal and replay would redo them [A7].

Commit. commit_through(lsn): if journal_durable >= lsn, return. Otherwise one waiter becomes the leader. It fsyncs every journal file that holds unsynced bytes, oldest first, and publishes journal_durable. The roll never fsyncs inside the lock. A waiter that arrives during an fsync waits for it and may need one more. The wait is 1 to 2 fsync times. Lanes, the change feed, the capture worker and the author clock share the same fsync.

Meaning of the old barrier. flush_scope(scope) becomes commit_through(tail at call). The scope argument no longer decides durability. This also removes the convoy over foreign groups (S/store/transaction.rs:12-27). flush_scope is the one funnel (flush_written_keys, the delete barrier, the GC flush, the author clock, the lane), so no product call site changes in the first journal step.

fsync count for one create (new key, 4 fields, 22 hash groups after the two merged trims):
- Before the sidecar flush rule (measured, #167): 22.2 data fsyncs + 6.5 sidecar fsyncs = 28.7 F_FULLFSYNC, all serial in the lane.
- Journal, commit for each KV batch: on the lane, 6 journal commits, shared with other callers. No group fsync and no sidecar fsync on the lane path.
- Off the lane, at the checkpoint: one fsync for each distinct dirty hash group (about 22 when 1 to 3 creates share an interval, because almost nothing repeats), one fsync for each new directory, and 2 to 3 journal fsyncs for each checkpoint.
- SUM of F_FULLFSYNC for each create, at 3 to 6 creates/s: about 22 + new directories + 6 + the checkpoint share. That is 29 or more. The journal does not lower the sum [B2]. It moves the fsyncs off the lane and runs them in parallel.
- Journal, one commit for each lane batch (later step): the lane path pays about 1/B commits for each create, where B is the batch size.
- Group reduction lowers every term of the sum. It is the first lever.

Lane change for the lane-batch step (the lane scope): a task-local flag, read in the async body before run_blocking, because task-locals die across the blocking hop (P/storage/laststore/mod.rs:204-248). apply_batch skips its commit when the flag is set. DeferredLaneWriter::write_batch (P/.../persist/lane_writer.rs:35-86) runs its envelopes as today, then does ONE commit, then returns Released(finished). On a failed stage it commits the finished prefix first, then returns Retry. The stale comment at P/resident/lane.rs:3-4 (adjacent envelopes share one group commit) is wrong today and becomes true after this step. The reaper is not in the lane scope and keeps its own commit; a debug assertion checks this.

## Lazy group update and the tick

What lazy means. Group files get the same bytes as today, spilled and fsynced together (sync_open). Only the time changes: the write happens at the checkpoint, not in the lane barrier. There is no state where a group file holds unsynced bytes that the journal lacks.

WAL rule (enforced inside spill_open, not at call sites): a group may write bytes to its file only if journal_durable >= the last LSN of the Shard. If not, call commit_through first. The rule covers the checkpoint, the roll sync, eviction (evict.rs:211, :314), warm_room.rs, warm_trim.rs:210, the transaction paths, snapshot and compaction. The Shard holds an Arc to the journal, so the static sync_open and spill_open functions can check it [A10]. sync_open is also called from a few read paths (sidecar repair, retire of a leased scan handle). A commit wait can run there. That is rare and stays inside one fsync time. The wal_violations counter must stay 0, and a debug assertion in spill_open checks the same rule. This is an invariant check, not a test.

Dirty period of a group: it starts at the first write after a sync (GBASE) and ends at the GSYNC that follows the next successful group fsync. The registry keeps first_unsynced_lsn for each period. The registry key is (ShardKey, generation id), and only the Shard with that generation clears the entry [A13].

Group sync failure [A3]:
- If a group fsync fails, mark the Shard poisoned. A later fsync that returns Ok does NOT clear dirty state. Some kernels drop the page error after the first failure, so a second Ok proves nothing. The platform behaviour of F_FULLFSYNC on macOS after a failure was not tested.
- The registry entry stays, so T stays behind. The journal keeps the frames.
- After a bounded number of retries the process exits (fail-stop). Recovery then rebuilds the group from the journal. This is safe only because the journal holds the data. Whether to exit or to stay up and refuse writes is an open decision.
- The open P1 papercut about a retry that clears dirty without an fsync must be fixed before the writer release, because sync_open is the core of the checkpoint.

Checkpoint (replaces the whole-store flush walk at the tick). One checkpoint runs at a time (a mutex) [A11]:
1. Under the journal lock, read the tail L and copy the registry (groups with first_unsynced_lsn <= L) and the dirs_pending set. Do this before any group is touched. This is the same seal-before-keys proof as S/store/flush.rs:37, but exact, and it also covers leased handles (flush_sync_keys does not list leases).
2. commit_through(L). One fsync.
3. Sync the listed groups with 8 to 16 workers (reuse the PARALLEL_SCOPE_FLUSH permit and the EMFILE reclaim in S/store/flush.rs:146-216). Each worker spills under the shard lock, runs the fsync on a duplicate descriptor outside the lock, then clears only the spilled prefix and appends GSYNC (segment, spilled length, last spilled LSN) [B17]. There is no order between groups.
4. Directories [A6, B5]. dirs_pending is a set of new directories and new files, each with its own LSN floor (the first record that created it). It is separate from the group registry, so another sync path that cleans a group cannot lose the flag. The checkpoint fsyncs every pending directory and its new ancestors once (collection, shard, g/, and data/ for the first). A set of already synced directories avoids repeats. A directory fsync is a full F_FULLFSYNC on Apple (S/durability.rs:90-93 and :124-129; Rust std maps sync_all to F_FULLFSYNC), so it counts in the sum. Plain mode skips directory fsync today, so a crash can lose a new group file today. The journal closes that hole. Syncs that happen outside the checkpoint (eviction, roll) also register their new directories.
5. T = the minimum of the registry first_unsynced_lsn values and the dirs_pending floors, or L+1 if both are empty. Append CKPT(T, L). Commit. That is 2 to 3 journal fsyncs for each checkpoint in all.
6. Publish durable_through(L) (mark D). apply_durable_through clears the warm-set Dirty tokens. Reap the clean pins.
7. Unlink journal files whose last LSN is below T. Keep the newest file. Fsync the journal directory.

Dead Shard [A8]. A registry entry can outlive its Shard (a leased handle whose last Arc dropped; flush.rs:300-326 says its bytes can be gone). In plain hash-group mode the point-path threshold sync is a no-op, so the early-return path named by the skeptic does not trigger there. The guard is still cheap:
- A Drop guard on Shard: if it holds dirty ops, run sync_open, or flag the entry needs-replay.
- The checkpoint treats an entry with no live Shard as needs-replay. It runs the recovery routine for that group online (truncate to base, append the journal ops, fsync).
- Gauges: oldest_unsynced_age and stuck_entries. Alarm on both.
- Control records (CKPT, GSYNC, DROP) have reserved space that the journal cap never refuses.

Why no order rule between groups is needed. A group file holds only records whose journal record is durable (WAL rule). Whatever subset of group syncs finishes before a crash, replay of the open periods brings every group to the end state of the journal. T is the minimum over all open periods.

The tick. BackgroundFlushTask (P/fold_db_core/mutation_flush.rs:134-170) keeps its 500 ms cadence. DbOperations::flush today calls 8 whole-store flushes (P/db_operations/core/mod.rs:205-226). It calls checkpoint() once. Other callers use commit(). Under load the effective interval is max(500 ms, last checkpoint time), because the tick uses MissedTickBehavior::Delay. A checkpoint also starts early when the journal passes 128 MiB, when the dirty pin bytes pass the pin cap (proposal 256 to 512 MiB of loader_pin_bytes, S/store/pins.rs:305-318 is a measurement today, not a cap), or when the Dirty-token count nears the 5000-token refusal.

Gates before any journal code (Experiment 0d):
- T1: append 4 KiB then F_FULLFSYNC on a preallocated file with 1, 4 and 16 committers, while 8 other threads fsync other files. A device flush waits for the device queue, so the commit may slow down during a checkpoint. Go only if the commit p99 stays within 3 times one solo fsync [B17].
- T2: time N concurrent F_FULLFSYNC calls on N files against one call on a directory descriptor. This gives timing only. No shortcut (fsync() in place of F_FULLFSYNC, one barrier for many files) is adopted without Tom's decision, because kill -9 cannot show it is safe.

## Recovery and truncation

Recovery runs inside open_resolved (S/store/open.rs:31-83), before any reader. Today open does no recovery. It does not load any group, and it does not parse any group file. That removes the two risks of the first draft: a boot that fails on an oversized group (the 2026-09-21 39 GB group, ColdGroupTooLarge) and a tolerant-tail rule that can cut synced data [A9, B1, B10].

Steps (Writer mode only):
1. Take the exclusive flock on journal/LOCK, non-blocking. If another process holds it, fail with 'home in use'. Today nothing locks the home. The socket probe in lastdb-maintenance is not a lock.
2. List the .wal files in order. Check each header. Skip an all-zero header. Start at the file that holds the newest CKPT. Scan the records (magic, length bound, CRC, LSN +1, epoch). Stop each file at prev_end_lsn of the next file if one exists. Apply the torn-tail rule from record_format: a bad record above the last CKPT tail_lsn in the last file is cut with set_len and fsync (the pattern of S/store/shard_load.rs:219-229). A bad record at or below it, or in a sealed file, fails the open and names the file and the LSN. Call the end of the valid prefix E.
3. Build the open periods. Walk the records in order. For each group keep a state: a record with GBASE starts a period (base_seg, base_len). A PUT or DEL adds its line to the period. A GSYNC closes the period and drops its ops. A DROP discards earlier ops of its scope and queues the redo of the drop.
4. Repair each group that still has an open period, with 8 to 16 workers, one group each:
   - Check that base_seg exists and that its length is at least base_len. If not, fail the open and name the group. The skeptic's stale-hint case cannot arise, because every structural change (roll, compaction, direct writers) ends with a GSYNC, and starts with a journaled DROP or DEL.
   - Delete segment files numbered above base_seg (they were created after the base; the journal holds their content).
   - Truncate base_seg to base_len. This removes the whole unsynced tail, torn or not, with no parse. Garbage in the tail is gone.
   - Append the period's lines verbatim, in LSN order. Keep only the final op for each key to save space. Later records win on group load, so this is exact.
   - Fsync the file. Fsync the directory if the group or file is new.
   Replay is logical, not positional. It uses only the journal and the synced length. It never loads the group, so its cost does not depend on group size.
5. Complete each queued DROP (remove the directory if it still exists). Fsync parents.
6. Start a new journal file with a new epoch and first LSN = E+1. Write CKPT(T=E+1). Commit. Unlink the old files except the newest. Fsync the journal directory.
7. Open for writes.

A crash at any point before step 6 repeats recovery from the same journal and gives the same result (truncate-then-append is idempotent). A stale key sidecar is advisory and validated against segment lengths; a truncated or longer segment makes it invalid or tolerated (S/keysidecar.rs read_ids_tolerating_appends), so replay does not rewrite sidecars.

Failure rules. A replay failure (ENOSPC, EMFILE, a missing base segment, a CRC fault in durable data) fails the open and names the record. Skipping a record is data loss and needs Tom. The reader release ships an operator command with --quarantine: it moves the bad frames to journal/quarantine/, logs the lost LSN range, and lets the open continue. Lazy replay at the first group load is not safe, because the directory walkers (hash_groups_on_disk, walk_all_groups) would miss groups that exist only in the journal.

Boot time. Cost is about (groups with an open period) x (one fsync, 5 to 12 ms) / workers. No parse and no cold load. A tail of 1 s at 20 creates/s has about 440 groups: 0.3 to 0.7 s. A tail of 60 s has about 5000 groups: 2 to 8 s. These are guesses [G]. The journal size cap sets the worst case. Add a boot-time check.

Residual risk. Group-file records have no checksum (S/segfmt.rs:1-3). Damage inside synced bytes is not detected today, and the journal does not widen that. A zero-filled hole inside a put body parses as a record with a zero body. A per-record CRC in group files is a format change (an open decision). Garbage in an unsynced tail is no longer a risk, because recovery truncates to the synced length.

Truncation. CKPT(T) must be durable before any file below T is unlinked. If a crash makes an old file reappear, recovery takes the maximum CKPT and ignores older files. A crash between unlinks only repeats work. Checkpoints are serialized. The newest file stays, so the LSN floor survives a clean stop [A11].

Destructive admin ops and direct writers [A4, B9]. These change group files without the append funnel: drop_hash_group_dir, compact_retired_groups, migrate, install_chunk, restore, maintenance_drop_versions (S/store/version_retention.rs:286 calls remove_index and compact_shard with no append). Rule: journal first, then act.
- Per-key removals write DEL records through append, and commit them, before the rewrite.
- Scope removals write a DROP record and commit it before they act.
- After the change, the group is synced and a GSYNC follows.
- drop_hash_group_dir refuses when a pin exists or the registry holds the group. Today it checks only the warm set (S/store/hash_group_admin.rs:128, :190), and a pin now lives until the checkpoint.
- A code audit lists every function in store/ that calls fs::remove_*, remove_index or rename. Each is on a journal-aware list or it fails review.
- delete_namespace is NOT on the DROP list. It runs store.transaction(deletes) (P/storage/laststore/namespaced_store_impl.rs:271-288), which the journal already covers [A4].

Hard erasure keeps the old bytes in journal files until T passes them. A hard-erase run forces a checkpoint, a roll and the unlink of old files before it reports physically erased. The physical audit (scripts/lastdbd/physical_atom_audit.py) scans group files only, so it must also scan journal/ or run on a checkpointed home.

## Durability contract

Three marks, in order:
- APPENDED: the record is in the kernel (the append returned).
- J, journal-durable: a journal fsync covers the LSN.
- D, group-durable: the group files hold it and are synced. The checkpoint publishes D as durable_through.

Contract in plain words:
- C0, process crash (kill -9, panic): every write whose store call returned survives. The record is in the kernel before the call returns, because the pwrite is inside the journal lock section, so no lower LSN can be missing [A1, B8]. Replay restores it.
- C1, power loss or kernel panic: every write with an LSN <= J survives. flush_scope, commit(), transaction and apply_batch (not deferred) return only after J covers them. A bare put, delete or append_for_resident is journal-durable within one commit interval (the next commit, at the latest the 500 ms tick).
- C2, order: the state after a crash is exactly the state after some prefix of the journal. The prefix reaches at least the last LSN that was acked durable. There is no reorder across groups and no gap. This is stronger than today, where a crash inside a batch leaves any subset of that batch's groups.
- C3, group files lag the journal by at most one checkpoint. A tool that reads raw group files (backup, copy, physical audit) must call checkpoint() first. snapshot() already syncs every resident and pinned group (P/storage/laststore/backup_manifest/cut.rs:92-96), so the backup cut stays correct; the open P0 papercut (S0 cut lacks a write fence) is not touched.
- C4, failure: an error from a journal commit or a group fsync stops writes (fail-stop). The caller sees a retryable error and must treat the outcome as unknown. The journal never lets a group file get ahead of it.

What the client ack means [B7]:
- Write mode (acks_on_resident): the ack means the resident memory write, as today (P/.../write/schema_delta_helpers.rs:105-108). It is not durable. The journal does not change this. Tom's rule stays: acknowledge before durable.
- The loss window is the lane backlog: the lane queue plus the running envelope. It is not 500 ms. Today it is 250 to 310 ms when idle and up to about 70 s under overload (docs/lastdb-warm-set-design.md, crash window). The journal shortens it only by draining the lane faster. The contract states the number as the p99 backlog age under a paced load, measured in the kill -9 drill. Until it is measured, no number is promised.
- Stage G1 of the warm-set design (refuse when the oldest unwritten ack is older than an age bound, proposal 5 s) belongs in the same card as the writer release. With it the window is at most the age bound plus one commit interval.
- Off and read mode, force_durable, synchronous: true and LASTDB_MUTATION_SYNC_FLUSH: the request waits for its lane envelope. That wait ends at J, not at the group fsync. The promise against power loss is the same as today. The Durable receipt (P/.../mutation_manager/receipt.rs) means J.
- Process loss of an acked write that the lane has not yet written to the store is still possible in every mode that acks from memory. The store journal sits below the lane and cannot see it. Tom's model taken literally (apply to memory, ack, then log the mutation, batched) is a logical intent journal at the ack point. It is a separate tier and an open decision. The store journal alone does not meet it.

What replaces the old promise 'every group file is current after flush': it is gone. commit() gives recoverable. checkpoint() gives group files current. Hash group files can lag, so any new reader of raw files must be classified.

Rename plan for the 163 callers of flush() in 86 files (rg count at HEAD; the earlier inventories said 116, 25 and 170) [B16]: rename the function so the compiler lists each caller once. The default for a new caller is commit(). Hot callers become commit() (put_typed_durable, molecule_key_store.ensure, the capture marker, flush_dirty_scope, finalize_batch). The tick, shutdown, backup cut, snapshot, compaction, atom retirement, hard erase, drop group, delete namespace, the S0 cut, the restore barrier and the maintenance CLI become checkpoint(). A per-file list of the 86 files goes into the writer card. A caller in the wrong class either reads stale group files or stalls a hot path, and nothing but review finds it. Do not leave a silent flush() with a new meaning.

LASTDB_TXN_DEFERRED_FLUSH has no use after the journal. Retire it. A no-wait variant is append and return with no commit wait. It is kill -9 safe and not power-loss safe.

Proof limits [B11]. kill -9 keeps the page cache, so it cannot show fsync order or power-loss safety (C1, C2). Group files have no LSN and no checksum, so a WAL breach cannot be found from the files. Power-loss safety rests on code review of the commit path and the WAL rule, on the wal_violations counter (must stay 0 in every drill and bar run) and on the debug assertion in spill_open. Whether Tom accepts that, or approves fault hooks, is an open decision.

## Effect on the lane, dirty-clear and tokens

DurabilityToken = LSN. The journal assigns it for every store write, under the journal lock, inside the shard lock. put, delete, compare-and-swap and transaction now get one too. This lets the checkpoint seal cover maintenance writes, so a truncation cannot drop their records. The LSN floor is persisted (see record_format).

Marks: APPENDED (tail), J (journal_durable_through, new), D (durable_through, group-durable). WriteAck.covered_by_file_len has no product consumer (only the declaration and one read in P/resident/logical_set/durability.rs). Set it false.

What each consumer keys off:
- Persist turns and graph dirty-clear (complete_resident_persist_turn, clear_deferred_job_dirty_after_success, P/.../persist/deferred_job.rs:25-26): stay per envelope, at APPLIED (the envelope changed store memory). They order in-memory state and keep graph and cache coherent (P/resident/graph/slots.rs:35-111, purge.rs:51-62). They are not a crash-safety device. A graph purge of a tip that is not yet journal-durable is read-safe, because the store pin still holds the bytes. This is option X. It changes what dirty-clear means, so it needs Tom's yes.
- Completion senders, after_release hooks, counts_completion, the Durable receipt, the off and read mode ack, DeferReservation drop and the search outbox file delivery: move to J. write_batch commits, then returns Released.
- The quarantine record (put_typed_durable, lane_writer.rs:88-135) must commit before the turn completes. Otherwise a crash can lose the only copy of a poisoned envelope.
- Warm-set Dirty tokens (P/resident/logical_set/durability.rs:31-83): clear at D, as today. This keeps Tom's rule that a purge never drops an unflushed record in its literal form. The pin holds the bytes in store memory until D, so a re-fetch is correct either way.
  - Risk of D [B3]: the 5000-token refusal then limits the burst to about 125 creates per checkpoint (about 40 tokens for each create), and the pin cap limits it more (see expected_throughput). Clearing at D turns that refusal into the throughput limit.
  - Option J: clear at J. A purge after J is safe because the pin still holds the record. This lets the checkpoint interval grow. It changes the literal meaning of flushed. Tom decides, and the decision comes BEFORE the writer release, not after a soak.
  - Never clear at 'spilled but not synced'. Spill implies fsync in this design, so that state does not exist.

Option Y (persist-turn completion after the batch fsync) deadlocks across lanes. Tickets include protein-sibling slots (P/.../molecules/apply_gate.rs:351-361). Lane A envelope a2 can wait for lane B envelope b1 while b2 waits for a1. With batch-level completion neither batch can reach its commit. Y is safe only if write_batch commits and completes its applied prefix before it blocks on any turn wait. Not recommended.

Lane batch size [B15]. write_batch runs envelopes one after another, each with a cost c (8 to 40 ms [G]). With one commit at the end, the first envelope of a batch waits about B x c plus one commit before its completion fires. The lane batch is capped by count and time (proposal: commit after 4 envelopes or 30 ms). The durable-ack latency is quoted as a function of B, not as a single number.

A failed commit after turns completed: a Retry would hit 'predecessor already passed'. A commit failure is therefore fail-stop (C4), not a retry.

Author clock. The barrier changes from 'flush done' to 'record appended' (P/.../author_clock.rs:141-155). Log order then puts the clock before the data, and the lane's single commit covers it. Keep the PendingTask until a commit covers it, for shutdown. Without this change the off-mode ack pays two serial commits.

Pins. A dirty pin lives until D (the group sync), not until the barrier. Reaping changes from 'dirty_ops == 0 at the barrier' to 'synced at the checkpoint'. Steady-state memory is one Shard for each dirty hash group. On real data a tips group is 7 to 8 MB and an atoms sidecar covers 5.2 MB (S/keysidecar.rs:6-8, docs/lastdb-warm-set-design.md:184), so one create can pin 20 to 60 MB [G]. The pin cap forces a checkpoint.

## Effect on the GC reaper, delete barriers, sync and backup

Atom GC reaper. The reaper refuses to run unless batch_put_has_durable_barrier() is true (P/db_operations/atom_store/atom_ref_gc/reap.rs:19-31). In journal mode the property stays true, because a call outside a lane scope commits before it returns. The re-arm, the body and the tip of one job get increasing LSNs. Recovery gives a prefix, so a tip never exists without its body and its re-arm. This is stronger than today, where hash groups inside one batch have no durable order. The reaper's own delete batch is one commit. The lane scope never covers the reaper (the lane scope is task-local); a debug assertion checks it. Skeptic A found no path that deletes a live atom, and checked the batch op order (transitions.rs:55-71) and the re-arm order (atoms/write.rs:205-231). After a GC delete and compaction, the bytes also sit in journal files, so space and erasure proofs need a checkpoint and a roll. LASTDB_TXN_DEFERRED_FLUSH already turns the reaper off. Retire it.

Delete barriers (rdel:v2). The barrier record must be durable before the tip removal batch (P/fold_db_core/purge/converge.rs:44-70). The record write plus a commit does that with one shared fsync. The pending-barrier map clears after db_ops.flush(), which becomes a commit. Recoverable is enough for this clear. The ack-to-lane gap in the map is not shortened by the journal.

Hard erasure and purge envelopes. The delete is journal-durable after the commit. It is not physically erased until the checkpoint passes it and the old journal files are unlinked. The status shows two levels: logical erase (delete record committed) and physical erase (gone from groups and from the journal). Hard-erase meters and journals (hard_erase_journal.rs) keep their intent-before-delete and commit-after-delete order. The LSN order now proves it, but the code does not read LSNs yet.

Protein folds and sibling tips. The sibling tip batch is the second batch of the envelope (deferred_job.rs:107-150). LSN order places it after the main tips. A crash can leave main tips without sibling tips, as it can today. After a crash the lane queue is gone, so this gap is not new. Atomic envelopes would close it, but only with parent closure across lanes. Not in the first versions.

Cloud-sync capture. The mutation log stays a sync outbox. It cannot replace the journal. Lane and replay writes are capture-suppressed (deferred_job.rs:21-27). The log holds one MutationIntent for each request. A device skips its own entries on download. It cannot rebuild atoms, molecules, side records, GC records or schema records. The capture marker (a pointer, values stripped) and its flushes become commits, so they get cheaper. Order is data first, then the marker, so a crash leaves data without a marker (re-exportable), never a marker without data. The capture worker is still one serial worker.

Backup and snapshot. The backup cut calls store.snapshot() and then reads the numbered group files (cut.rs:92-96). snapshot() syncs every resident and pinned group (skeptic B checked store/compact.rs:6-18 and :116), so the group files hold everything. The journal adds a CKPT. The journal is not in the backup. A restore needs no journal.

Restore and live bulk writers [A5, B9].
- Offline restore (the destination is a fresh home): Restore mode, journal off. restore_put_many_deferred writes groups directly and ends with flush_restore_parallel. The destination has no journal/ directory (delete any old one first). The descriptor stays version=1 until the first journaled write after the restore opens. A restore larger than the journal cap works, because nothing is journaled.
- Live bulk writers on a running store: cloud purge row copy (sync/cloud_purge/row_copy.rs:37), blob copy (blob_copy.rs:50), cloud share refresh import (sync/engine/cloud_share_refresh/import.rs:90), inbound share data (sync/cloud_purge_peer/inbound_share/data.rs:72) and receivers (receivers.rs:66, :123). They write through the normal append, so they ARE journaled. A skip would let a stale journal record replay over a newer imported record. They commit in chunks. At the soft cap they wait for a checkpoint; they are never refused at the hard cap. Their large records pay the double write (see the large-record decision).
- The seven call sites join the destructive-admin list for review.

Shutdown, adoption and stopped copies. The last shutdown step is checkpoint(). The shutdown receipt keeps its bytes and its schema: ShutdownFlushReceipt has deny_unknown_fields and version == 1 (lastdb_node/src/ops/session_ledger/receipt.rs:8-24), and older readers (safe-upgrade strict stop, adoption, backup markers) would reject a new field [B13]. flush_ok becomes true only after a checkpoint and an empty journal. New tools also check journal/ themselves. The cloud-purge adoption copies data/ and a fixed sidecar list (lastdb_node/src/cloud_purge_adoption/switch/files.rs:7-60), so it relies on that receipt.

Keyed (frame-AEAD) homes. Not supported in this version. They keep the barrier path.

## Migration and rollback

No data migration. Group files do not change. The journal is a new additive file class under <root>/journal/. The storage-format rule applies, so Tom must decide it. The plan is a dual-read plan with a reader release first, so a way back exists before any journal is written [B1].

Fence. The layout descriptor reader ignores unknown keys and rejects only version != 1 (S/store/layout.rs:219-227). A new key is not a fence: an old binary would open the home and silently drop the journal tail after a crash. So the first journaled write after open rewrites the descriptor to version=2 (a new crash-safe rewrite path is needed, because S/store/layout.rs:291-294 never rewrites). An old binary then refuses the home with 'unsupported layout descriptor'. That is a loud failure, not a quiet loss. Every caller of describe_home must read version 2 in the reader release: backup_markers.rs:148, restore_guards.rs:13, status_ops_cmds.rs:308, cloud_rescue_publish/plan.rs:37, S/store/layout_restore.rs:22 and the open path.

Two releases, in this order:
1. Reader release. It contains: the journal reader, replay at open (as in recovery_and_truncation), the descriptor accepting versions 1 and 2, the open modes, the journal-disable command (offline, stopped home: replay, checkpoint, delete journal/, write version=1) and the --quarantine command. It never writes a journal and never changes the descriptor. It ships through lastdb-safe-upgrade and soaks on the primary. It is the rollback target for the writer release. Replay never blocks boot: it loads no group, so an oversized group cannot stop it.
2. Writer release. It adds the journal writer, the WAL rule and the checkpoint, behind LASTSTORE_JOURNAL=off|shadow|on. Default off. The launch setting that turns it on counts as a version change and goes through lastdb-safe-upgrade with its own gate (an open decision for the skill). Before the cutover, the journal-disable command must have been run on a synthetic seed that was killed under load.

Switch values:
- off: the store writes no journal. If a journal exists, the store replays it, checkpoints, deletes it and writes version=1 back. So off is always safe.
- shadow: the journal is written with no fsync wait, barriers stay as they are. Boot replay runs in report mode (counts what the groups lack, writes nothing). Shadow doubles write bytes, so it runs on synthetic homes only unless Tom decides otherwise. Gate numbers: wal_violations = 0 over N kills, journal bytes at most 2 times the group bytes, write p50 within baseline +2 ms.
- on: the journal is the barrier.

Rollback:
- on to off, clean stop: the shutdown checkpoint leaves the journal empty and the receipt says so. The descriptor goes back to version=1. Any older binary can open the home.
- on to off, after a crash: the reader release replays at open, checkpoints and deletes the journal. Run it once, stop it cleanly, then roll back.
- on to a binary older than the reader release: allowed only after the clean stop above (flush_ok true and no journal records). Add this check to the lastdb-safe-upgrade rollback step. The saved binary pair at ~/.claude/skills/lastdb-safe-upgrade/SKILL.md:645 and :721 must be the reader release.
- After a crash on version=2, a binary older than the reader release refuses the home. This is intended.
- A crash plus a run on an old binary plus a later upgrade would replay a stale journal over newer data. The version=2 fence prevents that run.

Layout group reduction (the first lever) has its own rollback. It changes key layout, so it follows the atom key encoding precedent (a marker in the home, an env override, a boot gate: P/db_operations/atom_store/key_encoding.rs). Reads check the new keys, then the old keys, and a background drain moves old records. Once the old records stop being written, an old binary reads the new keys as absent. So rollback after the soak is a reverse drain tool, or a restore from backup. Writing both layouts during the soak would bring back the groups the change removes. Tom must accept this one-way step.

New operational checks for safe upgrade: kill -9 the candidate on the synthetic seed, reboot, and check that every journal-durable key is present. A crash-consistent clone of a running node stays out of scope (Tom's rule: no copy of the primary). Replay on real group sizes cannot be rehearsed on a copy of real data; append-only replay (no group load) lowers that risk.

## Expected throughput and latency

All new numbers are derived from measured inputs and marked [G] (guess). Measured inputs [M]:
- After #167 and before the sidecar flush rule a create costs 22.2 data fsyncs and 6.5 sidecar fsyncs, 28.7 F_FULLFSYNC (2220 and 655 for 100 creates). It drains at 5 to 6 creates/s (100 creates in 17 to 20 s), about 6 to 7 ms for each barrier.
- One barrier costs 5 ms alone and about 12 ms inside the loaded lane. Sixteen in parallel cost 2.1 ms each (2.5x).
- Parallel scope flush gave 1.6x before #167. The default threshold is 64 groups (S/store/flush_config.rs:5), so a scope of 6 to 17 groups syncs serially today. Tick off gave no gain.
- Cold parse of real groups is NOT measured: 10 to 40 ms for each group [G]. The primary showed a 3.1 s gate hold for 24 cold loads (docs/lastdb-warm-set-design.md:370).

Ceiling rule [B2]. The device pays one barrier for each distinct dirty group, whoever issues it. The ceiling is (barriers per second) divided by (barriers per create). The journal does not change barriers per create. Group reduction does. At 3 to 6 creates/s, a 500 ms interval holds 1.5 to 3 creates, so almost nothing coalesces (21.9 distinct groups for each create at 10 creates in the model).

Ladder for one schema on tiny synthetic groups [G]. Each step adds to the one before:
1. Now: 5 to 6 creates/s.
2. Knobs only (LASTSTORE_PARALLEL_FLUSH_MIN_GROUPS=4, 8 workers): 8 to 10 creates/s. This is the baseline any journal must beat.
3. Sidecar flush rule (built 2026-10-14: no forced write for a group under 1 GiB): removes up to 6.5 barriers on every home whose groups are under 1 GiB: 10 to 13 creates/s [B6].
4. Group reduction, 22 to 14-18 groups: 12 to 18 creates/s.
5. Journal, one commit for each KV batch: the lane pays 6 commits at 5 to 12 ms plus the lane cost c. With c = 8 ms: about 44 ms, 23 creates/s. With c = 40 ms: about 76 ms, 13 creates/s.
6. Journal plus a lane commit for B = 4: the lane pays c + commit/B, so c bounds it: 25 to 90 creates/s on the lane.
Sustained rate with the journal is set by the checkpoint: 500 barriers/s (5 ms, 16 parallel) divided by 16 groups is 31 creates/s. 210 barriers/s (12 ms) divided by 16 is 13 creates/s.
Result [G]: the journal adds about 1.0x to 2x over steps 2 to 4 for sustained creates. It mainly cuts lane latency and the durable-ack wait. Group reduction multiplies both the lane and the checkpoint. This is why group reduction goes first.

Limits on the checkpoint interval on real data [B3]:
- The 500 ms rule (open decision).
- The 5000-token refusal: about 125 creates.
- The pin cap: one create can pin 20 to 60 MB on real groups (tips 7 to 8 MB, atoms 5.2 MB). A 512 MiB cap forces a checkpoint after 9 to 25 creates. At that size coalescing is near zero.
- The 128 MiB soft journal cap and the boot time.
So the 10 to 35 creates/s range of the first draft is withdrawn. Expect 9 to 23 creates/s at the best, and only when groups are small. Real-size groups lower it.

Cold parse [B4]. After fsync leaves the lane, the lane is bound by cold group parse and compute (c). The checkpoint reaps clean pins, so the next write to each group parses it again. At 10 to 40 ms for each of about 14 scattered groups, c can be 140 to 560 ms, and the rate returns to 2 to 7 creates/s. The journal does not reduce cold loads; group reduction does (fewer groups to load). The existing bars cannot show this: the latency bar updates one seeded record and says 'Synthetic data is tiny'. Experiment 0c measures the zero-fsync lane time. If it is 100 ms or more on real-size groups, neither the journal nor the reduction reaches 10 creates/s, and the lane compute is the target.

Directory barriers [B5]. A new directory costs one F_FULLFSYNC on Apple. A fresh synthetic home creates most of its 22 groups new, so 100 creates can add 2000 or more directory barriers [G]. That is the same order as the data barriers. Report fresh-home and mature-home numbers separately. A fresh home overstates the checkpoint cost.

Latency:
- Write mode ack: unchanged, 6 to 9 ms.
- Durable ack (force_durable, off or read mode): reported 440 to 670 ms, or 250 to 310 ms in off mode. Commit per KV batch: 40 to 100 ms [G]. Commit per lane batch: the first envelope waits about B x c plus one commit. B = 1 gives 15 to 50 ms. B = 4 gives 40 to 170 ms [G] [B15].
- Loss window of a write-mode ack: the lane backlog age. Today up to about 70 s at overload. After the lane step: tens of ms to about 1 s when the lane keeps up. Under overload: 512 entries divided by the lane rate (5 to 20 s at 25 to 100 creates/s), unless the age bound (proposal 5 s) refuses writes earlier.
- A closed loop of 6 clients acks in 6 to 9 ms, so it offers about 700 creates/s. No design drains that. The 512-entry queue still fills and persist_queue_full still appears. The proof cell must be paced (20 creates/s for 30 s) and expect zero refusals.

Disk and memory cost of the double write:
- Every record is written twice: once in the journal, once in the group file. Bytes double. At 10 KB for each create and 10 creates/s that is 17 GB/day of extra writes; 100 creates/s of a 100 KB schema is 1 GB/min.
- The journal write is a sequential append. The cost that matters is the fsync, and the journal pays it once for many writes.
- Group files do not coalesce bytes. A hot group that gets 50 puts of one key keeps 50 records until compaction. Only the fsync count coalesces.
- Journal live size = rate x bytes per create x (interval + checkpoint time). 20 creates/s x 10 KB x 3.5 s = 0.7 MB. 100 creates/s x 100 KB x 10 s = 100 MB. Soft cap 128 MiB forces a checkpoint. Hard cap 512 MiB refuses writes with a retryable error. Control records and bulk writers are exempt (bulk writers wait).
- Dirty pin memory = (dirty groups) x (pin size). The pin cap forces a checkpoint.
- The bottleneck moves to the checkpoint, then to cold group parse. Measure both before any promise.

## Comparison with the alternatives

1. Group reduction (the lever that remains in docs/lastdb-warm-set-design.md; FIRST, per Tom's request). A group file exists for each (collection, shard, hash group). Two records share a barrier only if they share the collection AND the partition string. The product opens with HashGroupKey::PartitionPrefix and fanout 1 (P/fold_db_core/factory/local.rs:399-451). The partition is everything up to the first NUL (S/store/placement.rs:185-205). Routing of key prefixes to collections: P/storage/laststore/key_routing.rs:7-60.
   Already shared: the v2 atom edge and live count (aref:v2:e:{atom}NUL... and NUL!live-count): one group for each atom (P/db_operations/atom_store/atom_ref_edges.rs:35-38, :223-230).
   Not shared (read from the key builders; Experiment 0b confirms or kills each):
   - Tip pointer mgp:v1:{M} (no NUL, hashed whole) and generation record mgr:v1:{M}:{gen}:{hash}NUL... (both in tips): two groups for each field (P/atom/molecule_key_codec/generation_keys.rs).
   - Atom body (atoms collection, partition of its tip) and tip record (tips collection): same partition string, different collection, so different group.
   - Locator aloc: (atom_locators), GC candidate aref:gc:v1:c: and pending aref:pending:v1: (atom_ref_edges).
   - Fixed per write: metadata 1, schema index 1, idempotency 1, change feed 2, locator 1.
   For the 4-field synthetic schema: 4 groups for each field + 6 fixed = 22. Candidates:
   - M1: put the tip pointer in the partition of the generation record. About 4 groups fewer.
   - M2: route atom bodies into the tips collection under the same partition. About 4 groups fewer, but tips groups grow by atom bytes (up to 256 KiB each), cold parse slows, and it touches decision 15 of the warm-set design (atom placement).
   - M3: move locator, GC candidate and pending into the v2 atom partition. 0 to 1 group fewer.
   - M4: skip-equal-put checks on the fixed groups (the #167 pattern; store_schema already skips an equal body, P/db_operations/schema_store.rs:224-244). 1 to 2 groups fewer, no format change.
   - M5: write the edge and count records without a barrier. About 4 barriers fewer. They are rebuildable, but the GC safety depends on the reachability audit gate. Needs review by the GC owner. Not claimed here.
   Estimate [G]: 22 to 12-18 groups, 20 to 45 percent fewer barriers. The no-format-change floor is about 3 groups per field + 4. M1 to M3 change key layout (dual-read, marker, one-way after the soak). M4 does not.
   With the journal, reduction lowers the checkpoint cost and the pin memory. Without the journal, it lowers the lane cost. It works either way and does not wait for the journal.

2. Status quo plus trims (#167 and earlier). Required with the journal too. Alone: 5 to 6 creates/s (measured), loss window unchanged, 6 clients still hit the queue cap.

3. Parallel fsync only (knobs). Measured 1.6x on the drain, 2.5x raw at 16 workers. The default threshold of 64 groups keeps it off. It is the fair baseline for the journal [B2]. Use it as the checkpoint engine too.

4. LASTDB_TXN_DEFERRED_FLUSH (skip the barrier, no journal). It drained 21 writes/s, but a kill -9 right after the drain lost 92 of 150 acked writes, 82 after 1 s, 18 after 3 s, 0 after 8 s. It turns the GC reaper off. The loss comes from records that stay in open_buf (user memory) until the serial tick reaches each group. The journal supplies the missing part: a record is in the kernel when the append returns. It also gives the zero-fsync lane bound for Experiment 0c.

5. Cloud-sync mutation log as the local journal. Rejected. It records logical intents, one for each request. Lane writes run with capture suppressed. Replay needs the whole mutation path. The log does not exist when sync is off. Own entries are skipped on download. Its worker does about 6 flushes for each mutation. Keep it as an outbox.

6. Journal above the store (a KvStore wrapper). Rejected for production. It misses direct LastStore users, cannot gate spill and purge, and cannot take the LSN inside the shard lock, so it cannot give prefix closure across lanes. Use only as the prototype in Experiment 0e.

7. Batch frame for each lane batch or apply_batch. Rejected for this version. It needs the frame to be written after the memory writes, which breaks per-key order across lanes. It gives batch atomicity that no caller needs. A later step can add atomic envelopes only with parent closure.

8. Logical intent journal at the ack point (Tom's model, taken literally). Apply to memory, ack, log the mutation in a batch. It shrinks the loss window to one commit even with a deep queue, and the lane cost c is not on the ack path. It needs an intent codec and a replay through the mutation manager (the cloud-sync replay path is a base). It is large. It does not raise the sustained rate: the lane still writes the groups, and an unbounded backlog needs an age bound anyway. Treat it as a separate tier. The store journal is its base layer.

9. Record-level atom and tip pending map (memory overlay so the lane need not load groups). It needs every read path to overlay the map, and it needs the journal for crash safety. Combine later.

10. Replace the storage engine (LSM with WAL). Too large, and it changes the format.

11. Stage-synchronous lane batching without a journal (run stage 1 for all envelopes, one barrier, stage 2 for all, and so on). It keeps the atoms-before-tips order and shares only the fixed groups (about 5 of 22). It has the cross-lane ticket risk of option Y. Gain about 20 percent at B = 16 [G]. Not planned.

Per-op record versus per-batch frame: per-op chosen. Journal in laststore versus above: laststore chosen. Spill without fsync as memory relief: rejected, because unsynced pages can leave garbage in the middle of a group file (S/store/shard_load.rs:180-250 cuts only a torn tail). Keep 'spill implies fsync'.

SKEPTIC DISPOSITION (A = crash lens, 13 items; B = fsync and plan lens, 17 items):
A1 LSN hole, fatal | real | fixed: pwrite inside the journal lock; LSN advances after the pwrite; no hole can exist.
A2 fallback puts groups ahead of the journal | real | fixed: no fallback to group files; tail ring and re-append; fail-stop.
A3 retried fsync clears dirty | real | fixed: poison group, T held, exit after retries; macOS behaviour untested; exit-or-refuse is an open decision.
A4 direct mutators resurrect data | real for retention and drop_hash_group_dir; NOT real for delete_namespace (a transaction of deletes) | fixed: journal-first rule, audit list, refuse on pin.
A5 restore through the journal | real | fixed: Restore mode with journal off; live bulk writers journaled and wait at the cap.
A6 directory flag lost | real | fixed: separate dirs_pending set that holds T.
A7 journal before fallible steps | real | fixed: journal right before extend; threshold sync removed; commit error is fail-stop; no undo.
A8 journal can wedge | real | fixed: control-record headroom; Drop guard; online replay of orphan entries. The early-return path does not trigger in plain mode.
A9 torn-tail rule unbounded | real | fixed: truncate to the synced length (GBASE and GSYNC); CKPT bound for the journal.
A10 funnel gaps (sorted, encrypted, static fns) | real | fixed: refuse non-plain descriptors; Arc to journal in Shard; wal_violations.
A11 LSN origin and file-set edge cases | real | fixed: LSN floor in newest header, max CKPT, serialized checkpoints, all-zero header unused.
A12 CRC polynomial | real | fixed: CRC-32/IEEE, named.
A13 registry key | hardening gap | fixed: (ShardKey, generation id).
B1 no safe exit, fatal | real | fixed: reader release first; replay loads no group; journal-disable and quarantine commands; safe-upgrade step is an open decision.
B2 total fsync work unchanged | real | accepted: the gate is the sum of F_FULLFSYNC; the baseline is the knob run; group reduction goes first.
B3 sustained rate limits | real | accepted: rate recomputed; clear-at-D versus J decided before the writer release.
B4 cold parse hides the gain | real, not measured | Experiment 0c; real-size seed is an open decision.
B5 directory fsync is a full barrier | real (code and std) | accepted: counted in the sum; fresh and mature numbers apart.
B6 sidecar without fsync | real | fixed 2026-10-14: keep the sidecar and its rename; skip the forced write for a group under half the cold-load cap (1 GiB); keep it for bigger groups, which may be too large to load. Residual: a group that grows past the floor after its last small sidecar write can lose that sidecar in a power cut.
B7 ack window is the lane backlog | real | accepted: the contract states it; G1 age bound in the same card; kill-under-load drill; intent journal is an open decision.
B8 C0 false across threads | same as A1 | fixed.
B9 live bulk writers | real | fixed: journaled, chunked, wait at the cap; large-record bypass is an open decision.
B10 tolerant tail removes loud failure | same as A9 | fixed.
B11 proof set and harness | real | fixed: drills cut to kill -9, file surgery and a small disk image; crash and fault hooks moved to an open decision.
B12 stages do not ship alone | real | fixed: plan re-cut with dependencies.
B13 hidden format changes | real | fixed: receipt bytes unchanged; journal created lazily; list goes to Tom.
B14 locking changes tool behaviour | real | fixed: open modes; per-store counters.
B15 durable ack latency for B > 1 | real | fixed: lane batch cap; latency quoted by B.
B16 flush() review size | real | fixed: 163 callers in 86 files; default commit().
B17 fsync under shard lock; T1 and T2 not gates | real | fixed: gates before code; fsync outside the lock in the writer release.

## Staged plan

- Step 0, Experiment 0 (cheapest, first, 1 to 2 days, no format change, no merge to main). Run it on a fresh synthetic home from scripts/lastdbd/lib/synthetic-node.sh. Never copy or open ~/.lastdb. The bars take extra env after a double dash (resident-latency-bar.sh --bin PATH -- ENV=VAL). Use a throw-away loop in the scratchpad for 100 paced new-key creates, and read memory_budget.fsync_*, groups_synced and flush_barriers from /api/status before and after. Parts: 0a no code: baseline run, then the knob run (LASTSTORE_PARALLEL_FLUSH_MIN_GROUPS=4, LASTSTORE_PARALLEL_FLUSH_WORKERS=8, LASTSTORE_PARALLEL_WHOLE_FLUSH=1); report creates/s and F_FULLFSYNC per create = data + sidecar + directory. 0b prototype switch LASTSTORE_TRACE_SYNC=1 (observation only, about 30 lines in sync_open): log collection, shard, group, bytes spilled, fsync microseconds and the caller kind for each group that had dirty ops. One create and 100 creates give the exact list of the 22 groups by collection. This confirms or kills candidates M1 to M4. 0c no code: LASTDB_TXN_DEFERRED_FLUSH=1 gives the zero-fsync lane time per create on tiny groups (a lower bound; do not set it on the primary). 0d microtrials T1 and T2 with a scratchpad Python script (fcntl F_FULLFSYNC), on the same disk. 0e only after 0a to 0d, and only if the journal is still wanted: prototype switch LASTSTORE_JOURNAL_PROTO=1, about 100 throw-away lines: under one mutex, pwrite each plain-path record to one file, make the flush funnel fsync only that file, keep the 500 ms tick as is. No replay, no checkpoint. It measures the drain rate with 6 shared commits for each create and the journal bytes for each create. Its kill -9 check counts records in the file against the oracle (it shows C0, not recovery). Proposed gates [G, Tom may change them]: do the layout reduction if 0b shows 5 or more of 22 groups removable; stop the rate work and target the lane compute if 0c is 100 ms or more for each create; build the journal only if 0e drains at least 2 times the 0a knob run, reaches 20 creates/s paced with zero refusals, and T1 holds. Output: one list of numbers. Clean up: stop the node, keep the receipts, delete the scratch home.
- Step 1, trims with no format change. Each has a switch and can be undone. (a) Make parallel scope flush the default from 4 groups (measured 1.6x; keep the EMFILE reclaim). (b) Sidecar flush rule (BUILT 2026-10-14, replaces "no sidecar under about 1 MiB"): keep the sidecar; skip the forced write before the rename for a group under half the cold-load cap; keep it for bigger groups; later, batch those flushes at the checkpoint. The existing skip for a one-block group of 4096 bytes or less (pins.rs) stays. A kill -9 drill cannot show this is safe against power loss; the card says so. (c) Skip-equal-put checks on the fixed groups that 0b names (the #167 pattern). (d) Close the open P1 papercut: a retry that clears dirty without an fsync; poison the group on a failed fsync. (e) Gauges: F_FULLFSYNC per create (data + sidecar + directory), groups by collection, oldest unsynced age, pin bytes. (f) Fix the stale comments at P/resident/lane.rs:3-4 and S/store/pins.rs:3-8. Do NOT add a dirty registry or a parallel tick here: tick off gave no gain, so they pay off only inside the checkpoint. Proof: the two bars in 3 modes, 3 runs each, inside the baseline; the 100-create run shows creates/s and F_FULLFSYNC per create against the 0a knob run; the kill -9 drill shows no regression. Rollback: unset the switch.
- Step 2, layout group reduction (Tom decides; format change). Build only the candidates that 0b confirms, in order of groups removed for the least risk: M4 first (no format change), then M1, then M3, then M2 only after Tom answers decision 15 of the warm-set design. Use the atom key encoding precedent: a marker in the home, an env override, a boot gate that refuses a mismatch, dual-read (new keys, then old keys), and a background drain. Read the GC doc first: the reaper order (re-arm, body, tip) and the count gate must hold in the new layout. Proof: groups for each create from the 0b trace fall from 22 to the target; bars; 100-create run; kill -9 drill; the GC reaper still refuses to delete a live atom (the oracle graph: every tip has its atoms). Rollback: reverse drain tool or restore; no dual-write window (it would restore the removed groups). Tom must accept the one-way step.
- Step 3, go or no-go for the journal. Re-run Experiment 0 (0a, 0c, 0e) on the binary that has Steps 1 and 2. Compare against the gates. If the lane is at or above 20 creates/s paced with zero refusals, do not build the journal now. If the journal is still wanted, Tom decides: clear Dirty at D or J, the 500 ms rule under load, the large-record rule, the exit-or-refuse rule, and the storage-format class. These come BEFORE Step 5.
- Step 4, reader release (no journal is written). Contents: journal reader, replay at open, descriptor versions 1 and 2, open modes (Writer, ReadOnly, Restore), per-store counters and parallel permit, journal-disable command, --quarantine command, lock on the journal directory, flush_ok true only with an empty journal. It ships through lastdb-safe-upgrade and soaks on the primary with the journal absent. Proof: bars flat; open of a synthetic home with a hand-made journal replays it (file surgery on a stopped home); disable command works; an old binary refuses version 2. Rollback: the previous release.
- Step 5, writer release, commit for each KV batch. Contents: journal.append in append_with_threshold (pwrite inside the lock), LSN = DurabilityToken, commit_through, flush_scope = commit, WAL rule in spill_open with Arc to the journal, dirty registry and dirs_pending, checkpoint with GSYNC and CKPT, GBASE periods, recovery by truncate and append, DROP and DEL for the direct writers, fsync outside the shard lock, poison on fsync error, flush() renamed to commit() and checkpoint() (163 callers, 86 files), snapshot as checkpoint, shutdown checkpoint, restore in Restore mode, the live bulk writers chunked, descriptor rewrite to version 2 at the first journaled write. Switch LASTSTORE_JOURNAL=off|shadow|on. First run shadow on synthetic homes only. Proof: bars in 3 modes x 3 runs inside the baseline; the fsync gauges show 6 journal commits and 0 sidecar fsyncs for each create on the lane path, and the SUM of F_FULLFSYNC for each create; wal_violations = 0 in every run; the kill -9 drills in kill9_and_fault_drills. Rollback: LASTSTORE_JOURNAL=off plus one clean boot on the reader release.
- Step 6, one commit for each lane batch. Contents: the task-local lane scope, apply_batch skips its commit inside it, write_batch commits once at the end (and commits the finished prefix first on a failed stage), completion, receipts, after_release hooks, search outbox delivery and the quarantine record move after the commit, the author clock barrier means record appended, the lane batch cap (4 envelopes or 30 ms, proposal). Persist turns and graph dirty-clear stay per envelope (option X, Tom's yes). Switch LASTDB_LANE_COMMIT=batch|stage. Proof: bars; the paced cell (30 s at 20 creates/s, persist_queue_full = 0); fsyncs for each create near 1/B on the lane; durable-ack latency quoted by B; kill -9 with 6 writers; the oracle graph holds after each recovery. Rollback: LASTDB_LANE_COMMIT=stage.
- Step 7, admission and tuning. Journal bytes, dirty pin bytes, dirty groups and oldest unsynced age feed reserve_persist_capacity (a retryable PersistQueueFull with a new kind). The age bound of Stage G1 (proposal 5 s) ships here or earlier. Pins kept by age and memory, not by clean. Adaptive interval max(500 ms, last checkpoint). Optional clear of Dirty at J if Tom decides. Proof: commit p99 during a checkpoint under 3 times one fsync; pin bytes stay under the cap in a 10-minute paced run; boot time at the journal cap under 20 s.
- Step 8, optional, separate decisions. A logical intent journal at the ack point (closes the ack window). Atomic envelope frames with parent closure. A per-record CRC in group files (dual-read format change). A group id dictionary in the journal. Frame-AEAD homes. A large-record bypass. Barrier-free edge and count records (M5) after review by the GC owner. Each needs its own design and Tom's yes.

## Kill -9 and fault drills

Rules for all drills: use a fresh synthetic home from scripts/lastdbd/lib/synthetic-node.sh. Never copy or open ~/.lastdb. Keep an oracle file with every acked id and fsync it after each ack. Use 1 and 6 writers with create, update and delete. After the drill, stop the node, keep the receipts, and remove the scratch home. These are operational drills with kill -9, file surgery on a stopped home, a small disk image and the existing gauges. No test code and no fixture harness is written [B11]. Crash-point env knobs, an fsync trace with truncate emulation and fault injection are NOT in this plan; they are an open decision.

Pass rules:
- Every write acked with force_durable survives at every kill time.
- Every write acked in write mode survives if the lane finished it before the kill (read the lane depth gauge before the kill).
- After the lane drains, loss is zero. This is the case that lost 92 of 150 with LASTDB_TXN_DEFERRED_FLUSH.
- Under load, the loss is at most the lane backlog (drill 2). The first draft's rule hid this case [B7].
- The recovered store is a prefix: every tip has its atoms, and no record exists whose parent is missing (read every oracle id and its atoms).
- A second boot changes nothing.
- wal_violations is 0 in every drill.

Drills:
1. Kill -9 at 0, 50, 200, 1000, 3000 and 8000 ms after the last ack, with 1 and 6 writers. Same curve as the 2026-10-12 deferred-flush drill. Expect 0 loss after the drain at every point.
2. Kill -9 under load (the backlog case): paced 20 creates/s, kill at a random time, with 1 and 6 writers. Record the lane backlog age p99 from the gauge. Loss must not exceed backlog age x rate. This sets the number in the contract.
3. Random kill loop: 200 runs, 6 writers, kill at a random delay between 0 and 2 s. After each kill, boot, check the oracle and the graph, then boot a second time. Repeat with a kill at a random delay during the boot itself (covers a kill during replay and during the first checkpoint). Journal growth past one file is covered by a run with large records. This replaces the six crash-point knobs and also finds a hole in the journal file [A1].
4. Torn journal tail (file surgery on the stopped home): cut the last file in the middle of a record; flip bytes inside the last record; zero the last 4 KiB. Boot. Recovered ids must be a prefix and include every id committed before the cut.
5. Mid-file fault: flip a byte inside the last file at an LSN at or below the last CKPT tail, with valid records after it. The open must fail loudly and name the file and the LSN. Nothing is cut.
6. Sealed-file fault: flip a byte inside a sealed journal file. The open must fail loudly. With the --quarantine command the open continues and the lost range is logged.
7. Group-file surgery on a stopped home that has an open period: append zeros, then garbage, to the last segment of a journal-named group. Boot. The tail is cut to the synced length and the journal ops are appended. The result equals the journal in both cases.
8. Lock: start lastdb-maintenance or a second Writer on the live synthetic home. It must refuse with 'home in use'. A ReadOnly opener must succeed.
9. Clean stop: the journal holds no records, the receipt says flush_ok, the descriptor returns to version 1 when the journal is off.
10. Rollback: on to off after a clean stop; on to off after kill -9 (the reader release replays); on to a binary older than the reader release (it must refuse version 2 with an error); the journal-disable command on a home that was killed under load.
11. Disk full: run the synthetic home on a small disk image (hdiutil, about 64 MB). Fill it. The journal append must fail before the memory change, the node must refuse writes with a retryable error, and it must recover when space returns. This needs no code hook.
12. Delete and hard erase: an erased id is absent after recovery. Its bytes are gone from the journal after the forced checkpoint, roll and unlink. Run the physical audit with journal/ included.
13. Destructive admin: drop a group, drop versions (retention) and delete a namespace. Kill -9 after the DROP or DEL commit. Boot. No old record comes back [A4].
14. Restore and bulk: restore a synthetic home bigger than the journal cap into a new home that has an old journal/ directory (it must be deleted first). Run a share import larger than the soft cap on a live node; it must wait, not fail [B9].
15. Sustained cell: 30 s at 20 creates/s with persist_queue_full = 0. Then a paced 10-minute run with the pin bytes and journal bytes gauges inside their caps.
16. Boot time: a journal at its soft cap replays in under 20 s [G].

Not possible with these means: an fsync failure on the journal or on a group, and power-loss behaviour. kill -9 cannot test the page cache. These rest on code review of the commit path and the WAL rule, on wal_violations and on the debug assertion. A power-loss claim is never made from a kill -9 drill alone.

## Pros

- Group reduction first lowers every term: lane barriers, checkpoint barriers, pin memory and cold group loads. It needs no journal and it needs no new on-disk file class (M4 needs no format change at all).
- Experiment 0 costs 1 to 2 days, changes no behaviour, runs on a fresh synthetic home, and decides the layout work and the journal with numbers instead of guesses.
- If the journal is built, one commit shares many writes. The lane path goes from 22 data fsyncs and 6.5 sidecar fsyncs for each create to 6 shared journal commits, then about 1/B.
- A kill -9 loses nothing that left the store call. The measured loss of 92 of 150 after a deferred flush drops to zero after the lane drains.
- The tick can run in parallel with no order rule. Hot groups share one fsync per interval at higher rates.
- The order is stronger than today. A crash leaves a strict prefix of one total order across all collections and lanes. The GC re-arm, the body-before-tip rule, the delete barrier and the converging deletes all gain from it.
- Recovery loads no group. It truncates each open group to its synced length and appends the journal tail. Boot cannot fail on an oversized group, a garbage tail cannot be misread, and the cost does not depend on group size.
- The change stays in one layer: one choke point (append_with_threshold) and one barrier funnel (flush_scope).
- It closes real holes: a home lock (nothing locks the home today), directory fsync for new group files (plain mode skips it today), and replay for every offline tool.
- Each step ships alone behind a switch, and the reader release gives a way back before any journal is written.
- The wait for a durable ack falls from hundreds of ms to tens of ms. The shutdown drain gets faster.
- It keeps Tom's rules: ack before durable, read-after-ack, purge never drops an unflushed record (Dirty clears at D by default), 500 ms tick.

## Cons

- PROVEN (measured, #167): the journal does not cut the total F_FULLFSYNC for each create. A create costs 22.2 data + 6.5 sidecar = 28.7 barriers now. With the journal it costs about 22 group + new directories + 6 journal + checkpoint share, so 29 or more at 3 to 6 creates/s. The gain is latency off the lane and parallel fsync, not fewer barriers.
- PROVEN (code): a directory fsync is a full F_FULLFSYNC on Apple (S/durability.rs:90-93, :124-129; Rust std maps sync_all to F_FULLFSYNC). Plain mode skips it today. The journal must add it for every new group directory, and on a fresh synthetic home most groups are new. The directory term is in the sum.
- PROVEN (code): the default parallel flush threshold is 64 groups (S/store/flush_config.rs:5). The fair baseline for the journal is the knob run (threshold 4), which already gave 1.6x. The first draft compared against the serial default.
- PROVEN (code and doc): the journal does not reduce cold group loads. The primary showed a 3.1 s gate hold for 24 cold loads. After fsync leaves the lane, cold parse and compute can bound the rate at 2 to 7 creates/s on real groups. The bars use tiny synthetic data and cannot show it.
- PROVEN (code and doc): the checkpoint interval is bounded by the 5000-token refusal (about 125 creates), the pin memory (a create can pin 20 to 60 MB on real groups; tips 7 to 8 MB) and the 500 ms rule. The first draft's 10 to 35 creates/s is withdrawn; 9 to 23 at the best [G].
- PROVEN (code): every record is written twice (journal plus group file). Bytes double. Group files do not coalesce bytes, only fsync calls. A 16 MiB blob is written twice, and the lock section copies it, which delays every other writer.
- PROVEN (code): the client ack in write mode comes before any store write (schema_delta_helpers.rs:105-108). The store journal sits below the lane, so the loss window stays the lane backlog (up to about 70 s at overload). Tom's model taken literally needs a second, intent journal.
- PROVEN (code): sidecar writes without fsync are not safe for big groups. The rename needs the data fsync (keysidecar.rs:373-392), and drop_hash_group_dir trusts the sidecar for groups too big to load (hash_group_admin.rs:51-52, :134). A kill -9 drill cannot show a power-loss fault here. This is why the built rule (2026-10-14) keeps the flush for a group of half the cold-load cap or more. Two points stay unproven: whether APFS can persist the rename before the file bytes (every such file fails the checksum), and whether the old sidecar flush also made a new plain-mode group directory entry durable. Plain mode does not sync new group directories (append.rs, sync_dir only on the sorted and encrypted paths).
- PROVEN (code): several writers bypass the append funnel: maintenance_drop_versions (version_retention.rs:286), drop_hash_group_dir, install_chunk, migrate, compaction. Each needs a journal-first rule, and a missed one brings deleted data back. Seven live bulk callers also write through append and double their bytes.
- PROVEN (code): the meaning of flush() splits into commit and checkpoint across 163 callers in 86 files. A wrong class is silent: stale group files for backup or audit, or a stall on a hot path.
- PROVEN (code): the layout descriptor reader rejects version != 1 and the writer never rewrites it. Version 2 locks out every older binary, including the saved pair in the safe-upgrade rollback, until the reader release is the saved pair. The shutdown receipt has deny_unknown_fields, so its meaning can change only in code.
- PROVEN (code): group-file records have no checksum or sequence (S/segfmt.rs:1-3). The journal cannot detect damage inside synced bytes, and a WAL breach cannot be found from the files. Only the in-process wal_violations counter sees it.
- PROVEN (code): the durability counters are process-wide statics (S/durability.rs). A second store in one process mixes the gauges and shares the one parallel permit. They become per store.
- Recovery can block boot only through the journal itself (a CRC fault in durable data). Then an operator command is needed. A poisoned journal also stops every offline tool that opens as Writer.
- Dirty pins live until the checkpoint. Memory grows with distinct dirty groups. Past incidents (memory-guard kills, EMFILE at flush) say a cap is required.
- Hard erasure takes longer. Bytes stay in journal files until T passes them. A hard-erase proof needs a forced checkpoint and roll.
- The journal commit may slow down while the checkpoint runs, because a device flush waits for the device queue. This is the largest unknown (trial T1).
- One journal lock sits inside every shard lock, and one journal fsync stall blocks every lane at once. A slow or failing group holds T back, and the journal grows until a cap refuses writes.
- No tests exist by Tom's rule. Power-loss correctness of the WAL rule, the checkpoint and recovery rests on code review, wal_violations and a debug assertion. kill -9 cannot show it. The layout reduction is also a one-way step after the soak.
- This version covers plain homes only. Frame-AEAD homes keep the barrier path.

## Decisions for Tom

- Confirm the order: Experiment 0 and the group reduction first, the journal only if Step 3 says yes. Confirm or change the proposed gates (layout work if 5 or more of 22 groups can go; journal only if the prototype drains at least 2 times the knob run and reaches 20 creates/s paced with zero refusals; stop the rate work if the zero-fsync lane time is 100 ms or more for each create).
- Allow the two prototype switches (LASTSTORE_TRACE_SYNC and LASTSTORE_JOURNAL_PROTO) in a DEV worktree build for Experiment 0. The trace switch is observation only. The journal prototype is throw-away and is not merged unless you say so.
- Layout group reduction scope: only the safe merges (M4, M1, M3), or also M2 (atom bodies into the tips collection), which grows tips groups and touches decision 15 of the warm-set design. Accept that the layout change is one way after the soak (reverse drain or restore is the way back).
- Barrier-free edge and count records (M5): may the GC owner review whether the reachability audit gate makes it safe? Without that review it is not claimed.
- Sidecar flush rule: built 2026-10-14 as "skip the forced write for a group under half the cold-load cap, keep it above". Tom approved the direction. The size floor came from review. A kill -9 drill cannot show it is safe against power loss.
- May the bars get a new-key create cell, and may a synthetic seed with real-size groups (a 14000-key tips group of 7 to 8 MB, 5 MB atoms groups) be made? Without them the cold-parse cost and the rate claim cannot be measured. This is decision 4 of the warm-set design, still open.
- What proves power-loss claims: accept code review plus the wal_violations counter plus a debug assertion plus the disk-image drill, or approve small fault hooks (crash points, an fsync trace with truncate emulation, fsync failure injection). They are product code that acts like a fixture harness, so you decide.
- Approve a new on-disk file class (<root>/journal/), the descriptor version=2 fence, the reader-release-first plan, the journal-disable command, the lazy creation of the journal, and the unchanged shutdown receipt (flush_ok true only with an empty journal).
- Should the launch setting that turns the journal on go through lastdb-safe-upgrade as a version change, with the journal-disable rehearsal as a gate?
- Choose what clears the warm-set Dirty token: D (the checkpoint; the literal rule) or J (the journal fsync; the pin keeps the bytes). It must be decided BEFORE the writer release, because clearing at D turns the 5000-token refusal into the throughput limit.
- Decide the 500 ms rule under load. Keep it as a minimum gap and let the interval stretch to the checkpoint time (recommended), or hold 500 ms and accept a lower rate. Set the journal caps (soft 128 MiB, hard 512 MiB) and the pin cap (proposal 256 to 512 MiB). At the hard cap writes get a retryable refusal, and bulk writers wait.
- On a group fsync error or a journal fsync error: exit the process so recovery rebuilds from the journal (recommended once the journal is on), or stay up and refuse writes? The macOS behaviour of F_FULLFSYNC after a failure was not tested.
- Choose lane completion option X (persist turns and graph dirty-clear at APPLIED; receipts at J) or option Y (turns after the batch fsync; can deadlock across lanes). X is recommended and changes what dirty-clear means. Also set the lane batch cap (proposal: commit after 4 envelopes or 30 ms).
- Decide whether a logical intent journal at the ack point (Tom's model taken literally) is wanted, and whether the Stage G1 age bound (proposal 5 s) ships with the writer release. Without the intent journal the loss window of an acked write is the lane backlog.
- Decide whether to add a per-record CRC to group files (a dual-read format change) to detect damage inside synced bytes.
- Decide the large-record rule: journal every record including 16 MiB blobs (this version), or bypass the journal above a size (proposal 1 MiB). Decide the rule for live bulk writers: journaled and waiting at the cap (this plan), or a fenced bypass.
- Define hard erasure with the journal: physically erased means gone from group files and from journal files, after a forced checkpoint and roll. Accept the extra latency for that proof.
- Choose where shadow mode runs: only on synthetic homes (recommended), or also on the primary through lastdb-safe-upgrade with a journal that is written without fsync (it doubles write bytes).
- A poisoned record in a sealed journal file: refuse to boot (default) or allow the --quarantine command to skip it with a logged range?
