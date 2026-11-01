# Warm set design: one clean warm set and one small write queue

> **Model note (2026-11-01).** This page was written for the molecule model:
> schema → field → molecule → atom → file. Since home format 7 (the record model)
> a schema stores one record per primary key, and field molecules, tips and atoms
> do not exist. The [canonical model](lastdb-canonical-model.md) wins where this
> page differs.

Status (2026-10-27): this proposal describes the LastStore segment engine (hash group files, store write memory, `LASTSTORE_*` settings), which decision D1 (2026-10-26) removed.

CAUTION: The write route below (memory ack, then a flush every 500 ms) is retired.
The 10,000 logical-record key budget below is also deleted. The warm set has a byte limit (256 MiB by default, `LASTDB_CORE_WARM_BYTES`).
The design is fast durable writes, then the ack (Tom, 2026-10-30). See [the canonical model](lastdb-canonical-model.md).

Update (2026-10-17): [the logical warm set design](lastdb-logical-warm-set-design.md)
supersedes this proposal's memory model. It uses one logical state for reads and
writes, with Hash and HashRange as the two key types. The text below retains the
October 12 proposal and its historical measurements.
The selected [schema preload design](lastdb-schema-range-preload-design.md) loads all required keys before the first selected-schema query continues.
Later ready ranges use complete logical keys, with sparse tips and values and batch fetches for misses.
The [key selection study](lastdb-key-selection-study.md) preserves the earlier disk-first alternative.

Status (2026-10-12): design for Tom's review. Nothing here is implemented. It
supersedes the "Revised order" in `docs/lastdb-resident-convergence.md` for
steps 3 to 6. Steps 1, 2, 2b and 2c of that document are merged.

Related designs: `docs/lastdb-blind-write-design.md` (since deleted; the write path: ack
from memory with no read first) and
`docs/lastdb-write-ahead-journal-design.md` (since deleted; a store journal; not built, group
reduction goes first).

How this was made: six agents read the code (logical set, graph, mutation and
lane path, read paths, storage objects, history). Four agents drafted designs.
Three judges scored them. One agent wrote this design. Three skeptics attacked
it (28 problems found) and the design was revised. The agents read the code at
commit f7369af0 and did not run anything. Claims marked "guess" have no
measurement. Claims of the form "verified" were checked in code by an agent,
not by a bar. Read the code before you rely on one.

Rules from Tom that bind this design: the warm set keeps exact used records
(decision 2026-10-02), the key budget is a count of 10000 records and not
bytes, a hash group does not stay in memory after the fetch, a write is acked
from memory before it is durable and a read after the ack sees it, the flush
is 500 ms, the mutation log keeps ack order for multi-device cloud sync, no
tests are written, and no bar or probe copies `~/.lastdb` (2026-10-12).

## Answer

Keep the ack at the memory write, and let the schema lane write the store in the background. Make the warm set the one cache of clean records (10000 exact records, least recently used purge), add a hash view that survives writes, and fix five verified bugs before any large change. Shrink the ResidentGraph to a write queue only after live gauges on the primary show that 10000 records can serve the real reads, because the two bars cannot see that. Send the log job at the last publish point of a request, so log order equals ack order on the success path, and prove it with a gauge on the log of a node that has sync on. Ship small stages in this order: gauges, bug fixes, hash view, log order, store limits, shutdown drain, then the graph change in three parts, and stop after the hash view if the live gauges say no.

## Measured data this design uses

Synthetic home, release build of main, fixed bars (milliseconds). The status
floor (curl and socket) is 12 ms.

| Measurement | Value |
|---|---|
| Point read p50 / p95, all modes | 13-15 / 14-55 |
| Point read with the graph's clean entries off (`LASTDB_RESIDENT_BYTES=1`) | p50 11-12, no change |
| Hot list of 300 records, graph coverage serves it | p50 13, p95 28 |
| Hot list of 300 records, served from the store | p50 31, p95 49 |
| Write ack seen by the bar: `off` / `read` / `write` | 246-312 / 275-355 / 24-38 |
| Write ack inside the node: `write` / `off` | 6-9 / 130-210 |
| Lane stages in `off` mode | atoms 45-100, molecules 65-100, schema about 1, side records about 5 |
| Lane drain rate | about 7 mutations per second per schema |
| Read-after-ack, 8 cells x 3 modes | all GREEN; 6-worker stress 0 bad (also with the graph clean entries off) |
| Live primary | graph 608k entries, 381 MB; warm set full at 10000 of 10000 |
| Real-data point read (owner run) | 28-31 ms against the 12 ms floor |

A stack sample shows several `F_FULLFSYNC` calls per mutation: the author clock,
the batch barrier for atoms, the batch barrier for molecules, and the change
feed. `LASTDB_TXN_DEFERRED_FLUSH=1` removes only the batch barrier.

## Options compared

Scores are 1 to 10 from three judges: safety / performance / simplicity. A higher score is better. The three skeptics found 28 problems. I did not re-score. I note where a skeptic changes the meaning of a score.

O0. Today: two caches.
- The ResidentGraph holds 608k entries (381 MB). The warm set is full at 10000 records.
- Measured: point p50 13-15 ms (floor 12 ms). Hot list of 300: p50/p95 13/28 ms from graph coverage, 31/49 ms from the store. Write ack: 24-38 ms in write mode, 246-355 ms in off and read mode.
- Problems read in code: a complete hash view cannot form for encrypted data. A hot list dies on every write and every lane completion. Two key forms caused the step 2c bug. Each flush copies all 10000 keys. A new slot starts at counter 0 after a purge (verified: bump_resident_revision uses or_default). An operand is read before the slot snapshot (verified in schema_delta.rs). forget_record drops a record without a dirty test (verified).

O1. Only set LASTDB_RESIDENT_BYTES=1. The graph then drops clean entries.
- Measured: point p50 11-12 ms. 8 of 8 read-after-ack cells green. 6-worker 25 s stress: 0 bad. Hot list 31/49 ms.
- Skeptic correction: this tests the read path only. The slot counters, the atom pins and the lane do not change with this setting. It is not proof of Stage C. It is also not the final state, because the graph still installs and drops clean entries on each read.

O2. Design 1: pending records in the warm set. Scores 4 / 6.5 / 7, total 17.5.
- Flaw: the lane reads the store through the encryption layer. The layer would return the pending body of the key that the lane is about to write. An equal clock loses the clock check. The acked write is lost in memory and on disk.
- Other cost: HMAC and OPE work moves onto the ack path. About 35 files change.

O3. Design 2: one cache in the database layer, delete the warm set. Scores 5 / 7.5 / 4, total 16.5.
- Best hot-list number (13 / 20-28 ms) and least memory.
- Flaws: it reverses the warm-set North Star that Tom closed on 2026-10-06. The lane loses warm-set hits. Its ticket rule names the wrong predecessor. No bar can check its new slot scheme.

O4. Design 3: staged shrink. Scores 7.5 / 7 / 8, total 22.5. CHOSEN, with Stage C now conditional.
- Keep the ack. Add a hash view in the warm set. Shrink the graph to a write queue only if live gauges allow it.
- Skeptic effect: the performance score of 7 holds for Stages A, B and D. For Stage C it is unproven. The skeptics showed that Stage C moves write operands, atoms and Hash-field reads from a 608k-entry cache to 10000 records, and no bar can measure that.
- Flaws: the hot list is likely slower than the graph path (guess 16-25 ms against 13 ms). A hot list works only when 2 x records x fields is small. Two key forms stay in the overlay.

O4b. Design 3 stopped after Stage B and Stage D. NEW. This is the fallback.
- The graph stays. The warm set gains the hash view. Log order is fixed. The bugs are fixed.
- Pros: hot list stays at 13/28 ms from the graph. No risk to write operands, CAS or atom reads. No loss of the clean cache.
- Cons: two caches stay. The 381 MB stays. The key form mismatch stays a bug source.
- Use: this is where we stop if the ghost gauge (Stage A1) shows a high simulated miss rate or a high cold-load cost, or if Tom says no to the hash view.

O5. Design 4: ack after the store append. Scores 6.5 / 5.5 / 3, total 15.0 (the first draft said 15.5; that was a sum error).
- It attacks lane throughput and the crash window.
- Flaws: the ack waits for the lane job. A cold group parse lands on the ack path. It changes vendored durability code that no bar sees. Tom must move the ack point.
- Use: take its store pieces as Track S.

## Chosen design

Name: one clean warm set plus one small write queue. Stage C is conditional.

The rule of the design: the warm set is never ahead of the store. Only the write queue holds a write that the store does not have.

WHAT CHANGED IN REVISION 2
- Stage C is split in three parts (C1, C2, C3). Only C1 can start without live evidence. C2 and C3 need the gates in the staged plan.
- Stage A is split: A1 has gauges only. A2 has small bug fixes.
- The slot protocol is now atomic: one lock section reads the predecessor and makes the slot.
- Change counters live inside the warm set and bump after the store write.
- The log order claim is limited to the publish order. It covers every path that I could close.
- Hot list capacity is 2 x records x fields, not 2 x records.
- A shutdown drain and an age bound on the write queue are named stages, not options. Tom decides.
- Lane group commit moves after the fsync gauges. The 5 to 10 times claim is removed.
- The proof for C and D comes from live gauges, because the two bars cannot see these changes and the safe upgrade no longer runs on a copy of real data.

THE GRAFTS AND THE CONFLICT DECISIONS (spine is Design 3)
1. Where an acked write lives before the lane writes it. DECISION: in the write queue (the shrunk ResidentGraph). Not in the warm set. Not in a new cache.
2. Key form. DECISION: the write queue uses API form. The warm set uses storage form. Read order rule R0 limits the join to a short list of sites (see the consumer table in Stage C1).
3. Plaintext. DECISION: only the encryption layer reads or writes warm bodies. The inner store path never returns a warm body.
4. Hot list. DECISION: a complete hash view at the encryption layer. A change counter proves that it is current. A write replaces the body in place and never demotes the view.
5. Count unit. Tom decides. Default: keep today's count (a tip and an atom count apart). Proposal: count a tip and its atom as one record.
6. Slot counters. DECISION: one global counter and a floor for each of 65536 buckets. The bump returns the predecessor and the target in one lock section. A new slot starts with durable equal to the floor, set in the same lock section.
7. Log order. DECISION: send the capture job at the last publish point of the request, inside the apply gate. No reorder buffer. A failed marker is retried at the head with a time limit.
8. Throughput. DECISION: a separate Track S. Measure fsync calls first. Do not move the ack point.
9. Crash window. DECISION: name it as the lane backlog (up to about 70 s at 7 mutations each second), not 500 ms. Stage G1 (shutdown drain and age bound) is a named stage. Tom decides.
10. Atoms. DECISION: an atom body leaves with its last tip and ages with its tip.
11. Modes. DECISION: keep `read` as an accepted alias of `off` with a warning. The overlay is active only in write mode.
12. Holds. DECISION: delete the hold counters. Every hold ends in the same lock section.
13. Pending writes in the warm set. DECISION: not planned.

SKEPTIC LEDGER (28 problems; K = concurrency skeptic, P = performance skeptic, S = plan skeptic)
Verified by the design agent in code at f7369af0: K1, K2, K4 (marker stays on failure), K6, S2, P1 (serial group loop and own-hash rule for an id without a NUL).
Taken from the skeptic reading only: group count of 1024 for atoms, the 3.1 s gate hold, parallel scope permit, the memory numbers.
- K1 slot race, fatal. ACCEPT. Fix in the slot protocol (ordering section).
- K2 operand read before snapshot. ACCEPT. This bug exists today. Fix in Stage A2.
- K3 change counter bump. ACCEPT with one change. The counter lives in the set. One bump after the store write, in the same lock section that installs the record, is enough. A pre-bump and an in-flight fill count are not needed, because the counter never goes back and the reader checks it after its store read.
- K4 log order paths. ACCEPT. Claim limited. Fixes in the log section.
- K5 crash window. ACCEPT the bound correction. I do not accept that Stage C makes it worse: in write mode the write queue is already the only owner of an unwritten acked write. Moved to Tom (Stage G1).
- K6 protected records dropped. ACCEPT. Fix in Stage A2 and in the purge rules.
- K7 floor buckets. ACCEPT. 65536 buckets and a gauge.
- K8 view demotion on update. ACCEPT. Replace in place.
- K9 overlay in off mode. ACCEPT. Overlay only in write mode.
- P1 cold loads, fatal. ACCEPT. Ghost gauge, parallel group loads and a gate on Stage C2.
- P2 fields multiply the cost. ACCEPT. New arithmetic and an admit cap.
- P3 lane and fsync. ACCEPT. Gauge by fsync first.
- P4 writes pollute. ACCEPT. Admit cap per call.
- P5 memory bounds wrong. ACCEPT. Charge by entries and cap the store write memory.
- P6 mutex. ACCEPT. Hold histogram, shared bodies, one touch per list.
- P7 barrier ids. ACCEPT. A flag in the view.
- P8 hot target. ACCEPT. Limit set after the Stage A1 split.
- P9 small errors. ACCEPT.
- S1 Stage C not provable by the bars, fatal. ACCEPT. Split in C1, C2, C3 with a shadow comparator.
- S2 stale catalog in CAS. ACCEPT. Verified in cas_idempotency.rs.
- S3 operand reads on a cold path. ACCEPT. Ghost gauge and a gate. The skeptic says the operand loads run in parallel. The design agent did not verify it.
- S4 hot list arithmetic. Same as P2.
- S5 Stage D not provable by the bars. ACCEPT.
- S6 Stage B proof is not the shipped setting. ACCEPT. Ghost serve counter.
- S7 decisions hide in the stages. ACCEPT. Stage and decision table, and the no-branch.
- S8 refusal site. ACCEPT.
- S9 consumers. ACCEPT. Table in Stage C1.
- S10 Stage A is not a no-change stage. ACCEPT. Split.

### Data model

WRITE QUEUE (the ResidentGraph after Stage C3). It holds acked writes that the lane has not finished.
- Key form: API (plain hash, plain range). Values: decoded tips and atoms.
- Kept: tips; atoms with atom_pins (atom uuid to a count of unfinished envelopes; decrement also on quarantine and on a failed envelope); key_index; key_tombstones; schema_key_tombstones (the Skip Delete overlay); the dirty set; dirty_by_molecule (a counter that changes only inside mark_dirty and the remove paths); slots.
- Slot control: next_revision (one global atomic counter) and slot_floor[65536] (8 bytes each, 512 KB). A slot holds a resident counter and a durable counter.
- Deleted at C3: byte ledger, LASTDB_RESIDENT_BYTES, partition and page coverage, key_completeness, the schemas map, SlotRead leases, clean installs.
- Size bound: charged by entries, not by atom bytes. The reserve step uses key count x measured bytes for each entry (live gauge: graph bytes divided by entries; about 0.63 KB in the 2026 data) and refuses with a retryable error when the estimate exceeds the defer window. The 10000 count does not bound it.

WARM SET (LogicalResidentSet, store layer, plaintext above the encryption layer).
- Key form: storage (molecule id, blinded hash, OPE range). A scope mixes into the molecule id.
- Records: tip (body), atom body, tombstone, Record (a molecule header or a known-absent id). A body is a shared `Arc<str>`, so a list copy is a vector of pointers, not a copy of bodies.
- State: Clean, or Dirty with a flush token. A full flush that covers the token makes the record Clean. A Dirty record is protected from purge, from forget_record and from forget_all_records.
- Count: tips + tombstones + atom bodies + Records. Cap 10000. LASTDB_RESIDENT_KEY_CAP can only lower it. No byte limit.
- Least recently used list: a lazy queue of (tick, key). A touch sets a new tick in the entry and pushes one queue item. The purge skips a stale item. The list holds only purgeable records.
- dirty_by_token: ordered map from token to key.
- Hash views: for each (molecule, hash), a flag (Absent, Partial or Complete), an ordered list of ranges, and a flag no_barrier (true when every delete barrier read in the fill was absent and no barrier write for this hash came after).
- Change counters: 1024 stripes. Every tip write bumps two stripes: the id stripe and the (molecule, hash) stripe. The counters live inside the set. write_put, write_delete, apply_batch and after_direct_write bump them. The bump happens after the store write, in the same lock section that installs the record. A counter never goes back.
- Deleted dead state: schemas, fields, covered_exact, hold counters.
- Unchanged: the absence-hint cache. The hash-group warm set for non-logical collections. The store write memory (its cap is Track S, see below).

GHOST GAUGE (Stage A1, a live signal, not a test). A small set of 64-bit key hashes that simulates a 10000-record least recently used list over the key stream of every clean graph hit and every write operand. It holds no record and no body (about 1 MB). It runs at three sizes: 10000, 20000, 40000. It reports, for each read class (Hash-field point read, co-key secondary, atom, list certificate, write operand): calls, simulated misses, and the distinct hash groups a call would open. The curve over three sizes also gives Tom the data for the count decision.

KEY FORM JOIN. API form lives in the write queue and in the Skip Delete overlay. Storage form lives in the warm set and the store. The consumer table in Stage C1 names every site.

ENCRYPTION. The store holds ciphertext. The warm set holds plaintext. Only the encryption layer moves a body between them.

STORE WRITE MEMORY (vendored store). A dirty hash group stays in memory as a whole until the flush (a tips group of about 14000 keys is 7-8 MB; the skeptic read 12 MB with the index). This conflicts with Tom's rule that a hash group does not stay in memory after the fetch. It cannot be dropped, because it holds unflushed bytes. Track S sets a cap on its bytes and on the count of dirty groups.

### A point read

1. Read the write queue first (rule R0). Ask for a dirty tip or atom by API key. This is one map read. A key tombstone means absent. The step costs nothing when the molecule has no dirty key (dirty_by_molecule is 0). R0 is active only in write mode.
2. Else the encryption layer asks the warm set. It takes one lock. A tombstone gives absent. A tip or atom body gives a hit and a touch. No file opens.
3. On a miss, read both change counters for the id under the same lock. Then unlock.
4. Read the store. The store write memory comes first, then disk. The loader opens the hash group, copies the one record, and drops the group. On real data a tips group has about 14000 keys and 7-8 MB. A cold miss costs 10-40 ms (guess). Atoms use Flat keys. Each atom id is its own hash for placement, so the atoms of one list spread over many hash groups. A read of many cold keys loads the groups in parallel with bounded workers (Track S3a). Today the loop is serial (verified in point_ops.rs).
5. Decrypt. Lock. Admit the record as Clean only if both counters did not move and no body exists. Purge once. Return the body.
6. get_many does the same in one pass. One lock covers all keys. One store batch covers all misses. One lock admits the results.
7. An atom read uses the dirty atom in the write queue, or the same path by uuid.
8. The inner store path never returns a warm body. This closes the false-absent race.
9. CAS head and idempotency reads use this same chain for every changed key: write queue, warm set, store. They never read the catalog molecule data as a source (see Stage C1). The catalog head can be stale until the converge lane runs.

Result: the graph adds nothing to a clean HashRange point read. Measured with LASTDB_RESIDENT_BYTES=1: point p50 stays at 11-12 ms. The skeptic is right that the bars read only this class. The Hash-field point read, the co-key secondary and the atom read use the graph today and were not measured. The ghost gauge covers them.

### A list of one hash (range list, count, windows)

HASH LIST (HashKey, HashRangeRange, HashRangePrefix on a HashRange field)
1. Rule R0: take the write queue overlay for (molecule, hash) first. Skip this step when the molecule has no dirty key.
2. If the hash view is Complete, copy the pointers to the plaintext records in range order. Skip tombstoned ranges. If the view has no_barrier, skip the delete barrier reads. Touch the view once (one tick for the list), then the tips and atoms in one pass without cloning keys. Open no file. Do not decrypt.
3. Else read both counters for the hash and unlock. Call the inner store list. Decrypt every record.
4. Mark the view Complete only when all of these hold:
- The prefix names a hash only. The range part is empty.
- The call returned every live key. A limit did not cut the list.
- The count is at most 1000 (HASH_VIEW_MAX).
- The whole call (all fields) admits at most 2500 counted records.
- No record was discarded as unencrypted.
- Both counters did not move. If a write moved them, retry the fill once, with the counter of the hash only.
- Every returned tip is still in the warm set at mark time.
5. Admit the returned tips only under the same two limits (1000 for one view, 2500 for the call). A larger list admits nothing and is never hot.
6. The field layer merges the overlay from step 1, reads delete barriers when no_barrier is false, and fetches atoms in one batch.
7. A hash above 1000 records is never Complete. An empty hash is never Complete.

CAPACITY. A list of N records that reads F fields costs about 2 x N x F counted records (one tip and one atom for each field; shared atoms count once). Examples for N = 300: F=1 costs 600 (16 lists fit). F=3 costs 1800 (5 lists fit). F=8 costs 4800 (above the 2500 limit, so it is never hot). F=24 costs 14400 (does not fit at all). With pair counting the costs halve. A board view of 300 cards with many fields is served by the graph today at 13 ms. After Stage C3 it goes to the store. That is the cost of Stage C. Tom decides the count unit or the count, or we stop at Stage B.

CYCLIC USE. If more lists rotate than fit, plain least recently used gives a hit rate near 0, because each list is purged just before its next read. One purge of a tip demotes the view. The ghost gauge shows this. Segmented least recently used is an open decision.

WINDOWS (Offset, After, and scan_range inside one hash). Slice the ordered range list of a Complete view. Else use the store. Admit nothing.

COUNT. A Complete view gives the count with no store call. Otherwise: read the write queue keys first (R0), list the store keys, add dirty keys, subtract tombstones. A keys-only store listing is in Track S3b. Today the store list loads every body.

NO CACHE FIXES THIS. Page, PageAfter and SampleN on a HashRange field list every mk: key of the molecule. Use another schema with the range as the hash.

HOT LIST COST TO WATCH. The field layer reads one delete barrier id for each record, and builds each id with one write call for each byte of the range (about 700 bytes for an 82-byte range). The barrier cache holds 1024 entries, shared with other ids, and refuses ids above 2048 bytes. The no_barrier flag removes these reads in the common case. Stage B also builds the id with a hex table. Stage A1 measures the split. A present barrier still reads from disk each time, so the L1 cell includes a hash with one present barrier.

### A write: ack, then the log

Steps 1 to 6 exist today in write mode. NEW marks a change.
1. Reserve before any state change: change-feed slot, author-clock slot, capture permit (100 ms limit), lane slot and defer bytes. NEW: the defer reserve counts entries, and counts the records this call will write (about 2 x keys x fields). NEW: refuse with PersistQueueFull(records) when the warm set holds 5000 records with a flush token plus the incoming count. The check is in the write-mode branch only. Replay and restore are exempt (they use mode off). A refusal waits up to 200 ms first. A failing flush is a separate alarm that this check must not hide. NEW (Tom decides, Stage G1): refuse when the oldest unwritten ack is older than the age bound.
2. Prepare outside the apply gate. NEW: take the slot snapshot FIRST, before any operand read. A publish after that point changes the counter and forces a reseed. If a field has no molecule id yet, resolve the id first, then take the snapshot, then read the tips. Then build atoms, changed keys and operands. NEW operand rule: for each key, read the write queue first, then the warm set, then the store.
3. Apply gate (the short memory-only step). Take slot gates in sorted order. Compare slot counters. Retry up to 64 times. Check delete barriers.
4. Publish to the write queue. NEW: for each slot, ONE lock section on the slot table makes the slot if absent (durable = floor of its bucket, read inside the lock), takes a new value from next_revision, and returns (predecessor, target). The tickets use these returned values. The code never reads the predecessor in a separate step. Atoms get atom_pins plus 1. Tips get the dirty mark and the key index entry.
5. NEW: the capture job is sent at the LAST publish point of the request, inside the gate. The scope knows how many publish points a request has (1 for create or update, 2 for a mixed batch, one for each schema in a multi-schema Skip Delete). Then fill the lane envelope and drop the gates.
6. ACK. Memory holds the write. In the node this takes 6-9 ms. In the bar it takes 24-38 ms (floor 12 ms).
7. Lane (background, one FIFO for each schema): wait for the author clock; wait for the persist turn; write atoms; write molecules and sibling tips; write the schema record; write side records; write idempotency last. Each put goes through the encryption layer. The layer replaces the warm body in place, as Dirty with a flush token, after the put, in one lock section that also bumps both counters. Atom bodies and headers get a token too. A header write replaces the old token and does not drop the record.
8. Lane completion: ONE lock section on the slot table does complete_persist_turn and the slot drop. If resident equals durable, the slot is dropped and the floor of its bucket becomes max(floor, durable). Then clear the write queue entry, only if the counter and the atom still match: remove the entry, the key index member and one atom pin.
9. Capture worker (background): wait for the clock, stage the marker, write the log record, upload later. The ack never waits for it (Async policy).
10. Flush (every 500 ms): take the flush mark. Pop dirty_by_token up to the mark. Those records become Clean. Purge once.

BULK WRITES. A call that writes more than 2500 counted records admits none of them to the warm set. The store write memory holds the bytes until the flush. Reads miss the warm set and read the store. This keeps one bulk mutation from replacing the warm set. It also keeps the 5000 limit meaningful.

WHERE A WRITE LIVES
- Before step 7: only in the write queue.
- Steps 7 to 10: in the store write memory (bytes) and in the warm set as Dirty (if the call is within the admit limit). The purge skips it.
- After the flush: on disk, and Clean in the warm set.

SAFETY RULES
- I0. The warm set is never ahead of the store.
- I1. Publish happens before the ack.
- I2. A write queue entry leaves only at lane completion, with the exact counter and atom.
- I3. The purge, forget_record and forget_all_records never drop a Dirty record before the flush mark covers its token.
- I4. Read order R0: write queue, then warm set, then store. CAS and idempotency use it too. Nothing reads the catalog as a source.
- I5. A fill cannot install after a counter moved.
- I6. If the lane drops a tip because the durable tip won the clock check, the write queue forgets that entry.
- I7. A ticket predecessor equals the durable value of the slot at the moment the slot is made.

CRASH WINDOW. It is the lane backlog. The write queue may hold up to 512 envelopes per lane. A lane drains about 7 mutations each second for each schema. So the backlog can be about 70 s of acked data that exists only in memory. A SIGTERM waits 30 s for lanes against a 60 s launchd limit (open papercut). This is true today in write mode. Stage C does not change it. Stage G1 bounds it.

### Least recently used purge rules

1. Count = tips + tombstones + atom bodies + Records. Cap 10000 (Tom, 2026-10-02). The count is records, not bytes (Tom, 2026-10-04). Body bytes are a gauge only. Tom decides the count unit (see decisions).
2. Admit only: a point fetch, a write, and one hash list. Limits: one view at most 1000 records; one call at most 2500 counted records (all fields); one write call at most 2500 counted records. Admit nothing from scan_range, paged lists, keys-only reads, or any list or write above the limits.
3. Touch (most recent): a point hit, an atom hit, an exists hit, a write, and every tip and atom that a Complete list returns. A list touches its view once and its records in one pass. A skip is not a touch. A flush is not a touch (a flush must not move a written record to the recent end again).
4. Purge: after a lock section that admitted records, while count is above the cap, drop the first live item of the least recently used queue. The cost is O(log n) or better. A stale queue item is skipped.
5. The least recently used list holds only purgeable records. A record leaves the list when it becomes protected. It keeps its tick and returns with that tick.
6. Protected (never purged): a Dirty tip, tombstone, atom body or header whose token the flush mark does not cover; an atom body that a resident tip links.
7. Drop a tip: remove the tip and its body. Remove its range from the hash view. Demote a Complete view to Partial. Unlink its atom. Drop the atom body with its last tip.
8. Drop a tombstone only when it is Clean. The store holds the delete. A counter that never goes back stops an old fetch from bringing the tip back. No in-flight fill count is needed.
9. A put replaces the body in place and keeps the view Complete. A put with a new range adds its range. A delete does not demote a Complete view. Only a purge of one tip demotes a view.
10. Over the cap: protected records can push the count above the cap. The bound is the lane backlog and the 5000 refusal. If no record can leave, raise the stall gauge and admit anyway. Never refuse a read.
11. Direct writes (restore_batch_put, share import, residue drain, index residue, delete_namespace): the caller passes the keys it wrote. The set clears only the views of the hashes and molecules that contain those keys. It bumps only the counters of those ids and hashes. It drops only the Clean tips and atom bodies of those keys. Dirty records stay. A call on an unrelated namespace does not touch the hot views. forget_record and forget_all_records skip Dirty records.
12. Hard erase and purge jobs: remove the record from the warm set even if it is Dirty. Remove its token from dirty_by_token. Then bump the counters. A memory that serves an erased record is a known incident (commit 0d83c121).
13. Holds: delete the counters. The lock section is the only hold. A hold never spans a store call.
14. Absent markers (proposal, decided by gauge): admit a known-absent record at the cold end. A second hit touches it.
15. The write queue is never purged by count. It drops an entry only at lane completion or on a hard erase.
16. Scan resistance (open decision): plain least recently used is the rule today. If the ghost gauge shows cyclic misses, Tom may choose a segmented list (a new record enters a probation part; a second touch moves it to a protected part).

HOT LIST BUDGET. See the capacity note in the range list section. The cost is 2 x N x F.

### Ordering and concurrency

LOCK ORDER, outer to inner: capture router read guard; CAS key locks; slot gates (tokio, sorted, deduped); tip-publication locks; write queue std locks (short); slot table lock; warm set mutex. No std lock is held across an await, a store call, a decrypt or an HMAC. A reader reads the counters under the set lock, unlocks, reads the store, locks again, and checks the counters.

SLOT PROTOCOL (this fixes the fatal skeptic problem K1)
- next_revision is one global atomic counter. Every publish takes a fresh value. No value repeats.
- ONE slot table lock section does all of this for a publish: read the slot or, if absent, read the floor of the bucket; make the slot with durable = floor; set the new resident value; return (predecessor, target). A ticket uses the returned pair. No code reads the predecessor in a separate step, and none calls slot_resident_revision before the publish to build a ticket.
- A normal lane completion takes no slot gate (deferred_job.rs). So the completion can run at any time between two steps of another writer. The one lock section closes this. If the slot exists when the writer publishes, the predecessor is its resident value, and the completion cannot drop the slot because resident then differs from durable. If the slot is already dropped, the floor already holds its durable value, and the new slot starts from it.
- complete_persist_turn and the slot drop share one lock section. The slot drops only when resident equals durable. The floor of its bucket becomes max(floor, durable).
- The prepare snapshot of an absent slot is the floor of its bucket. The gate check passes if the slot is still absent and the floor did not move. A real change always moves the floor, because a completed slot has a durable value taken from next_revision, which is above every floor that existed before it. A false mismatch costs one retry. 65536 buckets keep false mismatches rare even for a batch of 300 absent slots. Gauges: gate_retries by batch width, and a blocking bar failure for "retries exceeded".
- Tickets: predecessor from the first bump of this envelope for the slot, target from the last bump. wait_for_persist_turn and complete_persist_turn stay as they are. The code already allows a target more than 1 above the predecessor.
- purge_tip (the entry without a completed revision) keeps the slot while resident differs from durable. Today it removes the slot with no check, so a queued envelope for that slot fails with "absent slot".
- This also repairs a flaw that exists now: a purge removes the slot, the counter restarts at 0, and a stale prepare can pass the check.

SNAPSHOT BEFORE OPERAND (fixes K2). Today restore_missing_molecules reads the operand before prepared_slot_revisions takes the snapshot. A local write that publishes in between is invisible to the gate. A replayed write with an older clock then wins in memory over an acked write, until the lane drops it. The fix: snapshot first, then read the operand, then build the siblings. If a slot changes after the snapshot, the gate forces a reseed. This is a small change and does not need the graph change. It ships in Stage A2.

CHANGE COUNTERS (fixes K3). The counters live inside the set. Every writer path calls into the set, so no writer can skip them (the encryption layer delete only forwards to the inner store). The bump happens after the store write, in the same lock section that installs the Dirty body or tombstone. A tip write bumps the id stripe and the (molecule, hash) stripe. A stalled reader that read the counter before the write fails its check when it wakes. The tombstone can be purged later and the reader still fails, because the counter does not go back.

ATOM PINS. A count of unfinished envelopes for each atom uuid. Today a shared atom can lose its dirty mark at the first completion (mechanism read, reach not measured). Remove the atom at count 0. A quarantined or failed envelope also decrements.

ORDER GUARANTEES
- For one slot: gate order = publish order = lane order = store order.
- For one schema: lane FIFO.
- Different lanes are not ordered with each other.

CLOCK VERSUS GATE. The author clock is taken before the gate. Two writers on one key can apply in the opposite order of their clocks. The larger clock wins, so an acked write can lose. This does not change. The bars use one key for each worker.

LANE SKIP. If the lane drops a tip because the durable tip won, the write queue forgets that entry. Today the graph keeps it as a clean stale tip.

OVERLAY MODE (fixes K9). In off mode the ack waits for the lane. If the overlay were active in off mode, a reader could see a write before the writer has an ack, and the lane could still fail. So the overlay stays active only in write mode. Off mode reads the warm set and the store only. Read mode is an alias of off.

WHY THE LANE IS SAFE. The lane reads the store through the encryption layer. The warm set is never ahead of the store, so the lane never sees a body that the store lacks. This is the flaw of Design 1, and this design avoids it.

CAS AND IDEMPOTENCY (fixes S2). cas_idempotency.rs falls back to current_atom_uuid, which reads the catalog molecule data. The code comment says the catalog head can name the pre-delete atom until the converge lane runs. Today a clean graph tip hides this. After the graph stops holding clean tips, CAS and the idempotency check would read the stale head. The fix: read the changed keys from the chain write queue, warm set, store, and never use the catalog as a source. The 2026-09-02 sibling pair case becomes a required cell before C2.

MUTEX. One std mutex for each store. The skeptic noted that a Complete list holds it for the whole copy, and that no run shows the hold time. Stage A1 adds a hold-time and wait-time histogram. Shared bodies and one touch for each list shorten the hold. If p99 hold is above 1 ms in the 6 readers plus 2 writers cell, shard by (molecule, hash) with a global atomic count.

PANIC. Std locks poison on a panic. Keep each section short and free of user code.

### Mutation log and cloud sync

FACTS (read in code)
- The cloud log is logical. It holds one MutationIntent for each request, with full field values. The lane and replay run with capture suppressed, so lane writes never enter the log.
- The capture permit is reserved before any memory write (100 ms limit).
- Today the send happens in finish_logical_commit, after the write future returns. Log order is the order in which futures return. It is not the apply order and not the ack order.
- The log number is assigned at append time. If an append fails, append_product_marker leaves the marker in place (verified). The retry pass appends it later, after newer markers. handle_marker_failure falls back to a direct record_op, which also gets a later number. So a failed append puts that mutation after writes that were acked after it.
- The capture worker is one task. It stages a marker, flushes, and writes the log record. It uses 6 whole-store flushes for each mutation.
- The ack does not wait for the log (Async policy). Tom's goal already holds: apply to memory, ack, write the log in the background. The change is the order.

THE RULE (what we promise). If write W1 is acked before write W2 starts, W1 has the lower log number. This is publish order for the writes that succeed. It is not a promise for a failed append, unless the steps below close it.

CHANGE (Stage D)
1. Move the CaptureReservation, the policy and the clock barrier into the commit scope. Today the reservation is a function argument, not part of the scope.
2. The scope counts the publish points of its request: 1 for create or update; 2 for a mixed batch (create or update, then erasures); one for each schema in a multi-schema Skip Delete.
3. mark_logical_commit_published, at the LAST publish point and inside the gate, takes ack_seq under a tiny mutex and calls permit.send. Earlier calls only count. A mixed batch and a multi-schema request send once, after the last publish. Another request may publish between two of its publish points. That request is acked first or later; in both cases, a client that saw an ack before the request began is before it in the log.
4. finish_logical_commit skips its own send when the scope shows that the job was sent. A request with no reservation in its scope keeps the old send as a fallback.
5. The Durable and WaitForPublication policies send at the same point. They wait after the send.
6. Failed append: the worker does not take the next job until the head marker is appended. The retry has a time limit (proposal: 5 s). After the limit, the worker counts one `capture_order_break`, parks the marker in the retry pass, and continues. The direct record_op fallback also counts a break. Tom decides if a break is acceptable or if the node must stop taking writes (open decision).

RESULT. A success path keeps the order. A failed append is a counted, bounded exception. Different slots can send in any order, and that order is a valid ack order.

COST. One uncontended mutex and one channel send (guess: 1 to 2 microseconds).

ORDER IN OFF MODE. In off mode the ack waits for the lane. Publish order is not the order of the ack times, but the rule above still holds, because a client that has an ack began the next request after the first publish.

PROOF (what the bars cannot do). The synthetic home has no sync engine. reserve_capture returns None, so there is no capture. The bars show only that write p50 does not regress. The proof for order is the gauge `capture_log_order_violations`. It compares the ack_seq of the entries that the worker appended to the log, in log order, and counts each drop. It is not a count at the channel, because the channel is in order by construction. It must stay 0 in a soak on a node with sync on. A synthetic node with sync on needs a new mode (Tom's yes). If Tom says no, the proof is the primary soak and a code review. We say plainly that the review is a source check.

CONVERGENCE. Replay still decides by the clock key (written_at, logical_counter, device, mutation_uuid). Convergence does not need log order. Tom's rule needs it.

LIMITS, STATED PLAINLY
- The capture worker takes about 6 flushes for each mutation. Guess: 15 to 30 mutations each second for the whole node. Above that, a reservation waits 100 ms and fails with CaptureQueueFull. Track S2 batches the flushes. A batch of 3 flushes helps only in a burst.
- Crash gap 1: data reaches disk but the log entry is not durable. Peers never get the write. Rule W1 (Stage G2): the lane writes its first store record only after the marker is durable.
- Crash gap 2: the log entry is durable but the data is lost. The device skips its own entries on download. So the device lacks its own acked write for good, while peers have it. This is a permanent split, not a short window. Rule W2 (Stage G2): the lane saves an applied_through value in the idempotency batch. At boot, replay own log entries above it.
- Boot reconcile code was not read in this study. Stage G2 starts by reading it.
- The warm set holds plaintext and is never uploaded. Cloud sync and backup read disk bytes (ciphertext).

## Pros

- Smallest change that follows Tom's rules, and each stage ships alone with a revert. The risky stage is split in three parts, and the last two parts need live evidence.
- We can stop with a safe result. If the ghost gauge says no, Stages A, B and D alone fix real bugs and keep the graph hot list at 13/28 ms.
- The ack stays a memory write: 6-9 ms in the node, 24-38 ms in the bar.
- A hot list survives writes and flushes. Today every write and every lane completion kills the graph certificate.
- It finds and fixes bugs that exist now: a purge restarts a slot counter at 0 (verified); an operand is read before the slot snapshot (verified); forget_record drops a Dirty record (verified); CAS can read a stale catalog head when no clean tip is resident (verified); a complete hash view cannot form for encrypted data; false-absent race; list hits do not touch; full key copy at each flush; write_delete without a clock check; four env reads for each read; molecule_has_dirty walks the whole dirty set.
- The slot protocol is atomic. The first design had a race that could stop a whole schema lane. It is closed in one lock section.
- Log order equals publish order on every success path for about 1 to 2 microseconds (guess). A failed append is a counted, bounded exception.
- Proof comes from live signals that cannot lie: the ghost gauge, the log order gauge, the shadow comparator and the hold-time histogram. We do not claim a source check as proof.
- The risky parts are last or optional: pending writes in the warm set are not planned.

## Cons

- COLD LOADS (proved by the skeptic). Stage C3 removes a 608k-entry clean cache. Reads of Hash-field points, co-key secondaries, atoms and write operands then go to the store. Atom ids spread over many hash groups, and the group load loop is serial today (verified). A board view of 300 cards with about 700 atoms could open about 500 groups at 4-6 ms each, which is 2-4 s against 13-30 ms now (skeptic arithmetic; I did not verify the group count). The 2026-09-05 incident (26000 cold loads for one query) had this shape. The two bars cannot see it.
- HOT LIST CAPACITY. A list costs 2 x records x fields. 300 records with 3 fields use 1800 of 10000, so 5 lists fit. With 8 fields the list is above the 2500 admit limit and is never hot. With 24 fields it does not fit. Plain least recently used with cyclic reads gives a hit rate near 0. The graph serves these lists today.
- HOT LIST SPEED. The hot list loses the 13 ms graph path. The prediction is 16-25 ms (guess), and the three costs behind it (barrier ids, tip parse, atom parse) are unmeasured. A pass limit of p50 25 ms accepts a 1.9 times worse p50 than today.
- WRITE OPERANDS. Every write reads the current tips and atoms of its changed keys before the gate. After Stage C3 a card that is not in the 10000 records can cost up to 24 cold group parses before the gate (one for each field molecule; the code records a 3.1 s mean gate hold on 2026-08-17 for this shape, and the skeptic says the loads now run in parallel). No bar measures write ack on data of primary size.
- STALE CATALOG IN CAS. After the graph stops holding clean tips, the CAS and idempotency checks can read a stale catalog head. This needs the fix in Stage C1 and a sibling pair cell.
- BARS CANNOT PROVE C AND D. The synthetic home has one schema, one queried field, a serial writer and no sync engine. A missed read site passes 8 of 8 cells. The order gauge cannot trip without sync. New cells need Tom's yes, and they stretch his rule against a fixture harness. The safe upgrade now runs on a small synthetic seed and never on a copy of real data, so there is no pre-primary real-data proof. Real-data evidence comes only from live gauges on the primary.
- LANE THROUGHPUT IS NOT FIXED. About 7 mutations each second for each schema (measured). A client at the write-mode ack time sends about 30 each second and fills 512 envelopes in about 22 s. Then it gets persist_queue_full. The cost may be fsync calls for each hash group (a mutation with K fields dirties about 2K groups), not for each envelope. Group commit may give little. The gain is unknown until the fsync gauge reads.
- CRASH WINDOW. The window is the lane backlog (up to about 70 s of acked data), not 500 ms. A SIGTERM can leave lanes undrained. Crash gap 2 splits this device from its own log for good. Stage G1 and G2 address this, and Tom decides.
- LOG ORDER. The promise holds on success paths. A failed append puts the mutation after newer ones, until the bounded retry. The bars cannot see order.
- MEMORY UNDER LOAD. The 330 MB saving is true at idle only. The old size bound for the write queue counts atom bytes, not entries. A task of 100k small fields could hold about 190 MB in the queue. The store write memory is not bounded: a tips group is 7-12 MB and one mutation with 10 fields on cold groups can hold up to about 150 MB; 500 ms of writes can hold about 500 MB for each lane. A write is held three times until the flush (write queue, warm set, store). The pin bytes conflict with the rule that a hash group does not stay in memory.
- WRITES CHURN THE WARM SET. With 10 fields and 7 mutations each second, writes alone turn over the 10000 records in about 71 s. A hot list not read within that time is purged. The admit limit of 2500 stops one bulk call, not this slow turnover.
- ONE MUTEX serves all reads and writes. A Complete list holds it for the copy. The only contention data (19 threads blocked, 1 core working, 2026-09-29) is from the structure that Stage C3 removes. Stage C3 moves its load to this mutex.
- Two key forms stay (API in the write queue, storage in the warm set) until the write queue is the only API user. The step 2c class of bug is smaller, not gone.
- The warm set holds raw JSON. A list parses N tips and N atoms again (1-3 ms for 300, guess).
- CONSUMERS. Deleting read mode, the ledger and the coverage touches the restore CLI, both bars, the matrix script, the safe-upgrade skill, the node status wire fields and the status gauge contract. An unknown mode value falls back to off with no error. The repo rule requires a zero-reference scan before any resident/ deletion.
- A hash above 1000 records is never hot. Exact page certificates for big paged hashes are lost.
- The count does not bound bytes. The memory guard counts 0 bytes for the warm set. Tom accepted this.
- Unverified: the live hash group fanout; the live size of the hash-group warm set; every writer that bypasses the lane; the boot reconcile code; whether the parallel flush permit limits a second lane.

## Expected performance

All numbers are on the synthetic home, release build of main, in ms. MEASURED means a number from Tom's runs. GUESS means no run exists. The skeptics showed that the synthetic bars cannot predict the primary for Stage C.

POINT READ (HashRangeKey class)
- Measured now: p50 13-15, p95 14-55, floor 12.
- Predicted: no change. Confidence high for this class only.
- Hash-field point, co-key secondary, atom, write operand: not measured. The ghost gauge measures the simulated miss rate on the primary. Each simulated miss is a cold group load of 10-40 ms (GUESS). Real-data point reads are 28-31 now.

HOT LIST OF 300, QUIET HASH, ONE FIELD
- Measured now: 13/28 with graph coverage. 31/49 from the store.
- After Stage B (graph still on): the graph answers first, so 13/28 stays. The warm set answers only after a write killed the graph certificate. L2 and L3 measure that case.
- After Stage C3: 16-25 / 30-45 (GUESS). Stage A1 measures the split first (store list, decrypt, barrier, tip parse, atom parse). The pass rule is set from that split, not now.
- With 3 fields: 1800 counted records for one list. With 8 or more fields: not hot after Stage C3.

HOT LIST WITH A WRITER ON THE SAME HASH
- Today most lists are cold: 31/49. Predicted: 16-25 / 30-45 (GUESS), because the view stays Complete. A fill that loses to a write retries once.

WRITE ACK AS THE BAR SEES IT
- Measured: write mode 24-38 (p95 35-86). Off mode 246-312. Read mode 275-355.
- Predicted: write mode 22-38 on the synthetic home. Stage D adds 0 to 2.
- On the primary: unknown after C3, because of the operand cost. The primary gauge for prepare-stage time decides.
- The write bar default is 10 ms. The floor is 12 ms. No design can pass it. Compare against the baseline.

SUSTAINED WRITES
- Lane: about 7 mutations each second for each schema (measured). Unchanged by A, B, D and C.
- If the fsync gauge shows that calls for each hash group dominate, the fix is Track S3b (sidecar without fsync, exact flush mark). The sidecar part was built on 2026-10-14: a group under half the cold-load cap is no longer flushed (see docs/lastdb-write-ahead-journal-design.md, B6; since deleted). Group commit helps only the fixed part. I removed the 5 to 10 times claim.
- Six clients still get persist_queue_full until Track S.

MEMORY
- Graph now: 381 MB for 608k entries. After Stage C3: near 0 at idle. Under load the entry-based reserve bounds the write queue.
- Warm set: 20-50 MB at 10000 records (GUESS).
- Net: about 330 MB less at idle. Under load: unknown. The proof reports peak memory (RSS peak, store write memory bytes, write queue entries).

FLUSH TICK
- Today the cover step copies up to 10000 keys, possibly several times in one tick. After Stage A2 it costs O(records cleared).

READ-AFTER-ACK
- Predicted: 8 of 8 cells green in all modes, 0 bad in the 6-worker stress, also with cap 100. By rules I0 to I7. Not yet measured. The bars read only create, update, delete with point, range and count.

HIT RATE ON THE PRIMARY
- Unknown. Only the ghost gauge and the primary gauges can show it. The proof run on 2026-10-06 purged 465113 records at a full 10000.

## Staged plan

Each stage is its own PR and reverts alone.

- STAGE A1. Gauges and the ghost. No behavior change. No new bar cell. Items: (1) read the resident mode once for each process (resident_read.rs reads four env variables for each read). (2) Fsync gauges: F_FULLFSYNC calls by caller, hash groups synced for each barrier, sidecar fsyncs, lane stage time split in parse, append, fsync and sidecar. (3) The ghost gauge: a simulated 10000-record least recently used list, at sizes 10000, 20000 and 40000, over the key stream of every clean graph hit and every write operand; per read class: calls, simulated misses, distinct hash groups per call. (4) operand_would_miss and prepare-stage time. (5) view_would_serve: how often a Complete view in the warm set could have served a list that the graph answered. (6) List stage split (store list, decrypt, barrier read, tip parse, atom parse), hash_list_complete_hit, hash_list_miss, fill_refused. (7) Set mutex hold-time and wait-time histograms. (8) gate_retries by batch width. (9) Store write memory bytes, dirty group count, write queue entries and bytes for each entry, oldest unwritten ack age, cold parses on the lane path. (10) The capture log order gauge (it reads the log, not the channel). PROOF: the matrix (3 modes, 3 runs) equals the baseline inside noise. After one self-metrics interval the gauges read non-zero on a synthetic node. PRIMARY: safe upgrade, then read the gauges for a week of real use. The ghost curve and the operand gauge are the evidence for Stage C2. ROLLBACK: revert.
- STAGE A2. Small bug fixes. Each one is its own PR. These change behavior in a narrow way. Items: (1) Take the slot snapshot before the operand read (fixes K2). (2) forget_record and forget_all_records skip Dirty records and remove the token only when the record is replaced. after_direct_write takes the written keys and clears only the matching views and ids (fixes K6). (3) purge_tip keeps a slot while resident differs from durable. (4) dirty_by_molecule inside mark_dirty and the remove paths only. List every writer of the dirty set first. (5) dirty_by_token, so the cover step costs O(records cleared). (6) The least recently used queue holds only purgeable records and is lazy. (7) Dead state and stale docs: delete only after the compiler cascade, a zero-reference scan and the owner check that the repo rule requires. PROOF: the matrix equals the baseline. The read-after-ack bar is green. If Tom allows cell R7 (replayed write with an older clock against an acked local write), it must be green. ROLLBACK: revert each PR.
- STAGE B. The warm set serves hot lists. The graph stays on. Blocked by decision 1 (range records). Items: (1) the encryption layer owns plaintext; the inner get never returns a warm body. (2) Counters inside the set, two stripes for each tip write, one bump after the store write in the install section. (3) The complete hash view with the seven mark rules, the no_barrier flag and the hex-table barrier id. (4) Admit limits: 1000 for a view, 2500 for a call, 2500 for a write call. (5) A put replaces the body in place and never demotes the view. (6) Touch a list once. (7) Bodies as Arc<str>. (8) Reject a prefix with a range part. (9) Atom bodies and headers get a flush token. write_delete gets the clock check. (10) Windows and scan_range inside one hash from a Complete view. (11) The 5000 flush-token refusal in the write-mode branch only, with the incoming count, a 200 ms wait, and replay and restore exempt; add the `records` kind to the error and the node error map, and tell the kanban CLI that it is retryable. (12) A shadow sampler: 1 in 1000 Complete hits also reads the store and counts a mismatch; sampled calls are marked and left out of the latency bar. On the primary the rate is 1 in 20 for the first hour. PROOF: run the bars with LASTDB_RESIDENT_BYTES=1. L1 p50 at most the number set from the Stage A1 split, with loader_loads and cold_shard_loads delta 0 in the hot phase. L2 and L3 (list after a write, list with a writer) are the cells where the graph misses. All 8 read-after-ack cells green in 3 modes. Shadow mismatch 0. The cap-100 cell passes. Point p50 at most 15. PRIMARY: safe upgrade, then view_would_serve and the shadow mismatch gauge. L1 on the primary measures the graph until Stage C3. ROLLBACK: remove the view serve.
- STAGE D. Log order equals publish order. It does not depend on B or C. It can ship after Stage A1. Items: move the reservation, policy and clock barrier into the commit scope; count the publish points; send at the last publish point inside the gate; take ack_seq under a tiny mutex with the send; skip the later send when the scope sent; keep the old send for a request with no reservation; retry the head marker with a 5 s limit and count capture_order_break; the gauge reads the log. Read the mixed batch path (write_mixed_batch_with_erasures) and the hard erasure path first. PROOF: write p50 at most baseline plus 2; read-after-ack cells green; the log order gauge is 0 on a node with sync on (a new synthetic mode needs Tom's yes; otherwise the primary soak and a code review). The bars alone cannot prove order. ROLLBACK: revert.
- TRACK S (store limits). Separate from the cache. S3a is a prerequisite for Stage C2. S3a (vendored store, lastdb-safe-upgrade): (1) a cap on store write memory bytes and on dirty group count, as a retryable refusal; (2) group loads in parallel with bounded workers, one load for each group at a time; (3) the write queue reserve charges entries, not atom bytes. S3b: sidecar write without fsync; an exact flush mark from flush_scope; a keys-only hash listing; a cold group parse without the body copy. S1 (lane group commit) only after the Stage A1 fsync gauge shows that the fixed cost per envelope dominates. S2 (capture batches) has the rule: flush count for each batch at a stated batch size. Do not adopt the cut pass or the ack-point change of Design 4. Do not change the atom key format here. Grouping atoms by hash is a storage format change and a Tom decision. PROOF: a sustained cell (30 s at 20 mutations each second) with persist_queue_full 0 and time to durable p95 stated; the L4 cold cell; the atom scatter cell A1; the fsync gauges. S1 and S2 are planned only if the gauges support them.
- STAGE G1. Shutdown drain and age bound. Tom decides. Recommendation: yes, before Stage C reaches the primary. Items: (1) a shutdown drain: wait until pending lane work is 0, within the launchd limit; (2) refuse with PersistQueueFull when the oldest unwritten ack is older than an age bound (proposal: 5 s). The age bound makes a slow lane visible sooner to the client, so Track S3 should come first. PROOF: new cells: SIGTERM, then N of N present after restart; load then kill -9, then every acked write older than the age bound is present. STAGE G2 (optional): W1 (the lane writes its first record after the marker is durable; needs S2) and W2 (applied_through in the idempotency batch; own log entries above it replay at boot). Start G2 by reading the boot reconcile code.
- STAGE C1. Read order, with the graph still holding clean entries. Blocked by decisions 3 and 4. Items: (1) the slot protocol (atomic bump, global counter, 65536-bucket floor, drop in the completion lock section). (2) Rule R0 at every read site, with this consumer table: count.rs (flip the order), secondary.rs, cas_current_state.rs, cas_idempotency.rs (never use the catalog as a source), restore.rs (per-key merge: write queue, warm set, store; replaces the 'all keys resident' rule), apply.rs sibling rebase, delete_winners.rs (newer_in_memory), purge helpers and storage_slot_resident.rs, guarded_steps.rs, admin_db/order_log.rs, query_executor.rs, record_molecule/compact.rs:297 and dual_read.rs:150-173 (audit the key form), the restore CLI (mode off). One proof for each row. (3) The overlay carries key metadata and is active only in write mode. (4) A shadow comparator: on every operand, overlay and CAS read, compute the new chain answer and compare it with the old graph answer. Count mismatches. Run on the primary until 0 for a week. (5) The lane skip forgets the write queue entry. PROOF: the matrix; cells R1 (poll across lane completion), R3 (sibling CAS pair, the 2026-09-02 case), R5 (idempotent retry and delete-then-create), R6 (failed lane write in off mode: no reader sees it), a schema with at least 8 fields; the 6-worker 25 s stress with cap 100 and 0 bad. PRIMARY: shadow mismatch 0. This stage is not a graph removal, so its rollback is a revert.
- STAGE C2. Stop installing clean entries. This is the risky stage. Needs ALL of these gates: (a) Stage C1 shadow mismatch 0 for a week on the primary; (b) the ghost gauge at 10000 shows a simulated miss rate and a cold-load cost per call that Tom accepts, for every read class and for write operands; (c) Track S3a is live (parallel group loads, store write memory cap, entry-based reserve); (d) Stage G1 decision is made; (e) the cells L1 for F=1, 3 and 8, A1 (atom scatter: 300 cards, 8 fields, atoms across hash groups), L4, L5 (6 readers and 2 writers; p99 mutex hold at most 1 ms) and a write-ack cell on a hash group of primary size pass their limits. Items: rehydrate_tip_at, rehydrate_atom, finish_partition_read and finish_page_read return the value and install nothing. Drop on clean at lane completion with atom_pins. A dirty entry still wins. PROOF: the matrix; the stress; limits from the Stage A1 split; graph entry count near 0 two seconds after load; write p50 at most baseline plus 2; gate_retries below 5 percent. PRIMARY: after the supervised step, watch hit rate by class, prepare-stage time, cold groups per call and the mutex histogram for a week. If any is worse than the accepted number, revert at once. If the ghost gate fails, do not ship C2. Stop at O4b. ROLLBACK: the safe-upgrade rollback copy.
- STAGE C3. Delete what C2 made unused: the ledger, LASTDB_RESIDENT_BYTES, partition and page coverage, key_completeness, the schemas map, SlotRead leases, and the graph term in the process memory budget. Keep `read` as an alias of `off` with a warning. Keep the status wire fields (resident_bytes, resident_entries, key_set_*) and report 0 for one release. Update both bars, the matrix script, the lastdb-safe-upgrade skill and docs in the same PR. Run the compiler cascade and a zero-reference scan first. PROOF: the matrix gives the same numbers as C2. ROLLBACK: revert.
- STAGE E. Move the write queue out of the cache module. It becomes WriteQueue in mutation_manager. Delete the remaining cache methods and stale docs. No behavior change. PROOF: same numbers as C3. ROLLBACK: revert.
- STAGE ORDER AND DECISIONS. Ship now with no Tom decision: A1, and A2 items 1-6 (bug fixes). Needs a decision: B (decision 1), D (decision 7 and cells), Track S3 (decision 8, 14), G1 (decision 9), C1 (decisions 3, 4), C2 (decisions 2, 11, 12, 13 and the gates). NO-BRANCH: if Tom says no to the hash view (decision 1), Stage B shrinks to the bug fixes, the hot list stays at 31 ms from the store, and Stage C2 has no reason to ship. Then stop at O4b with A1, A2, D, S3 and G1.
- NOT PLANNED. Pending writes in the warm set (Design 1). Start it only if overlay bugs keep returning, or Tom wants unflushed writes to count in the 10000. It needs a durable-only read mode for the lane, a pending header record, and a stamp-checked settle. It needs a new review first.

## Proof plan

TOOLS. resident-latency-bar.sh and read-after-ack-bar.sh, driven by resident-bars-matrix.sh. Use a fresh synthetic home. Never copy ~/.lastdb. Write no tests. Set LASTDB_SELF_METRICS_SAMPLE_SECS=1 and wait one interval before you read /api/status. The default sampler publishes every 60 s and hides the counters.

HONEST LIMITS OF THE TWO BARS
- The synthetic schema has fields bucket, id, title and body, only HashRange, one queried field, a serial single writer, a fresh hash for each rep, and no sync engine.
- So the two bars cannot prove: Stage C (CAS, idempotency, operand reads, atom reads, Hash-field reads), Stage D (order), capacity at 10000 records, cold load cost, or lane throughput.
- A source check is a source check. The rules I0 to I7 and a code review are not proof of the live result.
- The safe upgrade now boots the candidate on a small synthetic seed and never on a copy of real data (Tom, 2026-10-12). It checks keyed reads, read latency, memory, persist status, and a source-free restore of a DEV snapshot. It is the right path for each primary change. It gives no real-data evidence. Real-data evidence comes only from live gauges on the primary after the supervised step.

LIVE PROOF ON THE PRIMARY (the live signal)
- Ghost gauge (Stage A1): simulated miss rate and cold groups per call, by read class, at 10000, 20000, 40000.
- operand_would_miss and prepare-stage time.
- view_would_serve (Stage B).
- Shadow comparator mismatch, 0 (Stage C1). Hash view shadow mismatch, 0 (Stage B).
- capture_log_order_violations, 0, and capture_order_break (Stage D).
- Set mutex hold p99, hit rate by class, fsync calls, oldest unwritten ack age, store write memory bytes.

CELLS TO ADD INSIDE THE TWO BARS (Tom must say yes; this stretches his rule against a fixture harness; the fallback is below)
- L1: seed 300 records under one hash. Variants F=1, F=3 and F=8 queried fields. One hash with one present delete barrier. Run 20 warm-up reads and 200 reads. Report p50, p95, and the loader_loads and cold_shard_loads delta.
- L2: one write to the hash, then a list. 100 repeats.
- L3: one writer at 5 writes each second, and 200 list reads.
- L4: seed one hash group of about 14000 keys (7-8 MB). Read 50 distinct cold keys. Report p50, p95, cold loads.
- L5: 6 concurrent list clients and 2 writers. Report p95 and the p99 mutex hold time.
- A1: atom scatter. Seed 300 cards with 8 fields so that atoms spread over hash groups. Cold list. Report time and groups opened.
- P2: a Hash-field point read, and a co-key secondary read (needs a second schema).
- W1: write ack on a hash group of primary size (the L4 shape) with an 8-field schema.
- R1: after an ack, poll the list and the count every 2 ms for 100 ms. Every poll must show the write.
- R2: delete a key, wait 700 ms, list. The key stays absent.
- R3: CAS on a sibling pair (the 2026-09-02 case).
- R4: the 6-worker 25 s stress with LASTDB_RESIDENT_KEY_CAP=100, one key for each worker. Also write 300 distinct keys and read all back. No Dirty record is purged.
- R5: idempotent retry; delete then create of the same value.
- R6: a lane write that fails in off mode; no reader may see the write.
- R7: a replayed write with an older clock against an acked local write on the same key; the local write must stay.
- S1 cell: sustained 30 s at 20 mutations each second; persist_queue_full 0; time to durable p95.
- K1 cells: SIGTERM, then N of N present; kill -9, then every ack older than the age bound is present.
- Sync-on synthetic mode, for the order gauge.

PASS RULES
- Point: p50 at most 15, p95 at most 60, in every mode.
- Write mode: p50 at most 40, p95 at most 90, and at most baseline plus 2 from the stage before. The default 10 ms bar is below the 12 ms floor. Compare against the baseline.
- L1, L2, L3: p50 and p95 limits are set from the Stage A1 split. They are not set before. Hot phase delta of loader_loads and cold_shard_loads is 0.
- Read-after-ack: all cells green, 3 runs for each mode.
- Gauges: deferred_persist_failed 0; shadow mismatch 0; capture_log_order_violations 0; resident_over_cap_stalls 0 in the normal runs; gate_retries below 5 percent; no "retries exceeded" error.
- Stage C2 only: graph entry count near 0 two seconds after load.

BY STAGE
- A1: baseline equals the matrix; gauges read non-zero.
- A2: baseline equals the matrix; R7 if allowed.
- B: run with LASTDB_RESIDENT_BYTES=1; L1, L2, L3; primary view_would_serve and shadow.
- D: write p50, the log gauge on a sync-on node.
- Track S3: sustained cell, L4, A1, fsync gauges.
- G1: K1 cells.
- C1: R1, R3, R4, R5, R6, 8-field schema; shadow comparator 0 on the primary.
- C2: all gates in the staged plan; the primary hit rate watch.
- C3: same numbers as C2.

WHAT THE BARS CANNOT SEE
- Log order and clock reorder: the log gauge and a code review.
- Hit rate at 10000 records, and cold miss cost on large groups: the ghost gauge and L4, A1.
- Crash order inside a flush: only the coarse K1 cells.
- A missed bypass writer: the shadow sampler and comparator.
- Contention at many clients: the mutex histogram and lastdb ops.

PRIMARY RULE (Tom, 2026-10-04). A merged PR or a proof on a copy is not done. Stage B, Stage C2, Stage D and Track S3 end only with proof on the primary, through lastdb-safe-upgrade and the live gauges above.

FALLBACK if Tom does not allow new cells: use the existing cells, the key-cap bar with a small cap, LASTDB_RESIDENT_BYTES=1, and the live gauges. Then Stage C2 does not ship, because the gates (e) cannot pass. We stop at O4b.

## Decisions for Tom

- 1. Range records (decision 2026-10-07). Does a complete hash view that holds exactly the records a list returned break your rule against copying range records? The code at HEAD already admits returned tips. Recommend: allow it. If you say no, the hot list stays at 31 ms, Stage B shrinks to bug fixes, and Stage C2 has no reason to ship (we stop at O4b).
- 2. Count unit. Do you count a tip and its atom as one logical record, as your definition says (one path to one atom)? Today they count as two. The cost of a list is 2 x records x fields, or records x fields with one-record counting. With F=3 and 300 records, 5 lists fit, or 11. Recommend: one record.
- 3. Where unflushed writes live. The write queue holds them before the lane appends. Your text says the warm set keeps a write until the flush. Do you accept the effect in place of the letter? Recommend: accept. The other way is the not-planned pending writes in the warm set.
- 4. New bar cells. May the two bars get the cells in the proof plan (L1 to L5, A1, P2, W1, R1 to R7, S1, K1, and a sync-on mode)? Your rule says proof is the two bars, and you removed the fixture harness. These are bar cells on a synthetic node, but you decide if they are a harness. If you say no, Stage C2 and the order proof on a synthetic node do not ship. We use the live gauges only and stop at O4b.
- 5. Write bar rule. The default 10 ms is below the 12 ms floor, so it is red in every mode. Do you set the rule from the floor or from the node ack time?
- 6. Numbers. Confirm or change these proposals: list limit 1000, call limit 2500, write-call admit limit 2500, flush-token refusal at 5000 plus the incoming count, absent markers admitted at the cold end, 65536 floor buckets.
- 7. Log order. Do you require log order to equal ack order? Convergence does not need it. The promise is now: a write acked before another begins is first in the log, on every success path. For a failed append, do you accept a counted exception after a 5 s retry (capture_order_break), or must the node stop taking writes? Recommend: accept the counted exception.
- 8. Throughput. Do you fund Track S (S3a: store write memory cap, parallel group loads, entry-based reserve; S3b: fsync work; then S1 and S2 only if the gauges support them)? S3a is a prerequisite for Stage C2. Without Track S, sustained writes stay at about 7 mutations each second for each schema, and 6 clients get persist_queue_full.
- 9. Crash window. The window is the lane backlog, up to about 70 s, not 500 ms. Do you fund Stage G1 (shutdown drain and an age bound of 5 s) before Stage C reaches the primary? Recommend: yes. Do you fund Stage G2 (the two log gaps, permanent split of a device from its own log)?
- 10. Rule changes. May we delete the hold counters, and keep `read` as an alias of `off` with a warning? Both change the text of earlier rules. Recommend: yes. Also confirm: the overlay is active only in write mode, so no reader sees an unacked write in off mode.
- 11. Hit rate and count. The ghost gauge gives the simulated miss rate at 10000, 20000 and 40000. If it shows many misses, do you raise the 10000 count? The 2026-10-04 rule forbids a byte limit, so the count is the only knob. Or do you stop at Stage B (O4b)?
- 12. Scan resistance. Do you keep plain least recently used, or allow a segmented list (probation part and protected part) if the ghost gauge shows cyclic misses? Plain least recently used gives a hit rate near 0 when more lists rotate than fit.
- 13. Stop rule. Do you agree that Stage C2 ships only if every gate in the staged plan passes, and that the fallback is O4b (graph stays, warm set serves hot lists after writes, bugs fixed)? Recommend: yes.
- 14. Store write memory. A dirty hash group stays in memory until the flush. This conflicts with 'a hash group does not stay in memory after the fetch'. It holds unflushed bytes and cannot be dropped. Do you accept it with a cap (S3a)? Recommend: accept with a cap.
- 15. Atom placement. Atom ids spread over many hash groups, so a list of many atoms opens many groups. Parallel loads reduce the time. Grouping the atoms of one hash together would help more, but it is a storage format change. Do you want that study, or only the parallel loads first? Recommend: parallel loads first.

## Measured fsync cost of a write (2026-10-12, store gauges)

The store now counts its durability barriers by kind (`/api/status`,
`memory_budget.fsync_*`, `scope_flushes_*`, `whole_flushes_*`, `flush_barriers`,
`groups_synced`). Measured on a fresh synthetic home, release build of main plus
the gauges, write mode, 100 creates with new keys, default flush settings.

| Item | Value |
|---|---|
| Data fsyncs | 4002 (40 per create), 48 s of 62.8 s total |
| Sidecar fsyncs (before the 2026-10-14 flush rule) | 1212 (12 per create), 13 s |
| Drain rate | 1.6 creates per second (62.8 s for 100) |
| Whole-store flushes | 1411 (the 500 ms tick, 8 namespaces per tick) |
| Scoped flushes | 689 |

Fsyncs are about 98% of the lane time. A one-create trace shows 6 barriers and
33 hash groups: metadata 1; atoms phase 11 (atom locator 1, atom reference edge
4, reference edge v2 2, atoms 4); schema index 1; molecules phase 17 (reference
edge 5, reference edge v2 4, tips 8); idempotency 1; change feed 2. An update of
one record syncs 8 + 12 + 3 groups. Every atom puts its edge, count and pending
rows in groups of its own, so the group count grows with atoms, not with batch
size. Group commit shares only the groups that repeat across a batch.

One `F_FULLFSYNC` costs about 5 ms on this disk. Sixteen in parallel cost 2.1 ms
each (2.5x). Syncing the scope in parallel (`LASTSTORE_PARALLEL_FLUSH_MIN_GROUPS=4`,
`LASTSTORE_PARALLEL_FLUSH_WORKERS=8`) took the drain from 62.8 s to about 40 s
(1.6x). Parallel whole-store flush (`LASTSTORE_PARALLEL_WHOLE_FLUSH=1`) gave
36.6 s. Turning the 500 ms tick off gave no gain, so the cost sits in the lane's
own scoped flushes. `LASTDB_TXN_DEFERRED_FLUSH=1` raised the drain to 21 writes
per second but a kill -9 right after the drain lost 92 of 150 acked writes, 82
after 1 s, 18 after 3 s and none after 8 s: the tick needs seconds to cover a
burst. Do not set it on the primary.

Fewer groups per write is the lever that is left: put the edge, count and
pending rows of an atom under one group, or write them without a barrier. That
is a storage layout change and needs a decision.
