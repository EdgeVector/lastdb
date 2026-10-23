# Independent safety review: minimal compaction locks

Reviewer: `/root/atom_safety_review`, the first independent reviewer.

**Decision: APPROVE. No source or product evidence blocker remains.**

The approved source is `93729bfbbe2062711c29008b40516b86fd7fbe72`.
The review includes the full change from `1dac886d6`, the main integration at `b9e7434e`, and the final capacity-error correction.
The second independent approval and green final-head CI remain merge gates.

## Review scope

I independently read the source, the capture drivers, the request receipts, the result records, and the cleanup records.
The parent agent performed the product operations on generated homes.
This review does not claim a separate final execution by this reviewer.
No tests, primary data, archive data, or retired database tools form part of this review.

The source review covers these safeguards:

- Exact file protections precede file creation. Exact deletion claims reject protected paths and protected descendants. The registry mutex never spans filesystem I/O.
- Each copy retains its source hold. Every output dependency receives protection before a replacement catch-up cut releases the prior hold.
- Each cleanup plan captures its root and registry epoch under the writer mutex. Publication invalidates earlier plans while output and reused-input guards remain active.
- Prepared publication binds the job UUID, writer identity, and output owner. Atomic directory creation claims the canonical receipt without replacement.
- The publication boundary checks the complete write queue, current root, and accepted view. The matching view changes under the same boundary.
- Required receipt and filesystem barriers precede the CURRENT change. The successful native commit precedes the writer and read-view update.
- Stale work retries outside the shared boundary. HTTP cancellation leaves the detached operation owner responsible for its guards and completion.

The final correction preserves these safeguards.
`private_files.rs:75-89` retains the local guard through file close and exact unlink after a refused owner claim.
An initial `AlreadyExists` error returns before that unlink path.
`Owner::keep` takes the guard only after its path-budget check at lines 96-117.
`sparse_pages.rs:107-112` retries only `ACTIVE_DELETION`; it returns the original path-budget refusal.
No successful publication path changes in that correction.

## Final executable evidence

Both final captures name source `93729bfbbe2062711c29008b40516b86fd7fbe72` and executable SHA256:

`3cc3b86963f5d28c6541850017828efb2582a64c52613276b2d0ec188d7074ef`

### Lifecycle

[lifecycle-final/summary.json](lifecycle-final/summary.json) reports complete execution and all owned processes stopped.
All 13 exact readbacks agree with accepted values through writes, recent copies, long keys, deletes, reopen, backup restore, TTL, and schema removal.
The final live record count is 4,090.
Both complete counter tallies read 24,546 counters and 16,360 tips, with zero mismatches and zero suspect atoms.

The interrupted operation records SIGKILL during the copy phase with one private payload object present.
All known values agree after reopen and the later copy.
The data-only backup records 167 files and 181,028,208 bytes while a copy is active.
Its retained manifest hash independently matches `5d0f5083b0c6ca3b0d75be489e0475cb67f27f3e04762bbb66add181fdd837cf`.
The restore readback agrees with every accepted live record.

Two equal-frontier copies publish generations 161 and 162 with zero and one publication retries.
Seventeen cleanup calls overlap the copies.
The final writer is healthy, the write queue is empty, and both accepted and completed sequence values equal 20.

### Automatic worker

[automatic-final/summary.json](automatic-final/summary.json) reports complete execution and all owned processes stopped.
The generated home contains 4,096 user records.
The readback checks 253 selected keys, including every updated key; it does not claim a complete value readback.
The complete tally reads 24,582 counters and 16,384 tips, with zero mismatches and zero suspect atoms.

The worker completes a copy with 36,686 catch-up entries over nine rounds.
The final writer is healthy, the write queue is empty, and both sequence values equal 80.
Maintenance failures, cleanup failures, and worker restarts all remain zero.

## Larger integrated evidence

[large-run1/summary.json](large-run1/summary.json) records the earlier integrated source `b9e7434e48a5a4b752b5c94dde74af317deb9155`.
Its successful-path evidence transfers to the narrow final correction; its source identity remains unchanged in the receipts.
Four complete readbacks agree for 100,001 live records, including the final reopen.
Three complete tallies each read 600,108 counters and 400,004 tips, with zero mismatches and zero suspect atoms.

Both write-overlap copies publish with 27,047 catch-up entries each and zero or one publication retries.
Both unchanged-value copies also publish with zero or one retries.
Of 220 concurrent cleanup calls, 218 complete without deferral; cleanup removes 2,155 files.
The worker log records three orphan files removed between adjacent status receipts that each report two active jobs.
The original stronger `orphan_swept_during_copy` flag remains `false`.

I independently checked the compressed and decompressed SHA256 values against [receipt-archive.json](large-run1/receipt-archive.json).
Both hashes match; the compressed stream preserves the reviewed raw receipts.

## Cleanup and limits

The final cleanup records retain ownership, process, and exact-path facts.
I independently checked that both exact scratch paths are absent:

- `lifecycle-final`: `/private/var/folders/8n/hvjkb3pd0xddwxg89qp5ymgc0000gn/T/ml5.D4mSaA`
- `automatic-final`: `/private/var/folders/8n/hvjkb3pd0xddwxg89qp5ymgc0000gn/T/ml6.IjyhV6`

The earlier reviewed captures also retain complete cleanup records.
The primary and its archive remain outside these operations.

These limits remain explicit:

- Process interruption does not establish power-loss behavior or every filesystem failure order.
- No capture includes external file blobs, the physical 85-percent part-limit transition, or exhaustion of the 64 MiB path budget.
- No capture observes cleanup invalidation after partial removal. The original observation count remains zero; source review covers the epoch checks.
- The final lifecycle records a 37 ms maximum shared boundary. The final worker records 16 ms; the earlier large overlap records 11 ms.
- Resource samples do not establish strict memory, file-handle, or latency bounds. The adjacent writer phases support no paired speed claim.

No unresolved source finding remains for integrity, registry claims, stale publication, queue/view consistency, cancellation, or crash order.
