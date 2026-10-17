# Layer 2 source implementation

## Status

This note describes the measured node source `5b9183550151b3887df73be1b044aa121c101f7a`.
The physical command source is `a1d03310`; its vendor storage tree matches the measured node source.
The benchmark report contains the product results and their limits.

## Shared locator files

The [locator forest](../../../vendor/laststore/src/disk_pack/locator_forest.rs) gives new hash groups separate roots in one private locator object.
Each child and overflow reference receives its final object identity and page address before publication.
Existing locator objects use one sparse allocator per object during that publication.
The allocator releases its open descriptors between tree edits.

The [publisher](../../../vendor/laststore/src/disk_pack/publish.rs) finishes those locator files before the directory and CURRENT change.
It retains the complete dependency receipt and the existing file and directory barriers.
Published pages remain immutable.

## Ordered accepted requests

The [request scope](../../../fold_db/crates/core/src/storage/disk_pack/transaction/request.rs) keeps complete preparation under one gate.
The [queue](../../../fold_db/crates/core/src/storage/disk_pack/queue.rs) then owns the accepted request and releases that gate during the collection window.
Later preparation reads the complete accepted prefix through the [captured overlay](../../../fold_db/crates/core/src/storage/disk_pack/read/overlay.rs).
Current private changes take precedence, followed by accepted requests from newest to oldest, then the captured disk root.
Delete records suppress older values.

One writer combines complete ordered requests into one stored batch.
The first accepted request fixes the deadline; later requests do not reset it.
The normal maximum collection delay is 500 ms.
An explicit durable request or flush closes the collection window early.
The timer closes the window before the writer waits for the preparation gate.
An active preparation can still delay the start of physical output.

Admission refuses a prefix above 1,024 requests, the saved operation limit, or 128 MiB of quoted stored operations.
Fresh native homes permit 16,384 stored operations per complete batch.
The existing logical-path and defer-byte reservations remain separate limits.
The complete aggregate keeps its physical reservation before a memory ack.
The queue replaces an older aggregate reservation only after the new reservation succeeds.

CAS, idempotency, atom reference counts, reverse edges, and GC candidate changes use the same ordered accepted state.
The [typed metadata view](../../../fold_db/crates/core/src/storage/disk_pack/transaction/publication.rs) also includes earlier accepted requests during preparation.
The queue preserves the original stored operations and their side records.

After acceptance, caller cancellation cannot remove the request or release its resources.
The owner retains complete requests through disk publication and ordered later delivery.
A failed or uncertain publication stops new admission and retains the queue for recovery.
An unflushed memory ack retains its existing crash-loss boundary.

The [flush API](../../../fold_db/crates/core/src/storage/disk_pack/write.rs) captures one accepted sequence and waits for that prefix.
Later traffic does not extend its target.
The receipt exposes the target and completed sequences beside the disk generation and frontier.
The [status API](../../../fold_db/crates/core/src/storage/disk_pack/view.rs) exposes those queue boundaries without a forced flush.

## Author clock and feed

Native requests share a [durable author-clock reservation](../../../fold_db/crates/core/src/fold_db_core/mutation_manager/author_clock/coverage.rs).
It reserves up to one second and 1,024 logical allocations beyond the current state.
Covered requests wait for the same durable barrier before admission.
Live mutations keep their actual allocated clock values.
A restart loads the reserved maximum and skips unused values.
An uncovered physical or logical value requires a new reservation.

The [feed worker](../../../lastdb_node/src/change_feed_queue.rs) combines a bounded snapshot of queued jobs in their original order.
Every job retains its shutdown resource through the durable result.
The feed's background scope can join the open native collection window without an early flush.
The feed updates its tip and subscribers after its durable receipt.

## Warm exact reads

Warm results retain exact point dependencies, selected absences, range dependencies, and typed molecule-header versions.
The [history](../../../fold_db/crates/core/src/storage/disk_pack/validity/history.rs) checks changes only through the query's captured revision.
Identical point bytes preserve validity.
Relevant schema, tip, atom, membership, or conflict changes invalidate the result.
The typed header rule refreshes the returned molecule version without a new content read.

The change history permits 10,000 coordinates, 4 MiB, and 1,024 batches.
One dependency set permits 10,000 dependencies and 4 MiB.
The global witness registry permits 10,000 entries and 16 MiB.
An exhausted history or witness registry causes a normal read fallback.

Each query retains [immutable validated proofs](../../../fold_db/crates/core/src/storage/disk_pack/read/proofs.rs).
Global witness removal cannot invalidate a proof that the query already holds.
The call also has a 10,000-proof and 16 MiB proof limit.
Proofs contain dependency metadata and header scalars, with no retained storage object or independent value cache.

Optional [proof renewal](../../../fold_db/crates/core/src/storage/disk_pack/logical_paths/warm/refresh.rs) uses only budget capacity left after every required warm copy.
It refreshes retained provenance to the captured revision.
It does not expand the 10,000 logical-record warm set.
Older query results retain their earlier provenance; later queries must validate that provenance before reuse.

## Native atom reuse

The [native batch helper](../../../fold_db/crates/core/src/db_operations/atom_store/atoms/write/dedup.rs) reads all exact scoped atom keys under the existing atom locks.
An ordinary batch uses one read call. Larger batches split only at the native 10,000-key call limit.
The private and accepted views include earlier complete writes.
An existing atom retains its original bytes, creation time, source filename, and metadata.
This matches the single-atom write and the canonical immutable-atom contract.

The stored atom supplies blob references, locks, and size contributions.
Only a missing body receives encryption, a body write, and new appended-byte accounting.
The schema, locator, reference, GC, and tip steps remain in use.
Strict byte dependencies remain in use; no content-only validity exception exists.

Global and schema aggregate size estimates become incomplete after atom reuse.
The trust cause is `reused_atom_live_transition_requires_repair`.
The counters skip an unsupported old-body debit for a reused target.
Per-molecule contribution evidence and authoritative disk-cap accounting remain unchanged.

## Native Delete reply

The older native Delete reply published a resident tombstone before its erasure lane reached disk.
Native captured queries use disk and accepted batches, so that reply did not establish immediate native read visibility.
The [native erasure policy](../../../fold_db/crates/core/src/fold_db_core/mutation_manager/write/batch_inner.rs) now waits for the existing durable lane result.
The reply reports `durability: "durable"` after that result.
The schema and hash/range identify the target; `fields_and_values: {}` remains a valid Delete payload.

Mixed requests retain the existing writes-then-erasures order.
An explicit captured-prefix flush covers the earlier writes before the whole native receipt becomes durable.
This also covers the case where every cutoff-protected erase is spared and no erasure envelope exists.
The existing condition, protein, reverse-edge, and shared-atom paths remain in use.
This stronger reply includes disk completion, so its latency has a different boundary from an earlier queued Delete reply.

## Practical limits

- New admission pauses during physical output and ordered later delivery.
- Each admission rebuilds and quotes the bounded aggregate, so preparation cost grows with the accepted prefix.
- A replacement briefly reserves both old and new output. A hard disk cap can therefore cause a conservative refusal.
- Exact warm reads can miss after bounded history removal, witness removal, or dependency-cap exhaustion.
- Optional renewal adds bounded serialization and copy work.
- Atom reuse marks global and schema aggregate size estimates incomplete. Their repair remains separate work.
- Earlier incomplete and debug runs remain separate from the final comparison.
