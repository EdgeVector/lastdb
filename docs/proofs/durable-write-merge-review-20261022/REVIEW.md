# Durable-write merge review, October 22, 2026

**Recommendation: do not merge this branch yet.**
The review covers local product source `3023d9d62` and current main `a02d25ed0`.
No product source changes, PR creation, push, merge, primary access, or shared-system changes occur during this review.

## Source findings

### P2: reserve Delete/Purge memory after notification preparation

Location: `fold_db/crates/core/src/fold_db_core/mutation_manager/molecules/persist/purge_job.rs`, lines 28–35.

The erase path calls `reserve_native_core_memory` before `stage_native_events`.
The reservation uses `PendingBatch::memory_quote()` and then becomes an owner retained through the complete publication.
The later notification adds stored event bytes, the tip, and any retention deletes to that same pending batch.
The reservation therefore excludes those bytes.

The create/update path reserves memory after notification preparation and avoids this mismatch.
The durable request path does not call the final `has_reserved_raw_memory` check; its queue delay is always `None`.
`protect_paths` then reuses the earlier reservation owner.

Move the erase reservation after notification preparation.
Check the final staged size before acceptance and retain the existing disk-first fallback if memory cannot be reserved.
This is a source finding. The small operational capture does not measure the memory-limit boundary.

### P2: valid large requests restore the background writer delay

Location: `fold_db/crates/core/src/db_operations/change_feed/native.rs`, lines 54–58.

`native_events` returns no events when the mutation count exceeds 1,024.
The batch route then queues those notices after the product commit because none of their identities appears in the captured set.
That fallback uses `background_scope`, the same shared writer gate, and a separate complete disk commit.
The next client can therefore wait behind notification work again.

The native format permits up to 16,384 stored operations.
The 1,024-notice cutoff can therefore exclude valid native request shapes before the native operation cap is reached.
The 1,000-call benchmark sends one mutation per call and does not exercise this case.

Keep bounded notification chunks inside the same private transaction, with one tip owner and a complete final memory quote.
Use the native operation and byte limits to refuse an oversized complete request before acceptance.
Do not silently restore the separate background commit for a valid native batch.
This finding also remains source-based; no large-batch latency claim forms part of this review.

## Integration blocker

The branch still forks at `4118c7baa`.
Current main includes PR #270 lifecycle work, PR #275 minimal compaction locks, and PR #274 physical prefix-read changes.
The local prototype predates those changes.

A read-only Git merge calculation reports seven conflicts:

- `fold_db/crates/core/src/storage/disk_pack/maintenance.rs`
- `fold_db/crates/core/src/storage/disk_pack/maintenance/publish.rs`
- `fold_db/crates/core/src/storage/disk_pack/transaction/request.rs`
- `lastdb_node/src/exec/db_compact_routes/native.rs`
- `vendor/laststore/src/disk_pack/home/maintenance.rs`
- `vendor/laststore/src/disk_pack/maintenance/compact.rs`
- `vendor/laststore/src/disk_pack/maintenance/mod.rs`

The newer main prepares compaction output outside both locks and protects exact private files before creation.
It uses conditional root publication and permits unrelated cleanup during a copy.
Integration must preserve those rules instead of selecting the older prototype's compaction implementation.
The prior benchmark does not establish performance or recovery of that integrated result.
The [integration receipt](integration.json) retains the exact source identities and conflict list.

An existing draft PR #254 also changes durable acks and native notices.
It predates journal removal and reports conflicts.
It is background context for consolidation, not a reviewed substitute that this review approves for merge.

## Operational evidence

The review uses the exact final benchmark executable, SHA-256 `18b61d57b703ecb0b676fcfd52aa1e2604bf87bba6b273e3e9e9587abdc70871`.
It runs on one new generated home and two new schemas.
Three real batch calls create, update, and repeat the same update for both keys.
All three calls return durable receipts.
Six notices survive normal restart, and every notice identity appears in the returned receipt identities.

The repeated HTTP call allocates fresh author clocks and returns fresh mutation identities.
It does not exercise the imported-clock idempotency-filter branch.
The review therefore makes no idempotency regression claim from this capture.
Original requests, responses, reopened notices, binary identity, and the compressed daemon log remain in [receipts](receipts/).

No tests or fixtures run, and no candidate starts on an existing or primary home.
The two owned daemon sessions stop normally.
The owned generated home and copied executable are removed after the evidence is saved.

## Performance decision

The previous final single-client average improves from 40.61 ms to 25.64 ms.
It still exceeds the 20 ms target.
Its P99 changes from 84.73 ms to 86.12 ms and does not improve.
The four-client, eight-record batch average changes from 170.43 ms to 172.84 ms.
Its P99 improves from 873.10 ms to 548.48 ms.

Those results support the direction of the ordinary notification change.
They do not establish a complete performance win across the requested workloads or a result on latest main.

## Next steps

1. Fix the erase memory quote and keep valid large-request notices in the same native transaction.
2. Integrate the durable-ack and metadata changes with current main while preserving its newer compaction and lifecycle work.
3. Complete the non-test build and lint gates, then repeat fresh-home recovery and matched benchmarks on that exact source.
4. Revisit merge after those checks. Continue toward 20 ms through the measured metadata and root-publication costs.

The disk guarantee remains a requirement throughout these steps.
The previous performance captures and [detailed design limits](../durable-write-fastpath-20261022/DESIGN.md) remain available for comparison.
