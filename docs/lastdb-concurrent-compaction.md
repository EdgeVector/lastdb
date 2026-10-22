# Concurrent snapshot compaction

This change completes the compaction follow-up on the snapshot format from PR 268.
It includes the faster pack and shared locator allocator from PR 265.
It changes no snapshot format bit.

## Copy and publication

1. The maintenance owner captures a complete root under the write gate.
2. A typed read hold retains the source files. The write gate then opens.
3. The copy writes private immutable output. Ordinary writes continue on the live root.
4. Up to eight catch-up rounds graft newer locators into the private output.
5. The final handoff takes the write gate and drains accepted writes.
6. A final delta of at most 4,096 changed locators or new groups joins the output.
7. One complete receipt and one `CURRENT` rename publish the replacement.
8. The node installs that root, opens the gate, and reclaims old files.

The final delta includes tombstones. It uses original mutation sequence numbers.
A copy changes no atom counter, field value, or conflict record.
A final delta above its bound returns `compaction_behind` without publication.
The old live root and all accepted writes remain intact.

The copy retains the writer lease until every file operation ends.
The orphan sweep defers while private compaction output exists.
An interrupted copy leaves unreferenced files for the existing orphan sweep.
Readers and backups keep the complete closure of their selected generation.

## Recent tiers

The first copy after a reopen covers the full home.
Later copies use creation generations from the complete receipt.
They rewrite recent tree pages and recent payload objects.
They retain older subtrees through their existing page addresses.
Every rewritten key receives a new overflow chain when it needs one.
The copy checks the age order, key order, page height, and parent intervals.

Eight recent outputs merge into one output above the base boundary.
New payload bytes above one quarter of the base start a full copy.
Changes above one quarter of the base record count also start a full copy.
The record bound also reclaims dead base payloads after a large delete.
An explicit compact defaults to a full copy. `full_copy: false` permits a recent tier.
This conservative byte count includes obsolete recent payload bytes.
The policy resets after a reopen; it needs no format change or durable policy record.

A tier quote reads the receipt and recent payload bounds.
It does not walk old records. The reservation includes an uncompressed payload bound.

## Part limits and status

The worker starts below half of the hard part limit during normal operation.
The observed part rate and the previous copy time can move that start earlier.
A failed reclaim retains the existing 75-percent trigger and its back-off.
New automatic-maintenance writes receive a named 503 at the 85-percent soft limit.
The response includes `retry_after_secs`. A caller can retry after the worker makes room.
The hard receipt limit remains 65,536 parts.

Native status shows the current phase: capture, copy, catch-up, swap, reclaim, or idle.
It also shows the last copy time, swap time, maximum swap time, delta count, and tier boundary.
The phase owner survives cancellation of an HTTP caller.

## Other lifecycle changes

- The schema key list can end a successful partial page before its read budget runs out.
  The cursor names the last returned key, or the last examined key for an empty page.
  `has_more` remains true while work remains.
- The TTL sweep uses the same writer-cost measure and quiet-period watch as the other maintenance jobs.
- Inventory gives the whole-home native compact command.
- Private background batches can share the 500 ms collection window.
  They still return after durability. Write preparation alone can read their owned prefix.
  Ordinary reads retain the committed state. Shared definition caches are bypassed for that private prefix.

## Scope of proof

The product receipts accompany this change under `docs/proofs/atom-lifecycle-completion-20261022`.
They identify the source, executable, generated home, exact read results, and cleanup.
They establish only the workloads and process states that those receipts record.
A process interruption does not establish power-loss behavior.
The primary and its archive remain untouched. No tests run.

The previously documented conflict-marker window change remains a separate retention-policy limit.
This change preserves that behavior. It does not add cloud activation or a journal-home conversion.
