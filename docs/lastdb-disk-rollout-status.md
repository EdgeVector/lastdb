# Direct disk work status, October 15, 2026

This work follows [the five-stage plan](https://chatgpt.com/space/page_dac3cffa86f08191b9fe60c47d7477a9) directly.
It uses isolated code worktrees and fresh generated data. It does not use the factory, brain, situations, or kanban.
No primary, primary copy, or archive participates. No primary upgrade occurs.

| Stage | Current result | Remaining completion checks |
|---|---|---|
| 1. Measure reads | Opt-in physical read receipts and fresh-node baseline, PR #174; byte format documented | Preserve receipts as later paths change |
| 2. Disk directory | Exact pages and payload extents, complete journals, immutable updates, hash ranges, saved native codecs, journal resume, bounded inventories, PRs #190–197 and #199 | Complete memory-ack admission, protected capacity, retained live references, compaction, and recovery policy before activation |
| 3. LastDB reads | Explicit native node path, exact/batch/range results, aggregate read limits, normal restart, and fetched-record cache cap, PR #198 | Complete logical-path holds and admission; connect resident reads and memory-first acknowledgements without a root mismatch |
| 4. Conversion and recovery | Journal resume primitives and compatible native-reader reopen | Legacy adapters, bounded conversion, concurrent-write capture, safe handoff, backup restore, references, and reverse conversion |
| 5. Fresh-node proof and primary preparation | Generated-node correctness and initial read/write cost receipts | Release-mode results as data grows, full recovery and rollback evidence, and a concrete primary upgrade package |

## Current activation gate

The default new-node backend remains the legacy format.
The native node path requires explicit selection and refuses an existing legacy home.
Only `LASTDB_RESIDENT_MODE=off` is supported. Resident-first reads and memory-first acknowledgements remain refused.
Cloud-connected native homes also remain refused.
All generation files remain retained. The conservative inventory limit remains 65,536 parts.

The small debug node read is slower than the legacy path; it does not pass the final speed gate.
The generation-carry change reduces repeated writer metadata reads, but it does not complete the read performance work.

## Next implementation boundary

Reserve the complete native store mutation, its physical capacity, and protected logical records before resident publication and ack.
The existing schema-lane reservation covers deferred work; it does not yet cover the native journal and directory limits.
Preparation must include atoms, tips, schema, side records, and protein siblings.
A retry or cancellation must keep each accepted write protected until its complete native root reaches disk.
The normal 500 ms flush policy and read-after-ack behavior must remain intact.

Use the existing product build and non-test checks. Use generated-data product commands for operational evidence.
Keep the primary unchanged. Tom decides the primary action after the required package is ready.
