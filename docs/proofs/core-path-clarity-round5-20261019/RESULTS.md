This review removes unused query controls, duplicate write preparation, and a shared-atom deletion defect.
The final product source is `f9a5dc7b188a3e493c6c4a21834b71d083344c19`.
The normal release executable has SHA-256 `144e38bf6596ce70486f95b75581e16024f9bf881563f6fa5a14a1d2bba60b99`.

The source changes are:

- Native preparation transfers its schema, atoms, and related changes into the private disk stage. It no longer copies those payloads into another job.
- Search delivery has one native after-commit path. Unused zero-valued helper arguments and the unreachable inline delivery branch are removed.
- Current field readers no longer accept historical arguments. Read and count entry points explicitly refuse `as_of` and `rehydrate_depth`, including depth zero.
- Definition reads and writes share one entry-install step. Their separate memory charges and admission rules remain intact.
- Removal of the last key also removes an empty molecule model. A model with tips or a complete key domain remains.
- Both Delete forms remove tips. `must_exist` remains a caller precondition and no longer selects physical atom removal.
- Delete reaches Hash and HashRange peers with identical complete canonical fields. Partial cross-layout membership records retain their separate deletion boundary.
- Protein lookup collects molecule keys first and uses the existing batch reads.
- Both physical Purge paths use the same final shared-reference check. The check follows target-edge removal and precedes atom removal.

The first product exercise found the deletion defect and stopped.
A successful durable Delete removed three shared atom bodies but left the HashRange peer tips.
The peer then returned HTTP 500: `selected local tip has no readable atom body`.
The source still contained older rules that excluded this complete Hash/HashRange peer and treated must-exist Delete as physical Purge.
The failed result remains `complete=false`; its release identity and sealed receipts remain unchanged.

The separate fresh-data completion used the final executable and passed:

- All 74 checked read calls in the unchanged write cohorts, including mixed writes, concurrent complete states, and normal reopen.
- All 20 read/count control items: 12 explicit refusals and eight valid omitted/null controls.
- All 29 original protein items, including key changes, reverse writes, current atom identity, durable Delete, and deleted-state reopen.
- The second must-exist Delete refused the absent source with HTTP 400. Five subsequent items confirmed absence and unchanged unrelated records.
- All 12 explicit Purge items with `LASTDB_BARRIERLESS_PURGE=off`. The source disappeared; the surviving peer retained readable shared atoms across reopen.

The normal release build, full required Clippy scope, and separate off-lane Clippy scope passed.
Format, registry, write-route, file-size, function-size, and new-allow checks passed.
Two independent source reviews found no further blocker in the Delete and Purge fixes.
All 1,877 product inputs matched the committed source before and after the release build.

The removed Rust method arguments change the embedded API. Workspace callers now use the current-only signatures.
Wire query fields remain only for explicit refusal; this change adds no history path or compatibility wrapper.
Private disk batches, captured read owners, memory reservations, and explicit physical maintenance remain intentional.

This is a source review and an ordinary synthetic product exercise. It is not a throughput comparison or a whole-process memory-cap measurement.
No tests, primary data, archive data, cloud activation, database migration, installation, or primary upgrade formed part of this work.
The receipt files identify the failed exercise, final completion, source inputs, executable, and exact temporary-home cleanup separately.
