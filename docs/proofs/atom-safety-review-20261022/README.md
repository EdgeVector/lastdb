# Independent atom lifecycle safety review

**APPROVE** the product source at `bb7a4a7f5219265cb660ba8084b72fdbb2833554` and later commits with documentation changes only.
No unresolved integrity defect remains in the reviewed scope.
The parent agent owns the final current-source build, product operation, CI gate, and merge.

The independent build and product operations use frozen source `43ca120feb20ae6b95ab36dd3e2a088366522369`.
The product source in this reviewer worktree remains unchanged.
[SOURCE_REVIEW.md](SOURCE_REVIEW.md) records the later corrections and the reason this integrity evidence transfers.
[build.json](build.json) records the successful build and all three binary hashes.
Every heavy build and product operation used the shared host lock.

## Actual product result

The final operation started with 4,096 generated user records.
The operation also added long keys, one 128 KiB inline body, and a second schema during a full copy.
It used batches of exact known keys for the value checks and complete membership under one known hash.
The native administrative command supplied the immutable record checks.

The native check compares each selected tuple exactly:

`collection, key, batch, operation, body_sha256, body_bytes, group`

The selection contains all atoms and the `mk:`, `mh:`, and `rdel:v2:` records in tips.
It excludes unrelated clock and queue bookkeeping.
It rejects repeated exact keys and an incomplete native command result.
Before/after equality uses complete tuple dictionaries. The receipts also retain aggregate hashes.

| Actual operation | Result |
| --- | --- |
| Initial full copy | 40,982 selected tuples remained equal; 49,218 native records copied. |
| Nine recent copies | Every before/after tuple dictionary remained equal. |
| Deletes and later recreation | Eight ordinary keys were deleted; four returned with new values. The wide key was deleted and recreated. |
| Long native keys | The largest observed native key was 56,487 bytes after the wide-key delete. |
| Concurrent durable writes | Two writers each sent 16 batches of eight keys during a full copy. |
| New schema during copy | The receipt observed the copy phase after the durable schema records. It found 24 new native hash groups. |
| Concurrent catch-up | The copy receipt records 2,021 changed records over four catch-up rounds. |
| HTTP caller cancellation | The detached owner published the full copy. All 42,669 selected tuples remained equal. |
| Two process interruptions | Each SIGKILL followed an observed copy phase and non-header private payload bytes. Each reopen preserved the selected tuple dictionary. |
| Writes after interruption | Each reopen accepted a new long key. Each later full copy preserved the selected tuple dictionary. |
| Backup during copy | The observed copy-phase request produced a complete data-only backup of 16 files and 79,412,492 bytes. |
| Backup restore | The supported recovery command returned success. The restored root matched the bundle root. |
| Final value check | All 4,105 main records and 32 records in the new schema matched their acknowledged values. |
| Complete counter check | The verifier completed its full tally with zero mismatches and no suspects. It read 16,516 tips and tallied 12,379 atoms. |

The final selected tuple count before the backup was 42,689.
Its digest was `2548fc8c1f1a83ec445d47fb04f7ed1e991be1276976332342ad2247d0a544c2`.
The preserved backup manifest digest is `82bd233387692d00f2178fb2b43d5d4e4e91658383bf4a0d8e0fefc77138dabe`.

## Preserved earlier outcomes

The reports retain each unsuccessful operation and its separate recovery. No failed summary was changed to success.

- `run-1` sent a 20,012-byte API key. The node refused the request with HTTP 400 before compaction.
  The receipt retains the response: `native storage key exceeds format limit`.
  A separate reopen verified all 2,049 acknowledged records and a complete counter tally with zero mismatches.
- `run-2` sent a 7,912-byte API key. The node returned the same format-limit refusal before compaction.
  A separate reopen verified all 4,097 acknowledged records and a complete counter tally with zero mismatches.
- `run-3` completed the full copy, all nine recent copies, concurrent catch-up, the new schema, and caller cancellation.
  Its first interruption observer requested an empty recent copy. That copy finished before the private-payload observation.
  The original result remains `complete:false`, with a null interruption signal.
- `run-3/recovery` continued from the exact acknowledged state without a repeated seed or repeated completed phase.
  It first matched the original selected tuple digest, then requested explicit full copies for both interruptions.
  Its separate result is `complete:true`. Its recipe and exact driver source identify this correction.

The API key grows through the native range encoding and the delete-barrier key encoding.
The final legal API key is 3,512 bytes. The actual native key length above confirms the multi-page key path.
The format-limit refusals received no write acknowledgement.

## Evidence files and cleanup

- `capture.py` contains the independent schedule and native tuple check.
- `run-1/capture-source.py`, `run-2/capture-source.py`, and `run-3/capture-source.py` retain the source for each original operation.
- Each run retains its inputs, recipe, receipts, original summary, separate recovery summary, and cleanup receipt.
- `run-3/recovery/driver-source.py` retains the exact continuation source.
- `run-3/recovery/inputs.json` records the continuation and helper hashes, frozen source revision, and all binary hashes.
- `run-3/recovery/oracles.json` records both process interruptions and the later full-copy equality checks.
- `run-3/recovery/native-backup-manifest.json` retains the actual data-only backup manifest.
- The cleanup receipts for all three runs record `removed:true` after exact path, owner, inode, marker, and stopped-node checks.

All owned nodes stopped. All three generated temporary homes were removed after the required receipts were preserved.
The worktree retains only source, build output, and review evidence.
The `.jsonl` receipts and node logs can match global ignore rules. Preserve them explicitly if this folder enters the PR.

## Scope limits

These operations use generated local data. They never use the primary or its archive.
They use no tests, fixtures, install changes, shared daemon restarts, or shared infrastructure changes.
The 128 KiB value is an inline native body. This review includes no external file blobs.
The counter report confirms zero blob counters and zero blob queue records.

The interruptions cover private copies before publication. They do not cover hardware power loss or every receipt and root-publication crash window.
The physical 85 percent transition and exhausted-disk reservation boundary remain source checks here.
The later worker correction receives source approval; an ordinary worker operation cannot establish the physical 85 percent transition.
