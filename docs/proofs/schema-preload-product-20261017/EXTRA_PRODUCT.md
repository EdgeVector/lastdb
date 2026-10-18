# Extra product results

The final program passes the ordinary product operations on fresh generated data.
The concurrent operations show correct results, but do not show a write ack before the initial key load ends.
These results are operational checks. They are separate from the matched before and after measurements.

The program SHA256 is `2c14d7d2f04865454ba9b1a41ba441ee7b69e2bad11a577f1056fe5b43c1324b`.
The receipts name this program candidate 5 or `final`.
The primary and its archive are not used.

## Ordinary operations

`extra-ordinary-final/` contains the input recipe, source hashes, complete request receipts, node log, result, and cleanup receipts.
Its result is `complete: true`, with no error.
The workload uses 256 HashRange records, four hashes, five fields, 54-byte API ranges, and 1,024-byte bodies.

The operations pass:

- Five batch Hash reads, with four keys per call. These cover the first read, repeat, update and delete, flush, and normal reopen.
- Seventeen HashRange, record-envelope, and sample reads. The driver checks returned keys, values, and atom metadata.
- Exact count, offset pages, cursor pages, range prefix, and an exclusive end bound.
- A numeric value filter: `score Between 10 and 14`.
- A full record envelope after `/api/db/compact-record`, then a queued update before and after an explicit flush.
- A ten-record sample before deletes, after the first sample keys are deleted, and after a middle key is deleted.
- A range and a sample after physical LastDB compaction.

The Hash-only schema is explicitly selected by the preload policy.
Its calls leave the key-load count at zero, before and after normal reopen.

The repeated full record-envelope read reports one logical warm hit and zero decrypted bytes.
The core warm hit count changes from 216 to 217.
The read after the record update returns the new value.
The next read, after the explicit flush, also reports one warm hit and zero decrypted bytes.
The earlier read precedes that explicit flush; this sequence does not exclude an automatic flush.

The record envelope introduces two additional key domains. This changes the initial load count from two to four.
The delete and physical compaction comparison starts after that change.
The load count and Ready count remain four throughout deletes, compaction, and the final reads.
The key count changes from 257 to 246 after the deletes.

The physical compaction reports:

| Field | Before | After |
| --- | ---: | ---: |
| Bytes | 55,335,025 | 30,773,304 |
| Files | 577 | 91 |
| Parts | 342 | 7 |

The operation executes and reports no deferred reclaim.
It copies 6,113 records and 120 tombstones.
The byte counts are the operation's own counters. They are not a separate disk-size measurement.

All three owned nodes stop normally, without a forced stop.
The supported exact cleanup removes the owned root `sp.7ekv1x3t`.
`cleanup.json` records exit code zero and `root_exists_after: false`.

## Concurrent first load

`extra-loading-final/` contains the final 4,096-record run.
The workload uses four hashes, five fields, 54-byte API ranges, and 1,024-byte bodies.
The driver uses ordinary product requests and native status. It adds no delay inside the program.

Two bounded attempts run, with a normal reopen between them:

| Operation | Observed before the write | Actual write ack | State after the ack |
| --- | --- | --- | --- |
| Update one key and insert two keys | Loading, 1,024 keys loaded | Queued | Ready |
| Update, delete, and move a key | Loading, 768 keys loaded | Durable | Ready |

Both cold reads return the complete before-state, with 16 records.
Neither returns part of the before-state and part of the after-state.
The current reads then return all expected new values and key membership.
The reads after the flush also return the expected state.
Each attempt completes two key loads and keeps two Ready domains through the current reads and flush.
The final domain contains 4,098 keys.

The queued put-only request takes 536 milliseconds. Its cold read takes 160 milliseconds.
The mixed request takes 1,760 milliseconds. Its cold read takes 184 milliseconds.
These elapsed values describe these operations. They do not form a performance comparison.

The status after each ack shows that Loading ends first.
Thus, these receipts prove request overlap and correct current state. They do not prove publication while the initial load remains active.
No further attempts are required for this report.
All three owned nodes stop normally, without a forced stop.
The supported exact cleanup removes the owned root `sp.d571_shl` after the root agent's review.
`cleanup.json` records exit code zero and `root_exists_after: false`.

## Existing storage-meter limit

The separate earlier 20,000-record, five-field workload reaches the existing 128 MiB storage-meter batch limit at normal stop.
Its data writes and explicit flush complete before that error.
It does not reach its first read and provides no concurrent-load evidence.
The matched four-field 20,000-record workload stops normally.

`KEEP_SMALL_LIMIT.md` records the recipe, baseline source comparison, unchanged guard, error, and source paths.
`extra-loading-20000-candidate2/` retains the complete failure and cleanup receipts.
The supported exact cleanup removes that owned root. No guard or storage-meter redesign enters this change.

## Disk component operations

`disk-component/` retains 43 raw component receipts, program identities, and a separate result summary.
Those operations cover packed key pages, boundary key sizes, large keys, tall trees, changes, compaction, and reopen.
They run on generated data and use the component's ordinary CLI.
Their elapsed values are not before and after measurements.
The supported exact cleanup removes the owned component root.

## Earlier attempts

The earlier directories retain their original receipts.
The final ordinary and concurrent results above supersede their successful cases.
The earlier compact-record failure and its product fix remain visible in the candidate 2 receipts.
The candidate 3 false assertion remains labelled as a driver error.
The stopped partial seed remains labelled as an administrative cancellation.
All earlier owned roots are reclaimed through the supported exact cleanup.
