# Native commit barriers, 2026-10-22

Main sends memory acks for ordinary logical writes. Each request can still wait for
an author-clock disk commit. That commit previously required many full device flushes.

This change retains the public ack policy, the 500 ms flush interval, and the disk format.
On macOS, one complete native commit uses bounded file flush sets, ordered publication
barriers, and one final full device flush. An error stops write admission until recovery.
Other platforms retain their existing file barriers.

| Whole request, milliseconds | Main 8067843c4 | Barrier change 8319a6ad3 |
|---|---:|---:|
| Average | 165.54 | 34.17 |
| Median | 142.88 | 30.00 |
| P95 | 273.23 | 42.96 |
| P99 | 351.34 | 58.06 |
| Maximum | 1430.70 | 1688.54 |

The average fell 79.36%. P99 fell 83.48%. The maximum did not improve.
Each version received 1,000 calls across 64 distinct schemas, with one record per batch.
One client rotated through those schemas. No sample was removed.
Automatic maintenance stayed active, with a 4,096-part threshold and no initial delay.
The value size was 64 bytes. The local APFS volume and optimized build profile matched.
No compiler or other owned workload ran during either measure.

Each version returned 1,000 memory acks (`queued`), with no request error.
A later explicit durable mutation completed before an immediate forced process stop.
After restart, exact-key reads matched all final records and the durable record.
A final graceful stop completed its flush. Receipts, daemon logs, and all samples are beside this report.
The runs used fresh generated homes. No primary or archived data was read or copied.
A process stop checks recovery; it does not simulate a power failure.

The 1.69-second maximum remains a limit. Background pack work still holds the write gate.
A separate PR moves old-file cleanup outside that gate. The larger disk-ack experiment remains separate.

Apple documents the ordering and final device flush guarantees in its
[fcntl manual](https://github.com/apple-oss-distributions/xnu/blob/main/bsd/man/man2/fcntl.2).
The source preserves each publication order before its rename and requires the final full flush.
