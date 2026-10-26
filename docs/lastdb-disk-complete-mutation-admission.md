# Complete stored-batch admission

The native writer now quotes every ordered operation in one complete stored batch.
The quote includes journal frames and maps, locator and directory paths, possible splits, long-key overflow, complete generation receipts, and checkpoint files.
It assumes no space from future file removal.

The quote uses the existing format ceilings.
It allows two output pages at each of eight tree levels for each final map change.
It includes the initial empty locator for a new group.
It counts possible gaps from contiguous overflow-page allocation.
The retained-part check rejects a batch before append if its conservative output exceeds the 65,536-part inventory limit.

An owned mutation ticket binds the journal quota and map dimensions to the current generation.
A changed root refuses the ticket before append.
The exclusive native write gate stays owned through the complete publication.
Stored coordinates no longer consume the logical path budget.
The memory path reserves exact schema, canonical field, molecule, hash, range, and selected atom identities.
Each path owns that field's content in the existing warm set.
Metadata and absences do not count as logical paths.
Protein sibling paths use the target-addressed binding records of each molecule (`docs/lastdb-molecule-counters.md`). The `bindings` counter must equal the number of records, or the write takes the disk path.
Document-envelope paths reserve each selected declared field under the actual record molecule and atom.
Partial path reservations return their capacity on refusal.

Successful publication invalidates earlier fetched values before it releases the logical holds.
A failed append or publication stops native writes and retains the logical holds in the failed runtime.
The journal keeps its existing uncertain quota after a failed append.
Capacity failures have a distinct retryable write-resource result.

This is the stored-batch admission boundary.
The node now also owns a filesystem-space ticket through the complete publication.
It retains that ticket with logical protection after a failed append or publication.

Native Create and Update requests now prepare private schema copies under one native write gate.
Every schema's atoms, tips, sibling records, schema data, and side records enter one complete stored batch.
These stored operations can exceed the count of caller mutations.
A 256-record update to two fields needs at least 4,096 distinct stored changes before side records.
Atom references, counts, and GC queue records remain part of that same atomic batch.
Fresh native homes use required feature bit `0x20` for a maximum of 16,384 ordered stored operations.
Their initial durable CURRENT contains this bit before a journal append can use the larger limit.
Homes and standalone artifacts without this bit retain the 4,096-operation limit.
An open does not upgrade that limit.
The journal byte cap remains 128 MiB.
The writer quotes and reserves the complete batch before it appends any bytes.
It does not split a caller batch into separate publications to fit the limit.
Numeric refusal details report current, incoming, projected, and distinct projected operation counts.
This path does not publish a live graph tip or split the request across schema lanes.
It rechecks idempotency and preconditions after it acquires the native gate.
It completes the author-clock barrier before that gate, because the clock queue also needs native storage.
Search delivery starts only after the complete disk root succeeds.
Shutdown and defer resources stay with the complete request owner through its disk commit.

The request now owns an immutable accepted batch and an independent flush worker.
The worker uses the 500 ms default flush interval.
An explicit flush wakes the worker and waits for its complete root.
The exclusive writer gate permits one accepted batch at a time.
A later write or an explicit flush wakes the owner for an earlier complete disk publication.
The idle delay uses the configured flush interval, which defaults to 500 ms.
Native write mode refuses an interval outside 1 through 500 ms.
The interval starts disk work; disk completion also includes the publication cost.
A cancelled caller cannot cancel the accepted batch or release its reservations.
An older query keeps the same accepted bytes after the worker publishes the disk root.
The physical-space ticket returns after that root; protected paths stay until the last query releases its accepted batch.
The existing defer-byte gauge reserves a conservative complete request quote until that same boundary.
This byte reservation is separate from the 10000-path budget.
Failed publication keeps the request and its reservations, and stops later native writes.

Native queries now load schema data, state, and successor from their captured storage view.
They use a call-local typed cache and do not refresh the live schema cache from an older query.
Native write preparation reads its schema from the current private storage scope.
Bounded namespace removal names at most 4095 records plus its registry record before it changes any bytes.
Larger namespace removal refuses the complete request.

Incomplete reverse metadata, ambiguous source names, share fan-out, or unavailable path or byte capacity makes a request complete disk before its reply.
This fallback does not publish an incomplete memory view.
The native read bridge and captured schema-name directory now permit explicit resident read and write modes.
Eligible complete writes can receive a memory ack.
The owner can request a durable receipt with `POST /api/db/flush`.
The read-only `GET /api/db/native-status` reports accepted work and the durable frontier without a forced flush.
Fresh native homes use the version-2 schema-reference proof and required feature bit `0x10`.
Opaque conversions do not gain this authority from copied proof bytes.
The default backend remains legacy, and native cloud configurations remain refused.

LastStore library and binary Clippy passes with warnings denied.
Format and changed function, file, and allow checks pass.
Core and host library compilation passes.
The [7110c7f5 node receipts](https://github.com/EdgeVector/lastdb/blob/0b0a79d4c10949e2f0570e82d9bc4fdb9513a285/docs/proofs/layer2-completion-20261016/node-final-off-receipts.jsonl) record complete 256-record durable writes and normal reopen.
