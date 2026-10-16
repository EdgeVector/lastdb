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
The node currently holds every unique stored coordinate within the existing record budget before append.
This includes atoms, tips, schema data, side records, and protein sibling records that the complete stored batch contains.
Partial logical reservations return their capacity on refusal.

Successful publication invalidates earlier fetched values before it releases the logical holds.
A failed append or publication stops native writes and retains the logical holds in the failed runtime.
The journal keeps its existing uncertain quota after a failed append.
Capacity failures have a distinct retryable write-resource result.

This is the stored-batch admission boundary.
The node now also owns a filesystem-space ticket through the complete publication.
It retains that ticket with logical protection after a failed append or publication.

Native Create and Update requests now prepare private schema copies under one native write gate.
Every schema's atoms, tips, sibling records, schema data, and side records enter one complete stored batch.
This path does not publish a live graph tip or split the request across schema lanes.
It rechecks idempotency and preconditions after it acquires the native gate.
It completes the author-clock barrier before that gate, because the clock queue also needs native storage.
Search delivery starts only after the complete disk root succeeds.
Shutdown and defer resources stay with the complete request owner through its disk commit.

The request now owns an immutable accepted batch and an independent flush worker.
The worker uses the 500 ms default flush interval.
An explicit flush wakes the worker and waits for its complete root.
The exclusive writer gate permits one accepted batch at a time.
A cancelled caller cannot cancel the accepted batch or release its reservations.
An older query keeps the same accepted bytes after the worker publishes the disk root.
The physical-space ticket returns after that root; protected records stay until the last query releases its accepted batch.
Failed publication keeps the request and its reservations, and stops later native writes.

Native queries now load schema data, state, and successor from their captured storage view.
They use a call-local typed cache and do not refresh the live schema cache from an older query.
Native write preparation reads its schema from the current private storage scope.
Bounded namespace removal names at most 4095 records plus its registry record before it changes any bytes.
Larger namespace removal refuses the complete request.

The native resident-mode refusal remains in place.
Stored coordinates and metadata are not the user's schema-to-atom logical paths.
The canonical path and body bridge, alias-directory view, and generated-data evidence must complete before that refusal can change.

LastStore library and binary Clippy passes with warnings denied.
Format and changed function, file, and allow checks pass.
Core and host library compilation passes.
Generated-data admission evidence remains required.
