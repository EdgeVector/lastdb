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
The node also holds every unique stored coordinate within the shared logical-record budget before append.
This includes atoms, tips, schema data, side records, and protein sibling records that the complete stored batch contains.
Partial logical reservations return their capacity on refusal.

Successful publication invalidates earlier fetched values before it releases the logical holds.
A failed append or publication stops native writes and retains the logical holds in the failed runtime.
The journal keeps its existing uncertain quota after a failed append.
Capacity failures have a distinct retryable write-resource result.

This is the stored-batch admission boundary.
It does not yet reserve filesystem space or prepare all schema envelopes before graph publication.
It does not permit a memory-first success reply.
The native resident-mode refusal remains in place until the request owner and consistent accepted-write read view exist.

LastStore library and binary Clippy passes with warnings denied.
Format and changed function, file, and allow checks pass.
Integrated product compilation and generated-data admission evidence remain required.
