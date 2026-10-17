# Collected disk reads

The native reader uses one captured CURRENT for every item in a call.
Its page and decoded-block caches already share immutable data within that call.
The new read path also shares file handles and collects payload work before file reads.

## File ownership

One read call owns at most 32 open file handles.
The pool closes its least recently used handle before it opens a replacement.
It checks the file length before each extent.
Internal writer calls can thus read their own private parts after those parts grow.
Every extent still checks its exact bounds against that length.
The call releases all handles when it ends.

Receipts expose `pack_file_handles_peak` and `pack_file_handle_reuses`.
The peak states the largest single call pool, not all process descriptors.

## One batch

The reader follows these steps:

1. Resolve all supplied keys against the same directory root.
2. Collect distinct payload blocks and validate their map entries and part headers.
3. Order the exact payload extents by file and byte position.
4. Combine adjacent frames into reads of at most 64 KiB.
5. Decode each independent frame and assemble records in the original input order.

A combined read contains no gap bytes.
Repeated keys share block reads but retain their own result slots and record checks.
Every complete record retains its digest, full key, collection, and mutation-version checks.
The reader preserves errors for individual items and never converts an error into absence.

One aggregate budget covers planner space, metadata pages, payload blocks, decoded bytes, and returned records.
The planner also bounds its distinct block count before it allocates entries.
Physical order can change which item first reaches a shared resource limit.
The limit remains explicit and never resets for a later item.

Receipts expose `pack_batch_frames` and `pack_batch_read_runs`.
These counts include attempted batch runs and their required frames.
They do not claim physical-device I/O.

## Query and write limits

An ordinary query call permits at most 10,000 examined physical keys.
One complete write request can require more physical keys than its caller supplies.
The request also checks alternate key forms, atom references, counts, and GC candidates.

The exclusive writer gate selects a separate write-preparation constructor.
It permits 40,000 examined physical keys in one held read call.
Its other caps remain 4,096 metadata pages, 16,384 payload blocks, and 256 MiB of charged bytes.
One captured root, one pending batch, and one shared failure state cover the complete request.
The call keeps its file handles and page and block reuse until it ends.
No budget resets after an error or a later item.

A write collects the physical coordinates before it reads them.
It checks the remaining key allowance before any work for that set.
It divides a larger set into ordered batches of at most 10,000 slots within the same call.
An error refuses the complete request; a partial result does not reach the caller.
Ordinary query constructors retain their 10,000-key cap.

Required memory copies check their exact encoded size against the actual call budget before the copy.
Optional query metadata can decline admission when that budget lacks space.
The warm set still permits 10,000 logical paths.
Physical write checks do not add storage objects or unrelated paths to that warm set.
The new write limit needs a fresh 256-write product receipt before a completion claim.

## Scope

File reads remain sequential.
The format, write admission, acknowledgement boundary, and native activation gates do not change.
This source change does not establish a speed result.
Use release product commands on fresh generated data for the before-and-after receipts.
Keep the primary and its archive outside those commands.
