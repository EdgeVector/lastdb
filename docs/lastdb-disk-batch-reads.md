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

## Scope

File reads remain sequential.
The format, write admission, acknowledgement boundary, and native activation gates do not change.
This source change does not establish a speed result.
Use release product commands on fresh generated data for the before-and-after receipts.
Keep the primary and its archive outside those commands.
