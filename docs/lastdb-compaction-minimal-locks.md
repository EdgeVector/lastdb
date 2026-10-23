# Minimal compaction locks

Tom requested this change after PR #270. The snapshot format and the memory ack rule stay unchanged.
The primary, its archive, cloud sync, CI policy, and installed programs are outside this change.

## Current state

The node holds a maintenance mutex through the complete copy and cleanup.
The final write gate also covers a key delta, output tree writes, dependency checks, and receipt creation.
The orphan sweep defers for every active copy because private output has no receipt.
These restrictions protect more work than the final shared pointer needs.

## Required result

Copy, catch-up, space checks, receipt creation, and temporary-file flushes occur without the node write gate or writer mutex.
An output file receives exact deletion protection before it exists.
A cleanup can remove unrelated old files during the copy.
Compactions can share immutable inputs. Their private output files have different identities.
Only one conditional change to `CURRENT` takes the publication boundary.

## File protection

Each writer lease owns a small file-protection registry.
Each compaction owns a reference-counted set of its exact private output paths.
The page and payload allocators register each file before create-new, and retain that protection until publication or abandonment.
They also protect their output directories from removal while an allocator can create another file there.
A directory hold does not protect unrelated files inside that directory.

A cleanup takes a deletion claim for one exact file before removal.
The claim fails if an output owner protects that path.
An allocator cannot claim a path while its deletion claim exists.
The registry mutex covers only memory changes. No file I/O or copy runs under it.
File handles still close between trees. Protection does not retain file handles or file contents.

Source and catch-up read holds retain the complete input closure that the proposed root can reference.
Those shared holds permit readers, writers, other copies, and unrelated cleanup.
They are necessary until the replacement root stops referencing those inputs.
Before a catch-up cut hold ends, the job transfers each grafted dependency to exact file protection.
Another compaction can replace the same values at different file addresses.
An unchanged mutation frontier therefore updates only the expected root identity, without another payload copy.
Existing external-reader and single-writer process leases retain their safety rules.

Each cleanup plan captures a protection epoch with its root under the writer mutex.
Every removal claim checks that epoch, including receipt retirement and empty-directory removal.
Compaction advances the epoch before any publication attempt while all input and output protections remain active.
A stale plan stops new removals and flushes its previous directory changes.
This rule covers captured plans that start after publication as well as cleanup that already runs.

## Conditional publication

1. Capture the durable root and its read hold under the writer mutex, without the write gate or a queue flush.
2. Copy and catch up outside both locks.
3. Prepare and flush the complete receipt and `CURRENT` file under private temporary names.
4. Take the existing write gate and writer mutex.
5. If accepted work exists, release the boundary and wait for its normal flush. Do not force a flush.
6. Compare the writer root with the exact root that the prepared result includes.
7. If it changed, release the boundary, catch up, and prepare another receipt outside the locks.
8. If it matches, install the prepared receipt, rename the prepared `CURRENT`, and complete the required directory barriers.
9. Install the matching read view and release the boundary.
10. Run cleanup outside the boundary.

The boundary contains no record traversal, data copy, tree edit, reservation walk, or receipt construction.
Its remaining disk work is the fixed publication metadata and required durability barriers.
The filesystem can delay those barriers; this design makes no fixed millisecond guarantee.

Temporary receipts use UUID names outside the canonical generation directory.
They do not reserve the next foreground generation or change its normal allocation.
A stale result never changes `CURRENT` or poisons a healthy writer.
An uncertain publication retains the existing fail-closed rule.
Cancelled callers leave the independent owner alive until all file work ends.
A killed owner leaves only unreferenced output and explicit temporary names for supported cleanup.

## Concurrency and retries

Remove the operation-wide maintenance mutex and the active-copy cleanup refusal.
The existing cleanup exclusion still prevents two deletion plans from racing each other.
It does not exclude compaction or ordinary writes.
Compaction status counts active jobs so one completed caller cannot report idle while another copy runs.
Two nonblocking job permits bound aggregate copy resources. A third call receives a named capacity refusal before detached work starts.
Each job also has a 64 MiB conservative path-metadata budget, separate from its bounded copy caches and disk reservation.

Publication retries have a finite attempt budget and return a named capacity refusal when writers repeatedly change the root.
Queue deferrals and root changes both consume the eight-attempt budget.
This chooses a retry instead of an unbounded writer stall.
The automatic worker retains its existing retry and soft-limit rules.
Continuous writes can delay a copy's publication. The soft-limit refusal eventually supplies a publication opportunity.

## Required product evidence

- Actual daemon operations on fresh generated homes, with exact accepted-value reads and complete counter tallies.
- Durable writes, long keys, recent copies, deletes, backup restore, and process interruption.
- An orphan sweep and an old-file reclaim complete during an observed private copy.
- Two overlapping compaction calls either publish safely after a retry or return the named bounded refusal.
- Captured publication time and retry counts distinguish the shared boundary from the copy duration.
- All owned nodes stop, and exact owned temporary homes are removed after receipt checks.

No tests, fixture harness, primary copy, primary operation, or power-loss claim forms part of this work.
