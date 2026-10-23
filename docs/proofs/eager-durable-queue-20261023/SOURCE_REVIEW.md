# Durable write source review

The source keeps the disk guarantee.
The write queue starts without a collection timer.
The latest operational capture must still establish compaction progress during client writes.
The PR remains unmerged.

## Write order and disk guarantee

One FIFO gate orders complete write preparation and conditional writes.
The first accepted request starts its disk worker at once.
Callers already ahead of that worker can share its commit.
Later callers cannot extend the commit without a bound.
The queue retains accepted requests if a client cancels its call.

Ordinary reads use the last complete disk root.
Later write preparation can use earlier accepted values through the private queue.
The disk worker writes the complete files, installs the memory view, and completes delivery before it advances the acknowledged sequence.
A failed write leaves the queue sealed and requires recovery.

The queue limit is 1024 requests.
The complete stored commit also retains the existing operation limit and the 128 MiB byte limit.
A refused extension releases its own quota and leaves the earlier accepted prefix intact.
The server does not replay a conditional write automatically.

## Compaction and file counts

The copy, catch-up, file counts, and prepared receipt stay outside the global write gate.
The final gate checks the exact expected root and installs the prepared receipt and pointer.
A physical rewrite can retain a pending write's quota because it preserves the logical frontier.
A receipt-format change or insufficient dependency space waits for the admitted prefix.

The private compaction job retains one bounded receipt and one bounded set of file counts.
Receipt reuse compares immutable part facts and recomputes the current reference count.
Catch-up skips directory entries that exactly match its previous protected root.
The cursor and lookup caches keep their finite read budgets.

Each private round counts retired pages, new metadata pages, changed payload references, and the directory's empty root.
A newly grafted group requests a complete count outside the gate.
The failed intermediate revision omitted new metadata pages and refused compaction with a count error.
The corrected capture found no such error; it still found publication races during writes.

## Private files and final publication

Private compaction preparation now completes each file's `fsync` without an extra full-device drain.
It does not publish `CURRENT` or return a durable client receipt.
The visible publication still orders the files with `F_BARRIERFSYNC` and completes `F_FULLFSYNC` before success.
Files on another device keep their full barriers.
Any failed private write or flush invalidates that private job.

Apple distinguishes file flush, device order, and permanent storage in its [storage guidance](https://developer.apple.com/videos/play/wwdc2019/419/).
Its [fsync manual](https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/fsync.2.html) explains why the final full-device flush remains necessary for the disk guarantee.
The operational recovery captures use forced process stops; they do not simulate removal of device power.

## Memory publication and read holds

The typed memory view changes after the complete disk publication.
Requests invalidate old values and install clean values in request order under the same view lock.
A later request that uses disk-only memory fallback therefore cannot leave an earlier cached value visible.
The removed dirty-path marks and flush-completion walk no longer control the purge.
Active read holds and private write holds remain.

The memory captures use a 256 KiB warm limit and concurrent large reads.
Held records can temporarily exceed that limit.
The final warm size must return below the limit after the calls release their holds.
That limit describes the warm set, not all process memory.

## Review limits

The repeated captures support request order, same-key conditions, notice order, erase recovery, and warm-set purge behavior.
The final result must use its own binary identity and completed receipts.
Compaction readiness requires successful publications during the measured write interval and evidence of disk-space relief.
A completed capture alone does not establish those goals.
No primary data, primary daemon, archive, shared CI setting, or deployment changes in this work.
