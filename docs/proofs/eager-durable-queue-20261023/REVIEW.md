# Source review

The candidate keeps the disk-before-ack boundary.
The queue worker owns accepted work after caller cancellation.
A failed physical commit marks the writer for recovery.
The completed sequence advances only after disk publication and post-commit work.

Logical preparation retains one FIFO lock.
Private reads include the earlier accepted prefix.
Ordinary reads retain the previous complete disk root.
This separation protects conditions, counters, and metadata order.

The native change feed reads its sequence from private metadata or the durable in-memory tip.
It does not retain the in-memory tip lock across the queue wait.
The legacy path retains its existing tip lock.
This change prevents a later preparer from waiting for a lock that the disk worker must release.

Physical reservations start under the publication lock.
They retain their quota across an equivalent physical rewrite.
The logical frontier and required format features must still match at commit.
Compaction checks its exact dependency count against the queued quote before publication.
A logical root change still invalidates an old reservation.

Clean memory publication follows request order under one view lock.
Every request invalidates its own changed sources before it installs clean values.
A later request can therefore use the disk-only memory fallback without a stale earlier value.
Read holds and write reservation holds remain.
The removed dirty marks no longer represent a reachable publication state.

The queue uses no collection timer.
A first caller can wait for preparation of callers already ahead of the disk worker.
The maximum cohort retains the existing operation, byte, and request limits.
A refused extension leaves the earlier accepted prefix unchanged.
No automatic replay occurs after a condition check.

The performance and sustained-progress conclusions require the daemon captures.
Source review alone does not prove those results.
A forced stop checks process recovery; it does not simulate a physical power loss.

The first sustained capture exposed an incomplete design.
Every compaction attempt during active writes returned `compaction_behind`.
Disk size reached 5.98 GB before a compaction after the writes.
Those receipts remain as evidence of the failure.
The follow-up removes the empty-queue requirement with the logical-frontier reservation rule.
A new capture must establish progress during active writes.
