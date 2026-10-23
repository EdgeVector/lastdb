# Eager durable write queue

Every native write waits for its complete disk commit before the ack.
The queue uses no collection timer and no storage journal.

A preparation lock orders logical writes and their conditions.
The first accepted request starts the disk worker immediately.
The worker enters the same FIFO lock after callers that already wait there.
Those callers can join one disk commit.
Later callers wait behind the worker.

Each caller waits for its own sequence to reach the completed durable boundary.
Ordinary reads use the previous complete disk root until publication.
Private preparation reads the accepted prefix in order.
The change-feed sequence comes from that private prefix and the durable in-memory tip.
The in-memory tip changes after the disk commit.

A separate publication lock protects physical generation checks and reservations.
Compaction can publish an equivalent disk root during logical preparation.
Reservations bind to the logical frontier and format features.
Compaction preserves both values and the existing reservation ledger.
Its replacement must leave room for the queued dependency quote.
A logical write still changes the frontier and invalidates an old reservation.
Its file copy and catch-up remain outside the publication lock.

The memory publication follows request order inside one view lock.
Each request first invalidates its changed sources, then installs its clean values.
This order also protects a later write that cannot reserve warm memory.
Read holds and private write reservations still prevent the purge of held paths.
Unflushed warm-path marks and their full-path completion walk are removed.
The existing dirty status fields remain zero for compatibility.

The queue retains the existing limits: 1024 requests, the home operation limit,
and 128 MiB per complete commit.
A refused extension leaves the earlier accepted prefix unchanged.
Clients can retry a refused request after the flush.
The queue does not replay a request or its conditions automatically.
