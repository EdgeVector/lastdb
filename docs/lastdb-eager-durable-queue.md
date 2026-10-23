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
Its file copy and bulk catch-up stay outside the publication lock.
The private output keeps exact file reference counts after each catch-up round.
A new hash group adds only its locator subtree to those counts.
The output retains every file with a positive count until publication.
The final disk-root check still requires an exact match.

Continuous writes can change that root before the prepared replacement installs.
The compaction then offers its private output to the existing write queue.
The next physical commit combines a bounded recent delta with its new mutations.
It builds one set of metadata and publishes one complete disk root.
The delta keeps each original mutation version and each tombstone.
The new mutations receive the next normal frontier.

The final root capture starts a temporary exact change capture in the existing writer.
Each successful physical commit contributes only its new keys and file locations.
One generation hold protects each captured commit, including after another physical rewrite.
No payload bytes, page caches, file handles, or disk journal enter this capture.

At most two captures exist, each with at most 4096 operations and 16 MiB of metadata.
The memory charge includes key lengths and conservative commit-hold overhead.
The final merge reads this bounded metadata without file access or old record visits.
It also obeys the home operation limit and reserves extra metadata space first.
A refused merge leaves the accepted client ticket intact for its normal commit.
The compaction reports that refusal; it does not claim success.

A capture accepts only a continuous sequence of complete roots from its exact start.
It retains original mutation versions and tombstones.
Tracking failure invalidates only the optional capture, after the ordinary disk guarantee stays complete.
The writer never converts a successful durable commit into an ack failure because capture allocation fails.
The output owns the original arrays until its off-lock drop.
The registry also retains each capture until explicit off-lock retirement.
A temporary writer reference therefore cannot free these arrays under the write lock.

The queue keeps the offered output across the current worker's delivery phase.
Only the worker that takes a physical prefix claims that output.
An idle check after 100 ms can return unclaimed output to off-lock preparation.
This check delays only maintenance. Client writes have no collection timer.
The bounded final delta adds work to the normal metadata commit.
The commit reports this work as `compaction_merge_prepare_us`.
CI must measure its effect on client latency and require active compaction progress.
The capture must publish in the final quarter of the write interval.

An independent task owns the offer through a canceled call or a failed writer.
It releases an unclaimed offer outside the write lock when the writer fails.

The private source holds stay active through the complete merged publication.
A merged rewrite invalidates older file deletion plans before the disk-root rename.
The worker drops the private output after it releases the write locks.

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
