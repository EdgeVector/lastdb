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
The change-feed sequence comes from that private prefix and the durable in-memory head.
The in-memory head changes after the disk commit.

A commit writes one level-0 run file and installs its root as the readable state.
A background merge thread merges run files. A merge root holds the same frontier, so it holds the same data.
The thread installs the merge root after each step. A read also adopts it when it captures a root.
A merged input file goes away when the last read of the older root ends.

The thread waits before its next step while held reads keep more than 1/16 of the home in replaced runs.
That wait is at most 2 seconds. After a failed merge, no new merge thread starts for 30 seconds.
A commit with CAS changes writes up to three runs: new CAS files, then the other records, then CAS deletes.
A crash between them can leave a CAS file that nothing references.
It never leaves a reference to a CAS file that is not there.
Client writes have no collection timer.
Source: [run commit and merge](../fold_db/crates/core/src/storage/disk_pack/run_commit.rs).

The memory publication follows request order inside one view lock.
Each request first invalidates its changed sources, then installs its clean values.
This order also protects a later write that cannot reserve warm memory.
Read holds and private write reservations prevent the purge of held records.
The dirty status fields stay at zero, because no record waits in memory for a flush.

The queue limits are 1024 requests, and 16,384 operations and 512 MiB in one accepted prefix.
A refused extension leaves the earlier accepted prefix unchanged.
Clients can retry a refused request after the flush.
The queue does not replay a request or its conditions automatically.
