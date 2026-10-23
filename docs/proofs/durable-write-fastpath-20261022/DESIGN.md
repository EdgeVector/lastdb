# Durable writes and notification work

The previous route acknowledged a durable product write and queued its notification.
The next client write then waited for a second complete disk commit.
The earlier timing report identifies this wait as the largest individual cost.

The native request now writes its notification and notification tip with the product data.
Both use the same existing snapshot, reservation, root publication, and final disk flush.
The change adds no journal and changes no disk format.
The mutation log keeps its existing role.

Subscribers receive the notification only after the complete disk commit.
The memory notification tip changes at that same boundary.
A refused or cancelled preparation publishes neither the notification nor the product data.
An accepted request retains its notification work through disk completion if its caller stops.

Every native notification writer takes the native writer gate before the notification tip mutex.
This order prevents an inversion between the client path and the fallback notification path.
The hard-erasure lane carries the request's notification context into its existing durable transaction.
Old notification removal also joins the same bounded native commit when retention requires it.

The route resolves schema aliases for the private match and preserves the original name in the public event.
Repeated matching inputs consume separate hints, in input order.
The integration follow-up removes the 1,024-event capture cutoff.
Native notification stages use chunks of at most 1,024 events inside one private transaction and retain one tip owner through publication.
The complete notification set retains the 32 MiB byte bound and counts toward the native transaction's operation and byte limits.
An oversized complete stage refuses before acceptance.
The existing fallback queue remains for unmatched expansions.
Those fallback commits can still compete for the shared writer.

Delete and Purge reserve memory after their notification stage.
The request boundary also checks the final quote before it uses the retained memory owner.
An insufficient owner takes the existing disk-first path; it cannot create an uncharged clean memory record.

The change also removes repeated metadata work:

- Ordered prior and current dependency lists use one linear merge.
- The generation header and root hash are computed once per receipt.
- One bounded read call reuses immutable directory pages during a publication.
- Locator edits keep separate read calls for private files that can grow.
- A directory read-budget refusal resets that call and retries once.

The change preserves the checksums and dependency validation.
It retains no new storage-object cache between requests.
Compaction still prepares and catches up outside the global write gate.
The final root handoff still shares the writer gate with client commits.

The combined commit has costs that matter before merge:

- Notification operations now count toward the client transaction's disk and operation limits.
- Notification serialization or capacity refusal can reject that complete client transaction.
- Event convergence flags describe the commit boundary and remain conservative before any later convergence wait.
- A large concurrent workload still queues behind other client commits and the final compaction handoff.
- The existing flat generation receipt still grows until compaction removes its obsolete dependencies.

This work stays on the local branch for review.
No merge, push, primary upgrade, or shared-system change forms part of this capture.
