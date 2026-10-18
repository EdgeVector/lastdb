# Native disk node path

The native packed engine is the sole logical LastDB engine.
Normal startup and direct FoldDB constructors use it without an activation flag.
An empty home initializes directly. A nonempty older format receives an error before conversion or logical namespace access.
No migration path forms part of this cutover.
Cloud activation receives an explicit error. A later change must enable native backup and cloud transfer.

The [cutover results](proofs/core-clarity-20261018/RESULTS.md) identify the exact source, releases, and fresh-data checks.
The [logical warm-set plan](lastdb-logical-warm-set-plan.md) records the current component boundaries.
The primary and its archive do not participate in these checks.

## Memory and write acknowledgements

The core model owns current schemas, molecules, keys, tips, confirmed absence, and full atom values.
The recency list selects clean logical records for purge. It does not hold a second set of values.
The logical record cap defaults to 10,000. Separate allowances cover metadata, complete selected keys, and accepted write values.
Those allowances do not define a whole-process memory maximum.

A covered logical write can receive its ACK after complete core publication and before disk commit.
This memory ACK is the default when the required reservations fit.
An explicit durable request, insufficient reservation, or uncovered raw control operation waits for disk instead.
A queued response alone does not prove that its request avoided a disk wait.
Native status exposes the actual accepted and completed sequences, pending batches, and dirty logical records.

The default flush delay is 500 ms. Native admission requires a configured delay from 1 to 500 ms.
A private write queue retains prepared disk bytes and resource owners until the flush.
Ordinary reads cannot inspect that queue. Private serialized write preparation can inspect its pending records.
Captured core values retain their memory reservations until the last call releases them.

## Home fence

The node store contains `laststore-pack-node-v1`, with the exact bytes `format=1\nhome=disk-pack\n`.
The native physical home resides in its fixed `disk-pack` child directory.
Both the node store and native child contain a downgrade refusal in `laststore-layout-v1`:
`version=2\nlayout_mode=disk_pack_v1\n`.
Native startup verifies that refusal and the node marker.
The public constructor also recognizes a direct native physical home.
Neither entry selects the older engine or converts its files.

## One read boundary

One call captures an immutable typed core view and a committed disk root.
Queries in a batch share the call's work budget and temporary buffers.
Each query item retains its own result or explicit failure.
A failed native read cannot become an empty success through a fallback.
The call releases its file handles, pages, and buffers after completion.

A complete logical record can come directly from the captured core view.
A miss uses the captured disk root and can admit the fetched logical record into the core model.
An incomplete dependency receipt permits body reuse but requires normal current-tip resolution.
Read errors do not become confirmed absence.

Selected HashRange schemas can preload complete molecule keys for fast range selection.
The preload occurs on first use and remains useful while that complete key set stays ready.
The body warm set can remain partial. A partial set of bodies cannot prove range completeness.
The current flat and compound key forms share one physical prefix union; they are not selectable older layouts.

## One complete mutation

One native transaction collects its physical records and covered typed effects before publication.
The write gate selects the current comparison state for conditional writes.
Logical values and confirmed absence become visible together with their key-membership changes.
Purge operations use the same boundary.

Uncovered raw, administrative, and replay operations remain private until disk commit.
Their commit invalidates affected core dependencies and publishes key changes before success.
Only complete disk publication clears the corresponding dirty state.
This queue is disk output and resource lifetime state, rather than an alternate source for ordinary reads.

## Historical receipts

The [October 15 receipt](proofs/disk-native-node-20261015/report.md) records the earlier development path and its debug comparison.
That step required explicit activation and durable-only writes. Those controls do not describe current startup or ACK behavior.
The [7110c7f5 receipt](proofs/layer2-completion-20261016/node-final-off-summary.json) measures a later historical candidate.
The [October 16 status report](lastdb-disk-rollout-status.md) describes that candidate's earlier limits.
Neither receipt establishes current performance, primary behavior, or upgrade readiness.
