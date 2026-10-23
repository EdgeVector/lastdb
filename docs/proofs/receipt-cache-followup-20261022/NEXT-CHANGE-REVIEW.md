# Next change: reduce the separate app feed cost

This source review covers `e79b711517519c9ce27d0f6f1b405a364be3ba91`.
It proposes no product change and makes no measured causal claim.
The controlled and default-maintenance results remain separate evidence.

## Current state

The app change feed follows the product write in a separate native scope.
It contains local app notification metadata; it is separate from the sync mutation log and the author clock.
Product records remain authoritative.

| Source | Current behavior |
|---|---|
| `lastdb_node/src/exec/batch_mutation_routes.rs:105` | Reserve a bounded feed queue slot before the product write. |
| `lastdb_node/src/exec/batch_mutation_routes.rs:110` | Await the product handler, then construct events from its result. |
| `lastdb_node/src/exec/batch_mutation_routes.rs:138` | Submit the events without a wait for their disk result. |
| `lastdb_node/src/exec/batch_mutation_routes.rs:180` | Event time and convergence flags come from the completed handler. |
| `lastdb_node/src/change_feed_queue.rs:83` | Combine the current backlog, then await `append_many`. |
| `lastdb_node/src/change_feed_queue.rs:85` | Log an append error; release the jobs without a retry. |
| `fold_db/crates/core/src/db_operations/change_feed.rs:86` | Hold the feed tip mutex through event/tip writes and subscriber publication. |
| `fold_db/crates/core/src/db_operations/change_feed.rs:159` | Write the events and their tip through `transaction::background_scope`. |

The queue admits 1,024 jobs; its batch caps are 1,024 events and 32 MiB.
The batch collector takes only the current backlog; it adds no collection delay.
Pending-task guards retain the submitted work until its append result.
The feed retention target is 250,000 sequence positions; its delete work is best-effort.
These bounds appear in `change_feed_queue.rs:14` and `change_feed.rs:18`.

A durable product request seals its native prefix and waits for its disk result.
See `mutation_manager/write/native.rs:58` and `storage/disk_pack/transaction/request.rs:58` under `fold_db/crates/core/src/fold_db_core` and `fold_db/crates/core/src` respectively.
Its feed job therefore cannot join that completed product publication.
The feed scope requests a 500 ms delay, but that delay requires a reserved raw-memory quota.
The feed has no such reservation: `transaction.rs:58` starts with none, and `transaction/request.rs:103` removes the delay.
The feed then seals its own accepted prefix.
A queued product request can still share its open prefix with the feed job.
Queue timing can affect the actual count.

This path explains a possible extra publication per durable HTTP request.
The observed 131 publications for 64 requests fit 64 product writes, 64 feed writes, and three clock renewals.
The observed 34 for 16 requests fit 16, 16, and two.
Those decompositions fit the source; the whole-root counters do not prove that exact attribution.

## The clock already reserves a range

Both `08d` and `e79` contain `mutation_manager/author_clock/coverage.rs:10` under `fold_db/crates/core/src/fold_db_core`.
It reserves one second and 1,024 logical counters, then reuses the same durable barrier for covered requests.
`author_clock.rs:199` also combines the current queue backlog into one durable high-water write.
The native data path waits for that clock barrier before it enters the exclusive storage gate.
A larger clock reservation cannot remove the independent feed publication.

The author clock, feed sequence, sync log sequence, native accepted sequence, and snapshot frontier have different purposes.
The author clock supplies signed conflict order and the common caller-batch revision.
See `schema/types/author_clock.rs:26` and `mutation_manager/write/cloud_entry.rs:84` under the core source directory.
Boot loads its reserved high-water state from metadata: `fold_db_core/fold_db/init_boot.rs:20`.
A larger reservation increases the skipped clock range after restart and can affect later conflict order.
It must not replace the sync cursor or the feed tip.

## Smallest next action and candidate

Use the existing caller batch API first when several independent updates are already available.
Keep durable acknowledgements; add no wait to collect future updates.
This action amortizes both product and feed work without a new storage contract.
The measured batch phases support this direction, but they do not isolate its causal benefit against an otherwise identical single-request phase.

For a later product trial, a bounded delay in the independent feed queue is the smaller candidate.
It preserves the current async relationship and uses the existing job, event, and byte caps.
Start one deadline with the first job; do not extend it for later producers.
Preserve caller order, carry handling, pending-task ownership, shutdown drain, and the pre-write queue refusal.
Measure subscriber delay and queue pressure before any recommendation.
This proposal is not an instruction to bypass the native memory quota.

Put the feed in the product transaction only after a separate semantic review.
That design could remove the independent publication, but it changes the current post-write, best-effort feed failure boundary.
Feed write failure would become a product write failure unless the design states another rule.
It also needs the final receipt fields before those fields exist today.
The feed tip mutex currently precedes the native gate; a product scope that takes them in reverse order can deadlock.
Neither candidate removes the clock-before-data barrier or changes sync log order.

## Real measurement that can establish the cause

Use fresh generated homes, one frozen executable, and the same durable and queued request sequences.
Record bounded per-publication collection attribution, operation counts, request IDs, native sequences, and clock renewals.
The existing native gate log supplies sequence and operation counts; whole-root totals alone do not identify the publication owner.
Capture the feed tip before and after all pending tasks drain.
Then compare an isolated feed change against the same source without that change.
Record commits, logical and allocated bytes, request tails, feed delivery delay, queue refusals, and process memory.
Check exact reads, ordered ranges, feed event order, graceful reopen, and the current forced-stop contract.
Keep automatic maintenance fixed for the causal comparison; measure default maintenance in a separate run.
No such feed implementation or operational comparison forms part of this source note.
