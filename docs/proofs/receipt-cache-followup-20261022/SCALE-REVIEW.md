# Scale and admission review

This source review covers candidate `e79b71151` against baseline `08d8436cc`.
It starts no workload and supplies no larger-home performance evidence.
The default run supports a space benefit for its measured home.
It does not establish general capacity or large-home upgrade support.

## What the directory cap counts

The 65,536 limit counts physical directory coordinates: one collection name plus one placement group.
It does not count logical records, schemas, databases, or warm records.
The native placement function selects one of 1,024 groups per physical collection.
See `vendor/laststore/src/disk_pack/home.rs:99`.

Core schema and database data share structural storage collections.
They do not allocate one physical collection per logical record or schema.
The core opens its named namespaces in `fold_db/crates/core/src/db_operations/core/open.rs:49`.
The main namespace routes keys to structural collections in `fold_db/crates/core/src/storage/laststore/key_routing.rs:6`.
This model reduces directory growth relative to the logical record count.
It does not prove a universal upper bound on physical collections.

The native namespace API accepts arbitrary names and has no total namespace-count check.
See `fold_db/crates/core/src/storage/disk_pack/mod.rs:178`.
Sixty-four fully populated physical collections use 65,536 coordinates before any additional collection needs space.
This arithmetic is a directory limit, not a supported database-size estimate.

The 65,536 receipt-part ceiling does not imply the same directory-entry ceiling.
Several locator roots can share one file part through `LocatorForest`.
See `vendor/laststore/src/disk_pack/locator_forest.rs:23` and `:47`.
Ordinary counted publication has no corresponding global directory-entry admission check.

## Existing compaction limits

Baseline and candidate both enforce 65,536 directory entries in the compaction space quote.
See `vendor/laststore/src/disk_pack/maintenance/quote.rs:19`.
The quote also refuses more than 67,108,864 locator records across the home.
The compaction copy refuses more than 1,000,000 records in one directory coordinate.
See `maintenance/quote.rs:8` and `maintenance/compact.rs:101` under the same disk-pack source directory.
These existing limits already restrict general scale.
They are separate from the candidate's live-count work limits.

Compaction does not remove old directory coordinates to reset this allowance.
It copies every selected directory entry and preserves locator tombstones.
See `vendor/laststore/src/disk_pack/maintenance/compact.rs:50` and `:111`.
A smaller file count after pack does not prove a smaller directory count.
The candidate adds its complete count pass before it publishes the replacement root, at `maintenance/compact.rs:70`.

## New admission risk for an older root

An uncounted root now needs a complete metadata count pass at writer open, before any memory acknowledgement.
See `vendor/laststore/src/disk_pack/snapshot/writer.rs:41`.
That pass refuses more than 65,536 directory entries at `live_parts/bootstrap.rs:160`.
It also has a separate 67,108,864-step ceiling at `live_parts.rs:55`.
Steps include page, key, overflow, and payload-block metadata work.
They are not equivalent to the existing compaction record count.

A valid older home can therefore refuse candidate writer open even when the older writer can serve it.
The existing compaction directory cap does not remove this upgrade risk.
A counted root skips the open-time full count pass; later full compaction still uses the stated limits.
No result from the small operational homes establishes safe admission for every intended older home.

## Recommendation and next evidence

The measured space gain is credible for the measured workload.
An explicit space/latency tradeoff can describe that result.
Keep the PR as a draft for a general release recommendation.
Final cache-maintenance results and a representative larger generated home must support that recommendation.

Use actual product operations on a new owned home; do not copy the primary or its archive.
Represent the intended key/hash spread, value sizes, schemas, and update/delete history.
Complete real compaction, exact reads, ordered ranges, reopen, and continued durable writes afterward.
Also open a representative uncounted home from the accepted older executable and verify its first counted publication.
Record directory entries, retained parts, records, space reserve, refusal details, process memory, and request pauses.
State the measured support envelope; do not call the result unlimited support.
No implementation, node, build, or new operational run forms part of this review.
