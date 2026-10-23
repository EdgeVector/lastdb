# Review of exact live parts in snapshot receipts

The baseline is `08d8436ccce9e2493e187a6b0133dc93f47c410f`.
The final candidate product source appears in the [build receipt](after-build.json).
Both versions use the journal-free snapshot writer.
This review describes source guarantees. Separate product receipts establish the generated-data results.

## Current change

The old publisher copies every dependency from the prior receipt and adds its fresh parts.
An obsolete part therefore enters every later receipt until compaction replaces the inventory.
The candidate carries positive live counts instead.
It writes one complete immutable receipt before the atomic `CURRENT` publication.
The change adds no side file, replay chain, internal journal, or mutable recovery record.

Each normal tree page contributes one reference to its metadata part.
Each external key descriptor contributes references to the actual overflow pages in its chain.
Each live payload extent contributes one reference per logical block to that block's map part and data part.
Several records can share a block or a part. Their counts retain the shared bytes until the last reference goes.
Tombstones contribute no payload reference.

`TreeEdit` retires each replaced page's metadata and overflow references.
It changes payload references only for keys that the batch inserts or replaces.
The publisher reads its fresh private metadata pages to add their metadata and overflow references.
Unchanged payload values require no map reads when their search page receives a copy.
Old payload files and unrelated old tree pages receive no append or full walk.

The resolver reads block maps and part headers, without payload body decode.
It follows actual overflow next-page links, with a five-page chain bound.
It retains the map part that supplies the extent's complete object bounds.
A mixed live and obsolete part stays in the receipt.
One record that crosses a data-part boundary keeps every required part.

## Publication and lifetime

The candidate writes only positive counts into the new receipt.
It checks count additions and removals before receipt publication.
Receipt verification compares immutable file identities independently of each snapshot's count.
File length, creation generation, checksums, and complete `CURRENT` identity retain their prior checks.

The new receipt does not inherit a part whose count reaches zero.
An older receipt still names its own dependencies.
Reclaim uses the combined coordinates of all retained roots; it does not use a newer count as a global deletion instruction.
Read holds, durable references, grace periods, and the concurrent generation floor retain their existing rules.
Receipt removal alone does not remove a file from disk.
The automatic worker schedules reclaim at a 30-second idle interval, even below its compaction trigger.
It uses the existing installed-root capture and bounded drain, without a pack.
Explicit retry deadlines take precedence over the idle timer.
The orphan sweep also repeats at a 30-second to ten-minute interval, with a ten-minute default.
It retains the configured minimum file age, grace cutoff, retained roots, and concurrent generation floor.
Repeated native writes can create private payload parts that no final receipt names.
Ordinary receipt reclaim cannot discover those parts; the separate bounded orphan sweep can remove them.
Packs, active writes, holds, grace periods, and error backoff can delay cleanup.
Automatic maintenance off still disables the worker.

Recovery still selects the complete snapshot from `CURRENT`.
It neither replays a count delta nor reconstructs an absent receipt.
Private output from an incomplete commit remains outside the selected snapshot.
The ordered file barriers and final macOS device flush remain in the commit route.
The product operations do not establish power-loss behavior.

## Format and limits

New receipts require feature bit `512`.
Each existing 80-byte dependency record stores its positive count in bytes 24 through 31.
An older binary refuses that feature before writer file creation.
The snapshot format still requires bit `256` and refuses retired journal bit `2`.

An older supported snapshot receives one full metadata count pass at writer open.
That pass precedes memory admission and transfers its counts into the first new receipt.
Later counted commits use page and changed-key deltas.
New full builds and compaction collect counts from their complete fresh trees.

| Limit | Source behavior |
|---|---|
| Dependency parts | At most 65,536 per receipt and signed part map |
| Pre-ack part admission | Existing parts plus worst-case additional parts must fit the existing ceiling |
| Pending references | At most 4,096 per collector; a page drains before the next page enters |
| Ordinary payload delta | At most the old and new extent of one changed key before resolution |
| Count arithmetic | Checked integer addition and subtraction; zero counts leave the receipt |
| Resolver cache | Reset at 24 pages or 24 part-header identities |
| Older snapshot count pass | At most 67,108,864 page or reference steps, before writer admission |
| Full count pass | At most 65,536 directory entries, including new builds and compaction |
| Full new snapshot work | A finite ceiling derived from the metadata and extent format bounds |

The additional normal count workspace has a conservative 64 MiB quote.
It includes one live-count map, two simultaneous signed part maps, pending references, receipt vectors, and resolver caches.
The existing mutation inputs and search workspaces have separate budgets.
This quote is not a total process memory cap and does not change the logical warm-data byte budget.
An old receipt at the part ceiling can still need compaction before its first counted write.

## Checks and proof limits

The local format, full workspace Clippy, URL lint, and source size checks pass on the candidate source.
No tests ran, under the repository rule.
The operational sequence requires reclaim after incremental updates, without compaction first.
This order prevents a full count pass from hiding an incremental count error.

The separate generated operations cover shared data parts, shared map parts, a record across data parts, and external keys in leaf and parent pages.
They also cover older snapshot count collection, older binary refusal, retained roots, earlier backup restore, and restart after an explicit flush.
The automatic-maintenance operation distinguishes periodic cleanup from the startup drain and checks removal without a pack or manual reclaim request.
The workload does not exercise a payload map part above part zero, the maximum-size home, or the workspace ceiling.
No primary, archive, cloud, or physical SSD write claim is part of this report.

The [independent operational audit](independent-operational-audit.json) checks final candidate `a1941e3a8` and its exact binaries.
The [final report](RESULTS.md) records correct data with lower file growth and slower batch writes across three pairs.
The automatic operations use zero grace and startup delay; the orphan retry uses a ten-second age cutoff.
They do not establish default cleanup latency or a concurrent-reader race stress result.

The source map is `disk_pack/live_parts.rs`, its five child modules, `tree_edit.rs`, `locator_forest.rs`, `publish.rs`, and `generation.rs`.
The removed helpers are `Dependencies::from_coordinates`, the conservative generation writer, and the unused three-result `TreeEdit::finish` adapter.
The change keeps the directory, locator, payload, block-map, generation, and `CURRENT` files that the searchable snapshot still needs.
