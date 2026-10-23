# Source review: bounded snapshot reference cache

No source blocker appears in the final cache change.
This review establishes source properties. Separate receipts must establish performance and product behavior.

| Source | Scope |
|---|---|
| `08d8436ccce9e2493e187a6b0133dc93f47c410f` | Journal-free writer; each receipt carries prior dependency parts |
| `a1941e3a8742d4da4743a02db99153119bdf034b` | Exact live counts; periodic reclaim and orphan cleanup; a fresh resolver per key |
| `e79b711517519c9ce27d0f6f1b405a364be3ba91` | The same counts and cleanup, with one bounded reference cache per publication |

The cache change touches six product files: `live_parts.rs`, `live_parts/overflow.rs`, `live_parts/scan.rs`, `tree_edit.rs`, `locator_forest.rs`, and `publish.rs`.
All reside under `vendor/laststore/src/disk_pack/`.
It changes no file format, feature bit, admission limit, warm-data policy, or cleanup schedule.
The complete change from `08d` still requires counted receipts with feature bit `512`.
The cache change adds no compatibility step beyond the existing count conversion.

## Cache lifetime and immutable bytes

`publish_prepared` constructs one `ReferenceCache` from the proposed generation before the tree edits.
It passes that cache through every changed-key resolver and the fresh-part reference pass.
The cache retains checked metadata pages, part-header bounds, and a bounded file-handle pool.
It decodes no payload body and retains no pending extent after a resolution.
There is no cache between commits and no process warm cache.

`SnapshotWriter::commit` is the sole publisher caller.
It finishes payload files and block-map headers before the cache can read them.
Tree and overflow pages receive unique addresses; later private pages do not overwrite earlier page bytes.
Page cache identities include the full directory, object UUID, and page number.
The handle pool refreshes a private file's length before each extent read.

The cache retains its original copy of `Current.directory` after the publisher selects the new directory root.
Reference resolution does not search that root.
It reads named payload maps, part headers, and overflow pages with the same fixed generation and feature checks.
The writer lease and existing generation floor retain the file-lifetime rules.

The publisher explicitly drops the cache after the fresh-part pass.
The drop precedes the empty-directory checks, final count changes, and receipt-write allocations.
An error also drops the owned cache; the existing poisoned-writer rule prevents reuse after an incomplete commit.

## Resource and work limits

| Limit | Final source behavior |
|---|---|
| Page/header reset | Reset before a reference step when the cache contains at least 24 pages or 24 part-header identities |
| Cache bound | At most 26 pages and 25 header identities; at most 32 open handles |
| Page bytes | The conservative 26-page bound is 416 KiB, plus map, path, and handle administration |
| Pending references | At most 4,096; ordinary payload work contains only the old and new extent of one changed key |
| Part counts | At most 65,536 entries per live-count or signed part map; checked count arithmetic |
| Ordinary work | A fresh `Work::publication` for each resolution; its unchanged ceiling is `4096 * (u32::MAX + 5) + 65536 * 512` |
| Legacy count pass | The existing 67,108,864-step cumulative ceiling and allowed-part checks remain before memory admission |
| Full count pass | The existing format-derived work ceiling and 65,536-directory-entry cap remain |

Each payload block and overflow page still incurs a work charge, including a cache hit.
The overflow chain retains its five-page limit and cycle check.
Map ordinals, complete object bounds, part ranges, and all part deltas retain their checks.
First reads retain the existing identity, length, generation, and checksum checks.
Later hits reuse those checked immutable bytes.

Bootstrap and full-seed calls retain their separate, standalone resolver path.
They do not borrow the publication cache or reset their cumulative work counter.
The full compaction count pass therefore remains outside this optimization.
The existing 64 MiB count-workspace quote is additional workspace, not a total process cap.
The cache lifetime extends across edits and map merges, but no second populated reference cache enters the same publication.
The warm-data byte budget and its admission rules remain unchanged.

## Root, error, and recovery behavior

The cache supplies the same signed part deltas to the existing complete-receipt writer.
The publisher still checks removals before additions and writes only positive final counts.
The complete immutable receipt precedes the atomic `CURRENT` publication.
The final root comparison, file barriers, and native durability completion remain unchanged.
An older retained receipt still protects its own files; newer counts do not authorize global deletion.
Recovery selects the complete snapshot and does not replay cache state or count deltas.
The cache adds no journal, side file, fallback root, or corruption-repair path.

## Criteria for the recommendation

- Require final-candidate exact reads, ranges, reopen, shared-part, overflow, retained-root, and backup checks to pass.
- Compare the cache candidate with both `a194` and `08d`; a gain against the slower draft alone is insufficient.
- Compare paired batch p50 and p95, single-write tails, delete requests, and post-compaction writes. Keep the raw requests and pair order.
- Require the receipt disk benefit to remain. Separate logical growth, allocation, and operating-system write counters.
- Review compaction wall time and concurrent write pauses separately. Do not infer a shorter pause from a faster ordinary batch.
- Use the separate default AUTO-on run for cleanup, retained size, latency, and maintenance effects. Keep its finite-duration scope explicit.
- Compare observed process memory with unchanged source bounds. Do not describe checkpoint peaks as a hard cap.
- Keep profile stack weights separate from matched latency. A source hypothesis alone does not establish the measured cause.

Keep the draft recommendation until the receipts establish the complete disk and latency tradeoff.
Three pairs do not establish statistical significance; one long AUTO run does not establish steady-state size.
No source review establishes power-loss behavior, primary behavior, or cloud sync correctness.
This review runs no build, node, or test command and changes no earlier proof.
