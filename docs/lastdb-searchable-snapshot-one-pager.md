# LastDB: searchable snapshot and mutation log for sync

Current source: main `8a0a40b4f`, reviewed 2026-10-24.
The local journal removal merged in PR 268. PR 271 removes erase recovery records and journal-based cloud repair.
PR 273 contains receipt pruning and the bounded reference cache. A primary upgrade remains separate.
Native backup has a separate source gate that defaults off. Legacy Cloud Sync activation stays disabled.

LastDB uses a local searchable snapshot for current data and crash recovery. The mutation log serves multi-device sync. Devices can hold different local snapshots and exchange changes after a confirmed cloud snapshot. Ordinary reads and local recovery do not search or replay that log. The retained sync code does not yet establish the required common commit boundary.

The snapshot contains molecule keys, atom references, required bodies, and search pages.
A read checks the committed logical warm set, then fetches missing records by key from the matching captured snapshot.
A range names one hash and ordered bounds. A ready selected schema supplies complete keys from memory; other schemas use bounded snapshot pages.
Ordinary reads cannot use the private write queue. Its uncommitted changes remain private until complete publication.
Each request captures one consistent view. Supported ascending keyed continuations resume after the last key.

A write reserves its complete memory and disk work before acceptance. Current main commits native values and their app notices in one snapshot before the write ack. No collection timer delays ordinary native requests. A commit writes new payload files, changed search pages, and a complete generation receipt. Ordered barriers and one atomic root update select the new snapshot. The ack follows the required barriers and clean core memory publication, including the final macOS device flush.

Recovery verifies the selected snapshot, receipt, and dependencies. An interruption before root publication keeps the prior snapshot selected. Publication can leave either complete version selected; completed acknowledged native writes survive restart. Damaged committed bytes cause an explicit error and require restore. Earlier memory-ack measurements describe their recorded older source, rather than the current write path.

Each new receipt names only file parts with positive current references. Exact counts remove obsolete parts without a full database walk for each write. Shared parts and required allocator templates remain until their last reference goes; older receipts and read holds retain their own protection. A bounded cache shares checked maps and headers across changed keys within one commit. It retains no value bodies and drops before final count changes and receipt output. Its limits are 26 pages, 25 headers, and 32 handles, with a reset at 24 pages or headers.

Receipt omission does not delete a file. Periodic reclaim removes obsolete generations below the compaction trigger. A repeated orphan sweep handles files that no receipt names. Both paths retain age, grace, retained-root, and generation-floor checks. Active work, holds, and retries can delay deletion.

The useful property is a complete local commit boundary. Each snapshot names all required files, so recovery depends on one selected state. The current directory and locator files supply keyed and ordered access. This change keeps both. The cache reduces repeated metadata work; it leaves the 16 KiB search-page write cost unchanged. Shorter compaction pauses remain separate work.

The [cache follow-up](proofs/receipt-cache-followup-20261022/RESULTS.md) compares frozen sources before the later main changes. It finds retained space of 1,253 and 175 MiB with default cleanup, from one 12-minute run per variant. Three controlled pairs show durable-single growth of 671 and 639 KiB per update. Batch32 request p50 is 113 and 131 ms; the focused worst compaction write is 1.249 and 1.488 seconds. The cache cuts draft CPU cost by 19.6%. These values do not measure the later integration with current main.

The merge preserves main's concurrent compaction, exact file holds, and byte-based capacity rules. Full and tier compaction collect exact counts before publication. The integration removes the earlier directory-count and arbitrary old-root work quotas. A larger generated home still needs to establish the intended upgrade support envelope. Existing durable batches remain useful when independent updates are ready; main already combines native app notices with their data commit.

The warm set defaults to a 256 MiB byte budget; the former 10,000-record cap is retired.
The purge removes least recently used clean records. Held records retain their memory charges until release.
Native writes publish clean memory after disk commit. Their private preparation has separate reservations and no dirty warm-record state.
Definitions, selected keys, queues, and temporary work have separate allowances. The warm budget does not cap total process memory.

Native snapshots require feature 256; counted receipts also require feature 512 and count semantic version 1. Older programs refuse 512. The final writer refuses version-zero counted prototypes because their allocator references differ. An older supported uncounted snapshot needs a streamed count pass before writer admission, with format and workspace limits.

Backup manifests accept both supported feature sets and reject unknown bits. Journal-backed homes receive an explicit refusal; this change provides no converter. The local backup bundle excludes node identity and encryption keys, which need separate recovery arrangements.

Local native backup capture now saves an exact snapshot, source home, frontier, and durable file hold before network work.
Resume uses the saved cut. The separate gated native backup jobs supply transfer and restore source code.
The [native backup design](designs/native-cloud-backup-mini-v1.md) records the remaining cloud release gates.
Those components do not establish deployed cloud recovery or enable legacy Cloud Sync.
Multi-device sync still requires original mutation identities, atomic import progress, complete source-device boundaries, and safe log retirement.

Sources: [current native summary](lastdb-disk-native-node.md), [protocol](lastdb-snapshot-commits.md),
[eager queue](lastdb-eager-durable-queue.md), and [memory and disk routes](lastdb-rehydrate.md).
The [implementation plan](lastdb-searchable-snapshot-implementation-plan.md) and [code review](lastdb-searchable-snapshot-code-review.md) retain their dated scope.
The primary stays unchanged.
