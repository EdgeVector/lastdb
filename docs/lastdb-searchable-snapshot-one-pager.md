# LastDB: searchable snapshot and mutation log for sync

Revised 2026-10-22. The local journal removal merged in PR 268. PR 271 removes erase recovery records and journal-based cloud repair. PR 273 contains receipt pruning and the bounded reference cache. Native cloud activation remains disabled.

LastDB uses a local searchable snapshot for current data and crash recovery. The mutation log serves multi-device sync. Devices can hold different local snapshots and exchange changes after a confirmed cloud snapshot. Ordinary reads and local recovery do not search or replay that log. The retained sync code does not yet establish the required common commit boundary.

The snapshot contains molecule keys, atom references, required bodies, and search pages. A read checks accepted memory first, then fetches missing logical records by key. A range names one hash and ordered bounds. It combines bounded snapshot pages with newer memory changes and deletes. Each request captures a fresh view; supported ascending keyed continuations resume after the last key. Other continuation forms retain their current rules.

A write reserves its complete memory and disk work before acceptance. It changes memory as one complete unit and returns a memory ack; some author-clock metadata requires earlier disk completion. The normal flush interval is 500 ms. A flush writes new payload files, changed search pages, and a complete generation receipt. Ordered barriers and one atomic root update select the new snapshot. A durable ack follows all required barriers, including the final macOS device flush.

Recovery verifies the selected snapshot, receipt, and dependencies. An interruption before root publication keeps the prior snapshot selected. Publication can leave either complete version selected; completed durable commits survive restart. A memory ack remains volatile until flush completion. The 500 ms schedule does not guarantee a maximum loss window. Damaged committed bytes cause an explicit error and require restore.

Each new receipt names only file parts with positive current references. Exact counts remove obsolete parts without a full database walk for each write. Shared parts remain until their last current reference goes; older receipts and read holds retain their own protection. A bounded cache shares checked maps and headers across changed keys within one commit. It retains no value bodies and drops before final count changes and receipt output. Its limits are 26 pages, 25 headers, and 32 handles, with a reset at 24 pages or headers.

Receipt omission does not delete a file. Periodic reclaim removes obsolete generations below the compaction trigger. A repeated orphan sweep handles files that no receipt names. Both paths retain age, grace, retained-root, and generation-floor checks. Active work, holds, and retries can delay deletion.

The useful property is a complete local commit boundary. Each snapshot names all required files, so recovery depends on one selected state. The current directory and locator files supply keyed and ordered access. This change keeps both. The cache reduces repeated metadata work; it leaves the 16 KiB search-page write cost unchanged. Shorter compaction pauses remain separate work.

The [cache follow-up](proofs/receipt-cache-followup-20261022/RESULTS.md) finds a large retained-space benefit with default cleanup: 1,253 MiB falls to 175 MiB. Each variant has one 12-minute run. The three controlled pairs show durable-single growth of 671 versus 639 KiB per update. Batch32 request p50 rises from 113 to 131 ms; the focused worst compaction write rises from 1.249 to 1.488 seconds. The cache cuts draft CPU cost by 19.6%, but this remains a space tradeoff.

Keep PR 273 as a draft for general release. Use existing durable batches when independent updates are ready. A larger generated home must establish older-root admission, compaction, reopen, and further writes within declared limits. The next small engine trial should identify and reduce the separate app notification publication. It needs its own order, failure, memory, and subscriber-delay review.

The warm set defaults to a 256 MiB byte budget; the former 10,000-record cap is retired. The purge removes least recently used clean records. Held and unflushed records stay protected, and their bytes remain charged during temporary excess. The approved dirty-write share is 25%, but enforcement remains unfinished. Definitions, selected keys, queues, and temporary work have separate reservations. The warm budget does not cap total process memory.

Native snapshots require feature 256; counted receipts also require feature 512. Older programs refuse 512. An older supported snapshot needs one bounded count pass before memory-write admission. Journal-backed homes receive an explicit refusal; this change provides no converter. The local backup bundle excludes node identity and encryption keys, which need separate recovery arrangements.

Cloud activation requires atomic snapshot and sync-boundary capture. Outgoing entries must retain original identities and sufficient values or durable file references. Imports must commit their progress with their value changes. Each cloud base must identify the covered boundary for every source device. Log deletion requires a confirmed restorable base and completed holds. These requirements remain open, with dirty-share enforcement and lower search-page and compaction costs.

Sources: [protocol](lastdb-snapshot-commits.md), [implementation plan](lastdb-searchable-snapshot-implementation-plan.md), [code review](lastdb-searchable-snapshot-code-review.md). The primary stays unchanged.
