# LastDB: searchable snapshot and sync-only mutation log

Revised 2026-10-22. The local snapshot writer merged in PR 268 as `4118c7baa`.
The follow-up in PR 271 removes the separate erase recovery records and journal-based cloud repair.
The sync contract below remains a design requirement. Native cloud activation stays disabled.

LastDB needs two durable data structures: a local searchable snapshot for current data, and a mutation log for sync. Each structure can use several files. The snapshot supplies local reads and crash recovery. The mutation log retains changes after the latest confirmed cloud snapshot. Devices can hold different local snapshots and still exchange those changes. Ordinary reads and local recovery never search or replay the mutation log.

This branch removes the internal journal, its tail files, rotation, and replay. The snapshot writer creates ordinary payload files and changed search pages. It writes one generation receipt and publishes one root for each complete stored batch. It retains the grouped macOS disk barriers from PR 266 and the concurrent file cleanup from PR 267. The new operational receipts cover generated records, ranges, compaction, reopen, and a forced process stop. They establish no power-loss or performance result.

The second review also removes erase recovery records and journal-based cloud repair.
Erase accounting stores the final aggregate values with the delete in one snapshot commit.
The live debit follows the durable root. No debit replay or pending operation remains.
The cloud-repair commands, peer adoption records, and restart workers are removed.
Retired repair state and cloud generations that need adoption receive an explicit refusal.
The sync mutation log and authenticated retired sync source records remain.

The repeat benchmark uses three fresh-home pairs against the original journal-backed baseline `6d8aa0d57`. Durable-single request p50 falls from 86.1 to 48.9 ms. Process write bytes per durable update fall from 912.0 to 723.3 KiB. Batch latency shows mixed results.

The focused compaction write delay remains about 2.2 seconds. Automatic maintenance is off in both versions. The [repeat report](proofs/journal-removal-review-benchmark-20261022/RESULTS.md) separates these results from the [first report](proofs/journal-removal-benchmark-20261022/RESULTS.md). Neither comparison isolates the extra erase-meter removal.

The snapshot contains current molecule keys, atom references, required bodies, and search pages. A read checks accepted memory first, then fetches missing logical records by key. A range supplies one hash and ordered bounds. It merges bounded snapshot pages with newer memory changes and deletes. Each request captures a fresh view; ascending keyed continuations use the last key, while other forms retain their current offset rules. Optional selected-schema key preload remains supported under its separate budget; neither read route uses the sync log.

A local write reserves its complete memory and disk work before acceptance. It updates memory as one complete unit, then sends the memory ack. The normal flush interval stays 500 ms. The flush writes changed bytes and search pages. Ordered file barriers and one atomic root update commit the snapshot. The durable ack follows all required barriers, including the final device flush on macOS. Author-clock metadata can require disk completion before a memory ack.

Each new receipt keeps only file parts that its selected snapshot still needs.
Exact live counts remove obsolete parts from the next receipt without a full database walk per write.
A shared part stays until its last current reference goes.
Older receipts and read holds still protect earlier snapshots until reclaim can delete their files.
The change keeps the current 16 KiB search pages and their write cost.

A crash before the root update leaves the previous snapshot selected. A crash during publication can leave either complete version selected. A completed durable commit survives restart. Recovery opens the selected snapshot without mutation replay. A memory ack remains volatile until the flush completes; 500 ms is a schedule, not a guaranteed maximum loss window. Corrupt committed bytes require an explicit error and a restore procedure.

Before cloud activation, required sync entries must commit with the snapshot boundary. The sender must export only covered entries, with original mutation identities and sufficient values or durable file references. An import must commit received progress with its value changes. The cloud snapshot must record a boundary for each source device. Covered entries can leave disk after cloud confirmation of a restorable replacement base and the end of required holds. The retained sync code does not yet establish this complete contract.

The warm set defaults to a 256 MiB byte budget; the 10,000-record cap is retired. Each read enters the warm set, and the purge removes least recently used clean records. Held and unflushed records remain protected, so the charge can temporarily exceed the budget. The purge returns it toward the budget after calls end or writes reach disk. The approved dirty-write share is 25%; its enforcement remains implementation work. Separate reservations cover definitions, selected keys, queues, and temporary work; storage objects leave memory after their call.

Startup, backup, compaction, and restore use snapshot roots.
The local backup format is `lastdb-native-backup-2`; it excludes node identity and encryption keys.
Mandatory feature `256` distinguishes the journal-free format.
New receipts also require live-count feature `512`; older programs refuse that feature.
An older supported snapshot receives one bounded metadata walk before the writer accepts a memory write.
The first commit stores those counts; ordinary later commits use page deltas.

Old journal-backed homes receive an explicit refusal before writer file creation.
No old-native-journal converter exists in this change.
Fewer search files, shorter compaction pauses, dirty-share enforcement, and atomic sync capture remain separate work.
The primary stays unchanged.

Sources: [snapshot protocol](lastdb-snapshot-commits.md), [implementation plan](lastdb-searchable-snapshot-implementation-plan.md), [current code review](lastdb-searchable-snapshot-code-review.md), [operational receipts](proofs/snapshot-commits-20261022/RESULTS.md). Historical measurements: [October 22 barrier report](proofs/native-commit-barriers-20261022/RESULTS.md), [October 21 growth receipts](proofs/write-overhead-20261021/summary.json). The historical workloads do not measure this writer.
