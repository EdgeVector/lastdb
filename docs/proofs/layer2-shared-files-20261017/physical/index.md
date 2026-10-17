# Physical command results

The comparison uses fresh generated homes and the saved before and after programs.
The before source is `7110c7f54ea005e33678b5c93e89353d6117b41f`.
The after source is `a1d03310e433b99c788657ef149193776bf5aff6`.
The later native warm and Delete changes do not run in these disk commands.

The [full report](physical-report.md) contains all 76 cases and every measured latency increase.
The many-map 256-write median changes from 1835.126 ms to 221.883 ms.
The final many-map locator file count changes from 256 to one after compaction and reclaim.
The ordinary one-collection cases do not establish a broad latency gain.

The 16 KiB repeated 256-write median increases from 253.539 ms to 269.446 ms.
Its p95 increases from 296.549 ms to 312.694 ms.
The 128-byte repeated 256-write p95 increases from 225.309 ms to 308.196 ms.
The [latency increases](physical-regressions.json) retain all unrounded results.

Each returned body matches its generated input.
All eight maintenance runs retain the same 4096-record digest after compaction and reclaim.
The [maintenance results](physical-maintenance-comparison.json) retain the commands, file inventories, and digests.
The [maintenance time increases](physical-maintenance-regressions.json) retain the two higher single-call durations.

The operating system cache state is unknown.
A fresh process does not establish a cold-device read.
File-work counters measure program calls, not device traffic or write barriers.
Maintenance has one matched call per operation and program.
These results do not establish power-loss recovery or cap behavior near capacity.

## Results and exact receipts

- [1 KiB inputs](physical-1k/inputs.json), [summary](physical-1k/summary.json), [calls](physical-1k/receipts.jsonl), and [completion](physical-1k/completed.json).
- [128-byte inputs](physical-128/inputs.json), [summary](physical-128/summary.json), [calls](physical-128/receipts.jsonl), and [completion](physical-128/completed.json).
- [16 KiB inputs](physical-16k/inputs.json), [summary](physical-16k/summary.json), [calls](physical-16k/receipts.jsonl), and [completion](physical-16k/completed.json).
- [Many-map inputs](physical-many-maps/inputs.json), [summary](physical-many-maps/summary.json), [calls](physical-many-maps/receipts.jsonl), and [completion](physical-many-maps/completed.json).
- [All case comparisons](physical-comparisons.json) and [higher cost counters](physical-cost-increases.json).
- [Before program hashes](before-binaries.json) and [after source and program hashes](after-freeze.json).
- [Original 51-file receipt manifest](physical-receipt-manifest.json).
- [Ordinary command recipe](../drivers/engine_bench.py), [many-map command recipe](../drivers/engine_many_maps.py), and [maintenance command recipe](../drivers/maintenance_bench.py).
- [Complete comparison recipe](../benchmark-recipe.md).

The original 51-file manifest remains valid relative to this folder.
The three command files also remain here to preserve that manifest.
The copies in `../drivers` contain the same bytes.
The proof folder contains no generated home, generated value file, or program binary.
