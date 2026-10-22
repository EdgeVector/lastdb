# LastDB storage code review and implementation preview

Reviewed 2026-10-22 against main `9f0ab569b65ba8ff17cdc6902cdc7315aaae3d86`.
This pass reads source and saved operational receipts. It changes documents only.
No new node, crash exercise, build, or runtime measurement ran in this pass.

## Current facts and target changes

| Area | Current source | Target or remaining work |
|---|---|---|
| Local writes | `HomeJournal::commit_mutation` appends an internal journal batch, then publishes searchable pages | Write ordinary payload files and changed pages directly |
| Recovery | Writer open recovers complete journal batches before new admission | Open the selected complete snapshot without mutation replay |
| Current record bytes | A committed locator can point into a journal file | Convert those extents to ordinary payload files before journal removal |
| Publications | A data publication is followed by a fresh-stream publication and a second receipt | One receipt and one root publication per complete committed group |
| macOS durability | Bounded file sets, ordered publication barriers, then one final full device flush | Preserve this machinery around the new writer |
| Warm records | No 10,000-record cap; default byte budget is 256 MiB | Preserve the byte policy and counted temporary overshoot |
| Dirty share | The approved 25% threshold appears in status; admission enforcement remains incomplete | Complete write admission and durable-first fallback |
| Key preload | Selected HashRange schemas can opt into a complete membership-key load | Preserve the option and its separate 128 MiB default allowance |
| Ranges | A common view merges accepted changes and bounded disk pages | Preserve value, delete, order, and view semantics |
| Query continuation | Supported ascending keyed pages use a last-key cursor; other forms use offsets | Preserve that distinction; add no cross-request snapshot promise |
| Cloud sync | Native startup and activation refuse cloud configuration | Implement and verify atomic snapshot/log capture before activation |
| Capture | Retained asynchronous capture follows local work | Replace that ordering for required durable sync capture |
| Backup and restore | Native bundles still contain and validate internal-journal state | Capture a snapshot root and its required sync prefix instead |
| History | Queries refuse `as_of`; conflict records remain for 30 days by default | Preserve that exception and its required file references |
| Counters | Atom, blob, and molecule counters remain live correctness data | Retain them through conversion and the new commit path |

The 10,000-record workspace rule predates the October 19 byte-limit decision.
Other 10,000 values still bound individual query work or separate caches.
They do not restore a warm-data record count limit.
The [byte-limit decision](lastdb-warm-set-byte-limit-design.md#decisions-tom-2026-10-19) explicitly replaces the old rule.

## Updated performance evidence

PR 266 is already on the reviewed main revision.
Its [operational report](proofs/native-commit-barriers-20261022/RESULTS.md) compares old main with the candidate for the macOS barrier change.
It uses 1,000 queued requests across 64 schemas, one client, and 64-byte values on APFS.

| Whole-request latency | Old main | Barrier candidate |
|---|---:|---:|
| Median | 142.88 ms | 30.00 ms |
| P95 | 273.23 ms | 42.96 ms |
| P99 | 351.34 ms | 58.06 ms |
| Maximum | 1,430.70 ms | 1,688.54 ms |

All 1,000 requests return memory acks in each run.
They are not durable-write latency samples.
Memory acks can still wait for an author-clock disk commit.
The report includes a separate durable mutation and forced-process-stop recovery receipt.
That receipt does not establish power-loss behavior.

The older [512-record measurements](proofs/write-overhead-20261021/summary.json) use `8067843c4` and 1 KiB bodies.
They measure growth, process I/O, and explicit compaction with a different request schedule.
Keep their 94.7% metadata growth and compaction timings labeled historical.
The October 22 report does not repeat that growth workload.
Unchanged disk format does not prove unchanged per-workload growth: different request speed can change flush groups.

The earlier estimate of several removable full device flushes per commit is obsolete on current macOS.
Duplicate receipts and root work remain, but their current speed benefit needs a new measurement.
The maximum request latency still exceeds one second in the new report.
Journal removal alone supplies no proven compaction-pause reduction.

## Code preview

The current stored-batch route is:

```text
HomeJournal::commit_mutation
  complete_native_commit
    append internal journal data and tail
    publish changed directory and locator pages
    rotate stream and publish another receipt/root
    finish the platform durability barriers
  install committed disk view
  complete the queue receipt
```

The proposed route is:

```text
SnapshotWriter::commit
  complete_native_commit
    write ordinary payload files
    write changed directory and locator pages
    write required sync entries and durable references
    write one generation receipt
    order dependency writes and publish one root
    finish the platform durability barriers
  install committed disk view and retire covered memory changes
  complete the queue receipt
```

This is a control-flow preview, not compiled replacement code.
Keep author-clock reservations and their existing durability requirements before a memory ack.
The sync sender receives the durable-root handle only after the final barrier completes.
An uncertain commit stops new write admission until the selected root resolves its identity.

Reuse these implementation parts:

- Ordinary payload output in `PackWriter::new`.
- Directory and locator page copies and existing keyed readers.
- `publish_current` and its ordered publication-file barrier.
- `complete_native_commit`, `durability/ordered.rs`, and the macOS filesystem dependency.
- Root lifetimes, backup references, exclusive writer ownership, integrity checks, and reference counters.

Replace the journal-first writer and its startup, maintenance, backup, and restore requirements.
Do not remove the sync log or its required retained bytes.
An explicit converter must handle old `Source::Journal` extents before the new serving reader drops that source type.

## Document corrections in this pass

- Update the one-pager and plan to the reviewed source revision.
- Separate the historical growth measurements from the newer barrier report.
- Keep the retired count cap retired and the 25% dirty-share work unfinished.
- Preserve optional key preload and existing ascending-versus-offset continuation behavior.
- Carry the current ordered durability helper into the proposed writer.
- Replace stale warm-count wording in the canonical model and storage-boundary document.
- Preserve current conflict records and counters in the boundary description.
- Keep new queue, workspace, page, sync, and disk limits explicitly proposed.

## Next implementation boundary

The first engine change should add a journal-free format and direct commits on fresh owned homes.
It must include complete stored batches, root selection, memory-view publication, and durable receipts.
Conversion, journal-free backup, and sync capture follow under the same explicit format boundary.
Compiler-guided deletion comes after those dependencies move.
The [implementation plan](lastdb-searchable-snapshot-implementation-plan.md) lists the files, proposed limits, and required operational evidence.

Source review does not establish live results.
No primary data, archive, shared service, CI setting, or release setting changed in this pass.
