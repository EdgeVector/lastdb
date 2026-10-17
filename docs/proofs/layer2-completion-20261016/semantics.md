These earlier component checks use generated data only. Their last source is `08409a220e57364052d2eca030eafd12006e6bc4`.
The [final batch report](batch-semantics.md) covers the later completed node source.
This component daemon SHA-256 is `a350ae68759889e5e55c8b3251abe77226e18d4e4706bd4ab77a374f4e053417`.
The daemon reports `0.23.4-pre.0-239-g08409a220-dirty`. The suffix reflects receipt and document edits during the build.

The node uses native write mode, a 500 ms flush interval, and a 10,000 logical-record cap.
The final home starts empty under candidate `55c6d9aed`. Later candidates reopen that same generated home.
No primary data or identity key enters these artifacts.

- All eight final response comparisons match the full JSON, including the added bucket field, field metadata, key form, and page fields.
- Repeated exact, descending exact, projected exact, and descriptive-alias reads show zero file opens, read operations, payload bytes, and metadata operations.
- A projection change can use the normal disk path to restore query-specific metadata. The same records keep their correct values.
- The final write returns `durability: queued`. Native status still shows an accepted batch, and the immediate query returns `final-source-value`.
- An explicit flush returns durable. After a clean stop and reopen, the full JSON for that write remains equal.
- The final exact batch returns the repeated-key winner, the recreated record, and an empty result for the absent key.
- The bounded range under one hash returns records 0 through 2 with a limit of three.

The detailed mutation sequence runs on component source `70295dcbb6f3814d34a84f7d48b26952dccacd53`.
Its daemon SHA-256 is `7516e78a1c54152ab49b0136dcb39b75b5280138fdf976c1c31e939514123572`.
A conditional write returns queued and becomes visible before the flush. A stale condition returns HTTP 409 with `cas_conflict`.
A delete removes record 7 from reads. A create with the original bytes restores it. The final program reopens both results.
The three repeated-key mutations return three IDs. The existing author-clock rule gives the batch one clock.
The UUID tie-break chooses `repeat-A`, whose ID starts `c00ea52b`. The final reopen retains that value.
The initial run chooses `repeat-B` from different IDs. Its old generated home also reopens with that correct winner.
See `author_clock.rs:250` and `atom/mod.rs:47` for the existing order rule.

Five separate startup cases refuse with exit 1: flush intervals 0 and 501, a 1 MiB disk cap, cap 0, and a non-integer cap.
These cases use source `d1fae6469` and daemon SHA-256 `bd18e500cd961a1867664ed843c16389921bb44a8e8cd8d2adadeee192e5a2d6`.
Each exact file inventory contains startup metadata only. No native disk directory, CURRENT file, or native format fence exists.

The 70295 status receipt reports 19 logical records against the 10,000 cap. The final status receipt reports zero.
Both receipts report zero held or excess records.
This small workload does not prove behavior at a full cap. The code enforces that cap; these receipts show ordinary occupancy.
The normal flush worker also clears an accepted batch without an explicit flush in the earlier component run.
The receipt interval does not measure a strict 500 ms completion bound.

The earlier receipts retain both actual defects: the first warm response omits bucket, and the next candidate loses key-form metadata after a projection change.
The final response comparisons show both fixes. Initial and intermediate summaries keep their incomplete labels.
The request files are under `requests/semantics-*.json`. The response and receipt files use the `semantics-` prefix.
Documentation copies of HTTP headers use LF and one final newline. The supported helper then removes the owned scratch after receipt preservation.
No tests, fixtures, failure injection, or primary operations run for this proof.

The supported exact-path cleanup succeeds with exit 0. The owned scratch no longer exists.
The cleanup receipt records UID 501, the exact realpath, and no owned node process before removal.
See `semantics-cleanup.json` and `semantics-cleanup.log`.
