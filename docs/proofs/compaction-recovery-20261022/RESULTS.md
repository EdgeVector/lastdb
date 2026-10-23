# Compaction refusal and primary recovery — 2026-10-22

The installed primary used commit `f02ffcef6`. Its compaction quote stopped after 1,000,000 records or 65,536 directory entries. The copy step also stopped after 1,000,000 records in one hash group.

The old writer set its failure mark before compaction. Any compaction error kept that mark. The node then set its own failure mark and refused record calls. The health route returned a fixed success response.

The source confirms that the quote runs before replacement output starts. A count refusal leaves the previous CURRENT in place. A restart clears the process failure marks.

Tom authorized a primary restart and code changes in this chat. The primary stopped through its LaunchAgent and restarted with the same binary. Its process changed from 18281 to 78538. One durable record write succeeded. A read by the exact key returned the written value. The receipt is in `primary-restart.json`.

No primary data copy or reset occurred. The archive stayed unchanged. No brain, situations, or kanban command ran.

The main fix removes compaction record and directory-entry count caps. Per-path I/O budgets, cursor validation, disk-space admission, checked byte totals, and dependency-part limits stay active. The health route returns HTTP 503 when the record-admission failure mark is set.

Main already clears writer poison after a failed compaction when CURRENT stays unchanged. The main fix retains that behavior. The compatibility branch adds that behavior to the installed primary source without a disk-format change.

Format, diff, code-size, new-allow, byte-slice, and URL checks passed. The required GitHub CI and full-workspace compiler lint passed for the main code change. Tests stay retired.

The local compiler slot was unavailable for 600 seconds. That build command exited before compilation. The generated-home product run beyond both former caps remains pending. The compatibility build was canceled before compilation because its upgrade workflow requires prohibited app calls. The source stays on `hotfix/compaction-primary-20261022`, commit `abaa1fbb82b2635c56838699cebdc80c546e21b1`.

The primary remains on its recovered, existing program. The mandatory upgrade workflow requires prohibited brain and kanban calls. Its instruction says: "Do not hand-roll a weaker path." No binary cutover ran.

A full compaction still holds the write gate. This fix removes false count refusals; it does not shorten a large rewrite.
