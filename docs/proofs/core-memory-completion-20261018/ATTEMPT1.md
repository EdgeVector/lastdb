# Completion attempt 1: correct results, slower ranges

The first completion build passes the generated API proof, but its repeated bounded range reads regress.
The merge waits for a fix and a fresh comparison.
No failed request or slow sample is removed.

Before is the reviewed v4 executable `bdb83b38545a2161217392f19f9e6f6c2e0fd2d35f8f449e2d6221cb29f2cbed`.
Attempt 1 is executable `5786368cf94c243c852e42a1efbc13a27565ab7ac1502f760fa6b0bbf3931052`.
The source receipt covers 1,908 product and build inputs.
Root preserves its source in local Git commit `ea3f73edb693a33be8e190bc9d339d5dd59f9797`, tree `cb503477b2489aa5c8d64991c3aa249a8c12c737`.
That source object is local evidence, not a claim that the final public PR uses this intermediate code.
The local reference `refs/review/core-memory-completion-attempt1` retains that exact source commit.

The strict API workload passes 219 keyed memory-state checks and 30 equal-content checks.
It passes 58 zero-file-read gates and 24 additional warm equal-content gates.
The key allowance stays equal through 24 cycles of one key's create, delete, and explicit flush.
Schema maps, schema lifecycle, accepted writes, flush, reopen, and concurrent calls also pass.
These are ordinary generated-data product calls. No test suite runs.
See [operational result](operational-final/result.json).

All eight matched measurement cases pass: 696 keyed read checks and 112 measured write batches.
The cases take 226.896 seconds in total.
The first and disjoint medians have two observations. The repeated medians have ten.
The host is an M4 Max with 36 GiB. The operating-system file cache is uncontrolled.
Every case uses a new generated home, with 256 initial records per schema, four fields, and 1 KiB bodies.

Repeated read medians, in milliseconds:

| Read | v4 off | Attempt 1 off | v4 on | Attempt 1 on |
|---|---:|---:|---:|---:|
| Hash exact, one key | 0.273 | 0.313 | 0.289 | 0.300 |
| Hash exact, 64-key batch | 4.466 | 4.511 | 4.639 | 4.756 |
| HashRange exact, one key | 0.322 | 0.324 | 0.333 | 0.333 |
| HashRange exact, 64 keys | 2.074 | 2.134 | 2.146 | 2.280 |
| HashRange bounded, 64 records | 4.024 | 13.648 | 4.716 | 14.362 |

Every repeated preloaded bounded sample reports zero file-read calls, 192 warm hits, and no warm misses.
The attempt 1 preloaded bounded samples range from 14.137 ms to 14.499 ms.
The matching v4 samples range from 4.533 ms to 4.845 ms.
This repeatable cost requires investigation. Zero file reads alone do not establish fast total reads.

The write results also vary. Preloaded HashRange read-primed ack changes from 184.440 ms to 241.708 ms per 64 records.
Its through-flush median changes from 481.041 ms to 554.066 ms.
All write samples, conditions, distributions, costs, and limits remain in [comparison.json](comparison.json).
The earlier v4-versus-preceding-build report remains unchanged and uses a different reference build.

Original raw receipts remain local with their exact hashes and sizes in the [evidence index](EVIDENCE.md).
Later attempts use separate receipt paths and do not overwrite these results.
Supported exact reclaim removes all nine released attempt 1 data roots after review.
All 139 owned node sessions stop normally. The original 130 sealed receipt files retain identical bytes after cleanup.
The previous v4 executable remains in its separate owned container for the next comparison.
See [cleanup result](attempt1-cleanup-final.json).
