# Completion attempt 2: preload recovers, default ranges still regress

The second completion build passes the unchanged generated API workload and all eight matched measurement cases.
Preloaded warm range reads return to the previous build's time. The default disk-key route still needs a fix before merge.

The candidate executable is `031caa98bc8e45ff21bdb3b08406a1a068a05268580bb58ca0513d593b0ebd11`.
[completion-final-identity.json](completion-final-identity.json) preserves its 1,908 product and build inputs.
The local source commit is `79745fb27aebad2d2d61644a71b6e333cca40cf7`, tree `79b43cc7d9bb652dbbf294d541b1f6405ba17309`.
The local reference `refs/review/core-memory-completion-attempt2` retains that exact commit.
The comparison reference remains the reviewed v4 executable `bdb83b38545a2161217392f19f9e6f6c2e0fd2d35f8f449e2d6221cb29f2cbed`.
These results do not replace the earlier v4-versus-preceding-build report.

The strict workload again passes 219 keyed memory-state checks and 30 equal-content checks.
It passes 58 zero-file-read gates and 24 additional warm equal-content gates.
The key allowance stays equal through 24 create, delete, and explicit-flush cycles of one key.
All nine runs stop their 139 owned node sessions normally.
See [operational result](operational-attempt2/result.json) and [receipt seal](attempt2-receipt-seal.json).

The eight measurement cases take 227.575 seconds. All 696 keyed read checks and 112 measured write batches pass.
Round 1 uses current-on, before-on, current-off, and before-off. Round 2 reverses that order.
The fixed generated data and request method remain the same as attempt 1.
Each first or disjoint median has two observations. Each repeated median has ten.
The operating-system file cache remains uncontrolled.

Repeated bounded HashRange reads, 64 records, median milliseconds:

| Policy | Reviewed v4 | Attempt 2 |
|---|---:|---:|
| Preload off | 4.004 | 8.677 |
| Preload on | 4.922 | 4.942 |

All ten repeated preloaded reads report zero file-read calls.
The default route still recreates a candidate key set for each returned record, according to the source review.
The next change must reuse only a fully authorized matching tip, with equal entry details and the exact selected atom location.
It must not create new lookup authority from a partial receipt.

Write results vary. The read-primed HashRange ack median is 193.600 ms off and 230.659 ms on.
The matching v4 medians are 184.197 ms off and 224.174 ms on.
The full write conditions, through-flush costs, distributions, file costs, and limits remain in [comparison.json](attempt2/comparison.json).
No sample or failed candidate is removed. The merge waits for the remaining default-route fix and a fresh comparison.
