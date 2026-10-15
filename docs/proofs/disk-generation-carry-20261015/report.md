# Generation carry receipt, October 15, 2026

Source: `3de56ce98d1941711d44489e779115add801d68d`.
The change reduces repeated part-metadata reads within one exclusive writer. It does not change the saved format.
No primary, primary copy, archive, factory, shared service, tests, fixture harness, or failure injection participates.

## Binaries

| Binary | SHA-256 |
|---|---|
| Candidate release `lastdb-disk-home` | `b3240d841102a9f43496265a147b26ba1a18447a14cb8aef4d8b7c9197317f9c` |
| Candidate release `lastdb-journal` | `3af26b4f7dc275da5ce46f0495b2484d8ec3d88273b52429d1310ebe240d681e` |
| Candidate debug `lastdbd` | `6bf567d1cc78278f454356f98a22c84449b6f96b9b5f1060697a448a5dcc18f2` |
| Prior release home command, PR #197 | `3a9b6c9a5070d00672f46de52d27daa5885ddeded3cd5255d016ed7c21bcd5f5` |
| Prior debug node, PR #198 | `c7103e15a3f66996a9e5010d119017ef2b16e55867da0d05cbb5e6ac76b04924` |

## Generated home commands

The prior home command builds two fresh homes from the same generated input.
Each home contains 25,600 records across 64 named collections, with one encoded hash per collection and fanout 1.
Each hash has 400 keys; every fourth key is a tombstone. Each initial home has 193 data and metadata parts.
No database or identity is copied.

Each home then accepts two ordinary complete batches of 192 operations.
Each batch changes one existing key, deletes another, and adds or changes a named extra key in every collection.
The old command writes one home. The candidate command writes the other.
Both reach complete frontier 2 and generation 7.

The second pair runs sequentially after all owned nodes stop:

| Complete write command, including reopen | Prior | Candidate |
|---|---:|---:|
| File opens | 4,273 | 1,277 |
| Read operations | 2,710 | 1,016 |
| Format bytes | 36,439,424 | 15,057,280 |
| Journal recovery bytes | 11,151 | 11,151 |
| Command time | 691.324 ms | 595.793 ms |

The first pair also reduces file opens, from 3,342 to 1,144.
Its times are 687.470 ms and 793.734 ms. The candidate overlaps an owned node's normal shutdown, so that pair does not establish a latency improvement.
These are individual command samples. OS-cache state and other host work remain uncontrolled.
The repeat pair supports a reduction in userspace file work. It does not establish production latency percentiles.

Three 384-coordinate reads return the same expected results: the old home, the candidate home, and the prior reader on candidate output.
Each returns 192 absences and 64 values of each expected body: unchanged, changed, and newly added.
Both complete hash ranges return 300 live records in three pages and examine 401 keys.
They report the same 234,937 call file bytes and 30 command file opens.
Ordinary reads report zero group opens, legacy replay, journal replay, footer bytes, and directory walks.
All 193 initial parts in each home retain their SHA-256 after both batches.

## Product node

The fresh generated node from the native-node receipt also accepts two equal-length field updates to the same key.
The prior debug daemon completes its update in 5.120620 seconds.
The candidate debug daemon completes the next update in 0.796835 seconds.
The prior call overlaps candidate compilation; these times are product observations, not a controlled speed ratio.
The candidate retains the earlier update to another record and the earlier delete.

After a normal close, the prior compatible debug daemon reopens that same generated home.
Its query returns `final-title-0` and `final-body-0` for record zero, and `cost-title-2` and `cost-body-2` for record one.
Deleted record seven remains absent. No source replay or primary data is required.
This establishes compatibility with that native-format binary. It does not establish reverse conversion to a legacy-only binary.

## Checks and remaining gates

Package and workspace library/binary Clippy, native daemon and release command builds, format, source-size, function-size, new-allow, diff, and non-test lints pass.
No test command runs. The operational calls use normal generated-data writes and normal closes.

The retained inventory still grows with prior parts and stays capped at 65,536 parts.
Compaction, live references, complete logical-path admission, memory-first acknowledgements, conversion, backups, rollback, and final fresh-node performance remain open.
The explicit native development gate stays in place. No primary upgrade occurs.
