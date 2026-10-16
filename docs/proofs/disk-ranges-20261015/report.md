# Bounded hash range operational receipt

Date: October 15, 2026.
The commands used separate artifacts with generated data.
The primary, archive, factory, and reset stores were not used.
No ephemeral LastDB node ran in this unit.

Source: `438a75a6d827a514d72084ed16869def85b8fd13`.
The release programs used macOS and Rust 1.96.1.
The host had 14 logical CPUs and observed load averages near four.
Filesystem-page state is unknown.

| Program | SHA-256 |
|---|---|
| `lastdb-pack` | `04eeef8fc3606f8e966a7f110547b91ec3ff13634f0ab7daa9827bbc4c29740e` |
| `lastdb-journal` | `40acb43a3fe7f1abca89b1886fcc53865bba5d0ea6a708c381569bf2e9fcbf1c` |
| `lastdb-range` | `c0e376d499eb1ffa451d1b6f6987c95b004c6501476db3e8032f9a7213d884c8` |

The first two artifacts used one generated molecule and two distinct encoded hashes.
Both hashes used the same FNV-1a low-ten-bit placement value, 904.
A bounded search over at most 100,000 generated names selected the second hash.
The hashes therefore occupied the same sixteen configured groups.
Their range keys used the existing byte order.
The artifact codec identifiers remain explicit placeholders, not production allocations.

The requested hash held 400 keys, including 100 tombstones.
Ten later writes replaced earlier values under the same coordinates.
The second hash held 16 keys in the small artifact and 20,000 keys in the large artifact.
The complete journal publisher created the immutable committed roots before the range commands.
The requested hash's values and mutation versions remained identical between these two artifacts.

| Request | Returned | Examined | Pages returned | Directory pages | Locator pages | Payload blocks | File bytes | Time, ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Small, complete hash | 300 | 400 | 9 | 1 | 16 | 37 | 343,967 | 9.569916 |
| Large, complete hash | 300 | 400 | 9 | 1 | 34 | 37 | 638,879 | 13.362167 |
| Large, range 50 through 149 | 75 | 116 | 6 | 1 | 32 | 10 | 573,360 | 10.795167 |
| Large, prefix covering 50 through 59 | 8 | 26 | 3 | 1 | 32 | 2 | 563,696 | 10.217875 |
| Large, first seven live records | 7 | 25 | 1 | 1 | 34 | 3 | 597,875 | 10.499334 |
| Large, equal bounds | 0 | 0 | 1 | 0 | 0 | 0 | 4,096 | 0.128000 |
| Deep, complete hash | 3,750 | 5,000 | 30 | 1 | 98 | 176 | 1,831,494 | 19.983083 |

These times are single command observations, not latency percentiles or LastDB node latency claims.
The bounded requests count pending keys and keys that establish the upper boundary.
The first-seven request correctly reported `range_exhausted: false`.
Every short successful page reported an exhausted range.
Every returned key stayed under the requested hash.
All returned bodies and physical mutation versions matched their generated source bytes.

The deep artifact used one configured group and a locator tree with three levels.
It held 20,000 keys under the other hash and 5,000 keys under the requested hash.
Every fourth requested key was a tombstone.
Its range crossed a root child boundary through the ancestor stack.
The command returned all 3,750 live records and counted all 1,250 tombstones.
It did not enumerate the 20,000 unrelated keys.
The builder wrote this artifact directly, so its values used committed pack extents.
The first two artifacts supplied committed journal extents.

Each multi-page command used one captured CURRENT and one shared total call budget.
The cursor's lifetime retains its original call; a new call cannot reset its budget or generation.
A request with a four-key budget returned exit code one and no output page.
It reported `disk-pack v1: read resource limit exceeded`.
It did not report an exhausted range or a successful short page.
No file damage or crash injection formed part of this command.

Every successful range receipt reported zero group opens, footer bytes, directory walks, legacy replay bytes, and journal replay bytes.
The largest instrumented temporary byte-buffer peak was 2,357,291 bytes in the deep range.
This counter excludes parsed metadata, returned records, input JSON, and allocator overhead.
It does not measure total process memory size.
The [verification receipt](verification.json) records the exact value, order, version, scope, and page comparisons.

Package and full workspace clippy, release builds, format, changed-file/function size checks, source lints, and the branch secret check passed.
No tests or fixture harness ran.
The commands used real immutable files and exact product read operations.

The LastDB query routes and logical-record warm set do not use this range API yet.
This receipt does not establish online admission, local-write version checks, or the 10,000-record warm limit for the new format.
Those steps, the home bridge, journal resume, conversion, backup, restore, and primary preparation remain open.
The [range document](../../lastdb-disk-pack-ranges.md) states the API limits and continuation rules.

The generated temporary data can be removed after the receipts and PR evidence are preserved.
The source worktree and release programs remain available for review.
