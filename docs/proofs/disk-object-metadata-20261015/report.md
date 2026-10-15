# Complete object bounds operational receipt

Date: October 15, 2026.
This receipt covers separate disk-pack artifacts and journal artifacts.
The primary, archive, factory, and reset stores were not used.

Source: `33d456b1dbe7165f5cd8c63e3f922c8ceda63a40`.
The programs used a release build on macOS with Rust 1.96.1.
The host had 14 logical CPUs.
The observed load average was 13.37, 6.89, and 6.40.
The filesystem-page state is unknown.

| Program | SHA-256 |
|---|---|
| `lastdb-pack` | `02c5f2366deb86b39a7525e373ad43d885b94e9a98acc5fbd96bba8393b5cbcf` |
| `lastdb-journal` | `cf80f839b89c9030c83bf48b6884a7dfb3d9d0f1b06b501ad8f16b8e6525fd31` |

The old program came from the immutable reader unit in PR #190.
Its SHA-256 was `f0e9a4a83816e55fd74437fa58ac5ca1749c574e02bea27c06899885f78a6f44`.
It built an artifact with mandatory features equal to zero.
The new program built an artifact with mandatory feature bit zero set.
The new reader returned the correct value from both artifacts.
The old reader refused the new artifact before a payload read.
The new journal writer refused the old artifact before it created TAIL.

The new map page header holds the complete object length and data-part count.
Every map page repeats these values under its checksum.
The reader checks an extent against the complete length before it allocates the record buffer.
The reader also checks block count, part bounds, final block length, and creation generation.
Private map headers receive their final values before TAIL or CURRENT can reference them.
No published part receives a rewrite.

The single-value reads used the same generated key and five-byte body.
Both reads opened six files, used six file-read operations, and read 53,556 file bytes.
Both used one directory page, one locator page, one map page, and one payload block.
The new header requires no extra file read.
The old artifact read took 0.532583 ms; the new artifact read took 0.695709 ms.
These are single observations, not speed percentiles.

The large artifact held ten generated bodies of 1,048,576 bytes across two data parts.
Its four map pages each recorded 10,486,830 logical bytes and two data parts.
A separate command read three selected values and verified their complete bodies.
It read 3,302,942 file bytes and took 18.552125 ms.
The command used three map pages and 771 payload blocks.
The instrumented temporary byte-buffer peak was 4,288,699 bytes.
This count excludes returned values and allocator overhead; it is not process memory size.

The journal command wrote 32 generated mutations as two complete batches.
A separate command recovered all values with the correct body digests.
Recovery ended with `complete: true`.
The [verification receipt](verification.json) records the value, compatibility, and header checks.

The ordinary read receipts reported zero group opens, footer bytes, directory walks, and legacy or journal replay bytes.
No input came from an existing user database.
No ephemeral node ran in this unit.
No tests, fixture harness, or file-damage probe formed part of these commands.

Package clippy, full workspace clippy, release builds, format, source-size checks, and the branch secret check passed.
The complete object bounds support later root publication and backup dependency lists.
This unit does not activate the LastDB home format or provide a backup implementation.
Online writes, root publication, ranges, conversion, restore, and primary preparation remain open.

The generated temporary data can be removed after the receipts and PR evidence are preserved.
The source worktree and release programs remain available for review.
