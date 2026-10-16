# Immutable tree publication operational receipt

Date: October 15, 2026.
The commands used separate artifacts and generated data.
The primary, archive, factory, and reset stores were not used.
No ephemeral node ran in this unit.

Source: `e09ba23035e7b0016a4e40a5ab9dc940f0de2d25`.
The release programs used macOS and Rust 1.96.1.
The host had 14 logical CPUs.
Observed load averages were near three.
Filesystem-page state is unknown.

| Program | SHA-256 |
|---|---|
| `lastdb-pack` | `4f5374ee70161cbd51a5caaf86423aacc99b1a37874b469e2efe1aa7ecdac719` |
| `lastdb-journal` | `56bd7789640b0a8d178379003b931c064a8d752533432a8b92dc1aa525710423` |

The builder created each immutable artifact at a fresh temporary path.
The append command then created a complete journal and exited.
A separate publication command reopened and verified the complete captured suffix.
It copied the changed tree paths, completed the file barriers, and replaced CURRENT.
Separate read commands then checked the requested exact coordinates.

| Input | Initial keys | Journal operations | Complete batches | Read coordinates | Dependency parts | Publication time, ms |
|---|---:|---:|---:|---:|---:|---:|
| Multiple coordinates | 64 | 190 | 1 | 260 | 116 | 3314.104416 |
| Large key set | 20,000 | 6,000 | 2 | 23,501 | 44 | 5056.149250 |
| Long keys | 60 | 140 | 1 | 181 | 8 | 2693.737375 |

All 23,942 requested coordinates matched the expected values or absence.
The inputs included updates, deletes, repeated coordinates, new keys, and absent keys.
The first artifact added 110 explicit collection-and-group coordinates.
Its directory root grew from one level to two levels.
The larger artifact retained 20,000 initial keys before 6,000 mutations.
The long-key artifact used keys of exactly 65,535 bytes.
Each long key had a common 65,529-byte prefix and a six-digit suffix.
The publisher retained the last physical mutation version for each repeated coordinate.

Before publication, an ordinary read returned only the original committed values.
After publication, ordinary reads returned the new values directly from journal extents.
Every ordinary read reported zero group opens, footer bytes, directory walks, legacy replay bytes, and journal replay bytes.
Explicit publication correctly reported journal replay bytes.
It verified complete batches before any directory root change.

The 10,000-coordinate reads took 133.064667 ms and 115.527208 ms.
The final 3,501-coordinate read took 73.144958 ms.
Each batch used one captured CURRENT and one shared call budget.
These command limits do not establish the LastDB logical-record warm limit.
The LastDB read routes do not use this format yet.

Separate single-coordinate commands read the same generated value after publication:

| Artifact | Directory pages | Locator pages | Map pages | Payload blocks | File bytes | File opens | Time, ms |
|---|---:|---:|---:|---:|---:|---:|---:|
| Multiple coordinates | 2 | 2 | 1 | 1 | 87,960 | 8 | 2.214625 |
| Large key set | 1 | 3 | 1 | 1 | 87,982 | 8 | 2.218792 |

These are single observations, not latency percentiles or a LastDB node speed claim.
The structural work follows the held tree paths and the exact payload extent.
The commands did not reconstruct a hash group or replay a journal for an ordinary read.

The owner captured whole-part digests for all four original parts in each artifact.
Every original part retained its exact digest after publication.
The old generation receipt retained the complete original CURRENT bytes.
The new generation receipt retained the exact new CURRENT bytes.
Every new publication used generation two and retained generation one.
No published metadata part received an append or rewrite.

A repeated publication verified the current receipt and its named dependencies.
It retained the same generation, frontier, receipt UUID, and dependency count.
It wrote zero tree pages and performed zero journal replay.
A separate recovery command returned zero batches after the checkpoint.
The [verification receipt](verification.json) records these comparisons.

The largest instrumented ordinary-read byte-buffer peak was 26,798,828 bytes in the long-key batch.
The largest instrumented publication peak was 4,718,592 bytes.
These counters exclude returned mutations, parsed metadata, input JSON, and allocator overhead.
They do not measure total process memory size.
All page and block caches remain within their calls.

Package and full workspace clippy, release builds, format, source-size checks, source lints, and the branch secret check passed.
No tests, fixture harness, crash injection, or damaged-file probe formed part of this receipt.
The generation receipt marks whole-part checksums absent; it does not claim a verified physical backup.

The artifact command uses finite closure-walk and dependency-part limits.
Its closure walk needs an incremental inventory before the online 500 ms flush can use it at scale.
Online admission, token coverage, stream resume, the home bridge, ranges, conversion, backup, restore, and primary preparation remain open.
The [publication document](../../lastdb-disk-pack-publication.md) states these limits and the exact write order.

The generated temporary data can be removed after the receipts and PR evidence are preserved.
The source worktree and release programs remain available for review.
