# Incremental file inventory operational receipt

Date: October 15, 2026.
The commands used newly generated, separate artifacts.
The primary, archive, factory, and reset stores were not used.
No ephemeral node ran in this unit.

Source: `2c263f890ede4a5b69ce302442f1fe92ba967d48`.
The release programs used macOS and Rust 1.96.1.
The host had 14 logical CPUs.
An observed load average after the commands was 14.81, 7.69, and 5.91.
Filesystem-page state is unknown.

| Program | SHA-256 |
|---|---|
| Candidate `lastdb-pack` | `fb2fec3f008882b319d8ee0dd1942e8b456cfea1b12ad0091bb5765c4eef135a` |
| Candidate `lastdb-journal` | `ddab748a9607879d19461087235af5290d6ee97fd1ddc1a0f8a79d996293a959` |
| Previous `lastdb-journal` | `56bd7789640b0a8d178379003b931c064a8d752533432a8b92dc1aa525710423` |

The previous publisher program came from source `e09ba23035e7b0016a4e40a5ab9dc940f0de2d25`.
Both programs used fresh artifacts built by the candidate builder from identical input bytes.
No artifact came from a copy of another database.
The builder wrote the initial generation receipt before CURRENT.
The owner checked that its named parts exactly matched the created parts.

Each paired artifact received the same one-key journal update.
The previous publisher discovered the proposed dependency closure through a complete metadata walk.
The candidate retained the verified previous file inventory and added exact new part identities.
The candidate performed no full key walk for that publication.

| Initial keys | Publisher | Format-probe bytes | Read operations | File opens | New tree pages | Retained dependency parts | Time, ms |
|---|---|---:|---:|---:|---:|---:|---:|
| 64 | Previous | 365,156 | 35 | 43 | 3 | 7 | 119.753416 |
| 64 | Candidate | 283,236 | 30 | 39 | 3 | 8 | 111.401875 |
| 20,000 | Previous | 12,784,228 | 793 | 801 | 4 | 7 | 168.666125 |
| 20,000 | Candidate | 299,620 | 31 | 40 | 4 | 8 | 116.750208 |

All four commands verified the same 408 journal-frame bytes.
All four published the expected value at frontier one.
The candidate's extra dependency is a conservatively retained prior metadata part.
It does not change the readable key set.
These times are single observations on a busy host, not latency percentiles or LastDB node speed claims.

The owner then read every initial key and one absent key from each paired artifact.
All returned values and absences matched their source inputs.
The original parts retained their complete SHA-256 digests.
The old receipt retained the exact original CURRENT bytes.

Two other inputs checked these normal format dimensions:

- A complete batch deleted all 64 initial keys. The manifest still named the complete new journal data and map.
- An initially empty group received 110 new group coordinates. The new receipt named all 225 created parts across two receipt pages.

The empty group produced a complete two-part initial metadata inventory with no data parts.
Every newly created tree part appeared in the candidate receipt.
This included private intermediate tree parts and new groups' initial empty roots.
Every candidate receipt also retained all initial part identities.
The previous publisher's live-closure receipt did not retain the prior directory part in its new receipt.
Its older generation still retained that part.

A separate repeated command verified the two-page receipt and all 225 named parts.
It retained generation two and wrote zero tree pages.
The [verification receipt](verification.json) records the exact part-set and value comparisons.
The read commands checked 40,309 coordinates across all six artifacts.

The inventory contains fixed-width part identities and remains within the existing 65,536-part artifact limit.
It exists only during construction or publication.
It does not become a global key directory or a warm-set storage object.
The new receipt conservatively retains unused prior and intermediate parts.
A future compaction must rebuild the live dependency closure before retained-part growth reaches the explicit limit.

Package and full workspace clippy, release builds, format, changed-file/function size checks, source lints, and the branch secret check passed.
No tests, fixture harness, crash injection, or damaged-file probe formed part of these commands.
The primary program and the logical-record warm set remain unchanged.
Online admission, journal resume, token coverage, the home bridge, conversion, backup, restore, and primary preparation remain open.

The generated temporary data can be removed after the receipts and PR evidence are preserved.
The source worktree and release programs remain available for review.
