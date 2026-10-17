# Native disk home API

CAUTION: This page records the October 15 implementation step.
Its limits and unfinished-work statements describe that step, not the current candidate.
Use the [current source status](lastdb-disk-rollout-status.md) for the present boundaries and receipt links.
Final candidate measurements remain pending.

This change binds the disk reader and journal to saved LastStore placement and key identities.
It does not select a native node backend, change the memory ack, or activate a primary home.
The artifact format remains separate. No existing home upgrades implicitly.

## Home descriptor

The exact marker `laststore-pack-home-v1` contains 4,096 bytes.
Every committed CURRENT requires feature `0x4`, in addition to features `0x1` and `0x2`.
CURRENT bytes 160..192 contain the SHA-256 of the entire descriptor, including its stored checksum.
A valid but different descriptor therefore cannot silently change the destination of a key.
An older reader refuses the unknown feature before any value read.

All integers use little-endian bytes. Reserved bytes are zero.

| Offset | Bytes | Meaning |
|---|---:|---|
| 0 | 8 | Magic `LDHOME1\0` |
| 8 | 2 | Version 1 |
| 10 | 2 | Descriptor size 4,096 |
| 12 | 4 | Zero flags |
| 16 | 2 | Placement codec `0x0101` |
| 18 | 2 | Stored key codec `0x0101` |
| 20 | 1 | High-bit shard count, zero |
| 21 | 1 | Hash-group bits, 10 |
| 22 | 1 | Hash algorithm 1, FNV-1a-64 |
| 23 | 1 | Placement key 2, first-NUL-inclusive hash prefix |
| 24 | 2 | Hash fanout, a power of two from 1 through 16 |
| 26 | 2 | Zero |
| 28 | 4 | Retained physical layout epoch |
| 32 | 16 | Nonnil home identity |
| 48 | 80 | Zero; physical frame packaging 0 is plain |
| 128 | 32 | SHA-256 with these checksum bytes zero |
| 160 | 3,936 | Zero |

Placement codec `0x0101` uses the existing LastStore formula.
The base is the low ten FNV-1a-64 bits of the prefix through the first NUL.
The offset is the low fanout bits of the remaining bytes. The group is `(base + offset) & 1023`.
Without a NUL, the whole key forms the prefix, and the remainder is empty.
A hash range uses the saved fanout groups from the same base. It never discovers groups from files.

Stored key codec `0x0101` preserves the complete LastStore UTF-8 document id and unsigned byte order.
Higher layers retain their existing binary-id escapes, molecule spelling, blind keys, range encoding, and scopes.
Stored values remain opaque. The builder does not create an atom or change its logical history.
`HomeDescriptor::from_layout` refuses unsupported layouts and physical frame encryption before destination creation.
Owner encryption inside a stored value remains unchanged.

## Read and write boundaries

`DiskHome` holds one descriptor. Each `HomeRead` captures one CURRENT and checks the exact descriptor digest.
The call charges both 4,096-byte objects against its aggregate byte limit.
Exact reads derive their own group. A batch preserves input slots and individual errors.
The batch shares all call-local metadata and block buffers. No error resets its budget.
A range derives its groups and borrows the same call through every page.
The existing limits remain: 4,096 metadata pages, 16,384 blocks, 256 MiB, and 10,000 examined keys.
These APIs keep no logical records or storage objects after the call ends.
The node's 10,000-record warm set remains a separate integration requirement.

`HomeJournal` derives every mutation group from the held descriptor.
A complete append reaches the recovery journal. It does not release logical write protection.
Publication updates the complete directory before a caller can release that protection.
Reopen recovers complete pending batches and starts a fresh checkpoint stream before admission.
All generation files remain in place. This change grants no deletion authority.

## Explicit command

`lastdb-disk-home` requires an explicit `--home` path. It has no primary default.
`build` streams generated or converted records in directory-key order, then full-key order within each group.
The initial physical version is `(0,0)`. An existing destination is refused.
`read` accepts collected collection/key coordinates. `range` accepts one complete encoded hash and range bounds.
`append` writes one complete recovery batch. `write` also publishes it. `publish` recovers and checkpoints pending work.
The command returns costs and disk receipts. Its JSON hex output adds command-level space beyond native body buffers.
Artifact-only commands refuse native homes. The legacy LastStore opener also refuses an incomplete native home before any write.

Native node reads, memory admission, conversion handoff, backup, rollback, and fresh-node proof remain unfinished.
