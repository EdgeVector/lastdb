# Native disk home API

Updated October 22, 2026 for the direct snapshot writer.
The [active snapshot protocol](lastdb-snapshot-commits.md) defines the commit and recovery boundary.
The [source review](lastdb-searchable-snapshot-code-review.md) separates current source from recorded product results.

The disk reader and snapshot writer use saved LastStore placement and key identities.
Native disk storage is the product default; this document does not activate or change a primary home.
The artifact format remains separate. The home identity and key codecs stay unchanged.

## Home descriptor

The exact marker `laststore-pack-home-v1` contains 4,096 bytes.
Every native CURRENT requires native-home feature `0x4` and snapshot feature `0x100` (`256`).
Complete object bounds, packed keys, and atom records retain their feature checks.
The reader refuses retired journal feature `0x2` before writer file creation.
Startup does not convert a retired journal home, and no old-native-journal converter exists.
An older supported snapshot without live-count feature `512` receives a bounded metadata count pass before writer admission.
Its first ordinary commit stores those counts and requires feature `512`.
Older programs refuse that feature.
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
The node's warm set uses a separate 256 MiB default byte budget, with no fixed record-count cap.

`HomeSnapshot` derives every mutation group from the held descriptor.
A complete commit writes immutable payload files and changed directory and locator pages.
One generation receipt and one atomic CURRENT publication select the complete stored batch.
All required barriers finish before durable success.
The node installs the committed view before it retires covered memory changes and completes the queue receipt.

Reopen verifies the selected snapshot and its generation receipt; it performs no local mutation replay.
Interrupted private output remains outside that snapshot.
Read holds and explicit references protect files until safe reclaim.
The [file lifetime protocol](lastdb-disk-file-lifetime.md) defines that removal authority.

## Explicit command

`lastdb-disk-home` requires an explicit `--home` path. It has no primary default.
`build` retains its generic builder interface, but the atom-record fence refuses its unsupported native result before file creation.
It does not create a current product home or convert an old journal home.
The product factory creates fresh homes with the required atom-record authority.
`read` accepts collected collection/key coordinates. `range` accepts one complete encoded hash and range bounds.
`plan` quotes a complete snapshot batch without a writer. `write` commits that batch directly to the snapshot.
The former `append` and `publish` commands are removed.
The command returns costs and disk receipts. Its JSON hex output adds command-level space beyond native body buffers.
Artifact-only commands refuse native homes. The legacy LastStore opener also refuses an incomplete native home before any write.

Local backup and restore use `lastdb-native-backup-2`, without journal state.
The generic `lastdb-disk-recovery convert` and `reverse` commands are retired.
An existing forward fence can complete without an open legacy store; reverse completion is retired.
Cloud restore refuses a SegmentLog source or destination descriptor before destination file changes.
Legacy offline maintenance also refuses SegmentLog homes before any file change.
Historical layout decoders and conversion fences remain for explicit refusal and source inspection.
Native cloud startup and activation remain blocked.
Atomic snapshot/log capture and an old-native-journal converter remain separate work.
