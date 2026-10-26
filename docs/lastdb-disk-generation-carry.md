# Verified generation records within one writer

The prior writer opens old immutable parts several times for each publication and journal-stream checkpoint.
The generation receipt already names those parts, their lengths, creation generations, and exact CURRENT bytes.
This change uses that durable receipt within one exclusive writer lease.

## Verification boundary

Each new exclusive lock starts without a verified root.
The first generation check opens every recorded part and verifies its length and first header or page.
It also verifies every receipt page, the embedded CURRENT, and strict dependency order.
This remains metadata verification. It is not a whole-part body checksum or a backup integrity proof.

The lock then retains only a 32-byte digest of the complete CURRENT encoding.
The lock retains no dependency list, page, hash group, payload block, or data file handle.
A different CURRENT digest requires complete part-metadata verification again.
A reopened writer also starts without the digest and repeats that verification.

While the same exclusive writer holds a verified CURRENT, subsequent checks read the checksummed generation receipt.
They validate its CURRENT, page chain, count, order, coordinate kinds, lengths, and creation boundaries.
Publication carries the unchanged encoded records for old immutable parts.
New parts still require exact file metadata and header checks before their receipt becomes durable.
The new root gains verified status only after its file and parent-directory barriers succeed.
A missing previously verified receipt is an error. It does not trigger an empty-store or dependency-walk fallback.

Every carried metadata buffer belongs to the current publication call and leaves memory afterward.
The existing 65,536-part limit bounds its arrays. No on-disk format, codec, mandatory feature, journal marker, or record version changes.
Existing compatible readers can read the result.

## Limits

The exclusive journal lock must exclude every writer that changes these immutable objects.
Direct external file alteration is unsupported. Addressed reads still verify their page, block, and record checksums.
This change does not detect damage to every unused body during each new write.
It grants no file deletion, compaction, conversion, backup, or cloud authority.
Those paths must preserve the same immutable-part and reference rules before activation.

The inventory remains a bounded conservative superset. Receipt work still grows with the number of retained parts.
Live dependency closure, compaction, and reclamation remain required before production activation.

The [operational receipt](https://github.com/EdgeVector/lastdb/blob/0b0a79d4c10949e2f0570e82d9bc4fdb9513a285/docs/proofs/disk-generation-carry-20261015/report.md) records generated-home file work, exact reads, ranges, and compatible-node reopen.
