# LastDB disk directory and pack format v1

Draft, October 14, 2026. This document completes the format part of Stage 1.
It does not enable a reader, writer, converter, or primary upgrade.
All new numeric values below are proposals. Stage 2 must freeze them before it writes this format.

The [first Stage 2 implementation](lastdb-disk-pack-reader.md) freezes the local page,
key, record, and plain-frame bytes for separate immutable artifacts.
The [native home API](lastdb-disk-native-home.md) allocates a separate home fence and codec identities.
It does not select the node backend or change the memory ack boundary.
The journal, bridge, encrypted-frame, conversion, and retention rules below remain release prerequisites.

Source: [Draft LastDB pack files and bounded disk lookup](https://chatgpt.com/space/page_dac3cffa86f08191b9fe60c47d7477a9).
The canonical model and access requirement remain authoritative:
`lastdb-canonical-model.md`, `lastdb-agent-access-model.md`, and
`lastdb-access-complexity-requirements.md`.

## 1. Required result

A known storage key selects one of the existing 1,024 hash groups.
A cold read uses a committed root, directory pages, locator pages, and exact payload blocks.
An ordinary converted-group read performs no directory walk, group reconstruction, full-footer read, or journal replay.
The ordered tree serves exact keys, batches, and ranges under one named molecule and hash.
The API access requirement does not change. Physical page work follows the tree height.

The format keeps the following rules:

- The warm set contains at most 10,000 logical records.
- A call protects the records that it holds. A write stays protected until its flush completes.
- A memory write precedes its ack. The normal flush interval remains 500 ms.
- A read after an ack sees the accepted write, through the current write state when necessary.
- Local reads and writes do not wait for cloud sync.
- Molecules and atoms retain their node-wide identities.
- Atom content retains owner-key encryption. A receiver still receives re-encrypted atom content.
- The atom points at the same file. A new physical position does not create a new atom or change its history.
- Shared file references, tombstones, grace periods, and reclamation audits retain their current meaning.

The directory is the authoritative disk location map. It is not a second retained memory cache.
Only a call owns decoded pages, payload blocks, and file handles. The call releases them when it ends.
The writer holds only reserved current work. It does not retain old hash groups after a fetch.

## 2. Limits and terms

`F` is the readable frontier in CURRENT. `J` is the durable journal frontier in JOURNAL_TAIL.
Both name complete mutation batches. A batch sequence is a monotonically increasing `u64`.
A mutation version is `(batch_sequence, operation_number)`. An operation number is a `u32`.
This version orders physical store changes. It does not replace an atom UUID, author clock, write time, or device identity.

| Item | Proposed v1 value or rule |
|---|---|
| Directory and locator page | Exactly 16,384 bytes |
| Logical payload block | At most 4,096 bytes before block compression |
| Physical part | At most 8,388,608 bytes, including its 128-byte header |
| Independent read workers | Four; one shared call budget |
| Tree path | At most eight pages, including the leaf |
| Internal page | At most 88 separators and 89 children |
| Leaf page | At most 53 entries |
| Non-root internal occupancy | At least eight children after a completed publication |
| Encoded storage key | At most 65,535 bytes; equal to the legacy segment limit |
| Inline key prefix | At most 128 bytes |
| Key overflow | At most five 16 KiB pages for one key |
| Body bytes | At most `u32::MAX`, equal to the legacy segment format |
| One record | `96 + key_bytes + body_bytes`, with checked `u64` arithmetic |
| One record location | One contiguous logical extent; no inter-record delta chain |
| Complete batch encoding | At most 128 MiB and 4,096 operations; native feature `0x20` permits 16,384 operations |
| Writer batch admission | Proposed 128 MiB; a separate runtime limit below the format ceiling |
| Recovery suffix reserve | Proposed 512 MiB, including admitted work not yet appended |
| Recovery suffix pressure | Materialize at 384 MiB or two seconds; refuse new writes at 512 MiB or five seconds |
| Converter worker | Proposed 8 MiB sort buffer, 64 KiB stream buffer, and eight merge inputs |
| Converter concurrency | Proposed four workers with one aggregate memory and disk reserve |

The batch, suffix, age, call, and converter limits require a release decision and status fields.
An unset required limit prevents the new writer and converter from activation. It does not select an unlimited fallback.
The 128 MiB batch limit is not a write-queue cap.
This document does not activate the separate 64 MiB write-queue or 32 GiB disk-cap proposals.

Existing large records remain representable in packs up to the legacy body ceiling.
A runtime call budget can require a stream operation or return a resource-limit result for those bytes.
An incoming batch above its admission limit receives no success ack.
The converter reports a key, record, or tree limit by collection, group, source position, and byte counts.
It does not expose key content and does not convert the affected boundary.

## 3. Common byte rules

Offsets below count bytes from the start of the stated object. Ranges exclude their final offset.
All integer fields use unsigned little-endian bytes. UUID fields use their 16 raw bytes.
No structure uses machine padding, variable-length integers, native `usize`, or native byte order.
All sums, products, allocations, offsets, and conversions require checked arithmetic before a read or allocation.

Storage keys retain their existing UTF-8 bytes, separators, storage prefixes, and key codecs.
The tree compares the full encoded key as unsigned bytes. It performs no normalization.
A collection name also retains its current UTF-8 bytes. Its proposed format ceiling is 65,535 bytes.
An older valid name above that ceiling blocks conversion and remains readable through the bridge reader.

A SHA-256 field checks the complete declared object with that field replaced by zero bytes.
Unused bytes and reserved fields must be zero. Unknown flags, kinds, codecs, or mandatory features cause a format error.
An unkeyed SHA-256 detects damage; it does not prove authenticity.
Encrypted blocks require the authentication check below. Atom encryption remains an additional, independent check.

Every file identity creates an exact path under the home. No value can supply a path, `..`, separator, or absolute name.
The proposed data path is `packs/<uuid>/part-<u32>`.
Journal data uses `journal/<stream-uuid>/batch-<u64>/part-<u32>`.
Paged objects use `directory/<uuid>/part-<u32>`, `locator/<uuid>/part-<u32>`, and each data object's `map/part-<u32>`.
Numbers use fixed-width lowercase hexadecimal. UUIDs use the existing canonical lowercase UUID representation.
The home creates and syncs these named parent directories before it publishes a reference.

## 4. CURRENT and JOURNAL_TAIL

CURRENT is exactly 4,096 bytes. A call captures one parsed CURRENT and holds its generation until the call ends.
The durable file and the in-process readable root identify the same published generation.

| Offset | Bytes | CURRENT field |
|---|---:|---|
| 0 | 8 | Magic `LDCUR1\0\0` |
| 8 | 2 | Format version, 1 |
| 10 | 2 | Object length, 4,096 |
| 12 | 4 | Flags, zero in v1 |
| 16 | 8 | Generation number |
| 24 | 8 | Previous generation number; zero for the first generation |
| 32 | 8 | Readable frontier `F` |
| 40 | 8 | Journal frontier known at publication; must be at least `F` |
| 48 | 16 | Root-directory object UUID |
| 64 | 8 | Root-directory page number |
| 72 | 2 | Directory path height, 1 through 8 |
| 74 | 2 | Hash-group count, 1,024 |
| 76 | 2 | Existing placement codec identifier |
| 78 | 2 | Existing storage-key codec identifier |
| 80 | 16 | Journal stream UUID for the checkpoint boundary |
| 96 | 8 | Checkpoint batch sequence; normally equal to `F` |
| 104 | 8 | Conversion descriptor page, or zero |
| 112 | 8 | Required feature bits: complete object bounds `0x1`, checkpointed journal lifecycle `0x2`, native home descriptor `0x4`, file lifetime protocol `0x8`, schema-record reverse edges `0x10`, larger native batches `0x20` |
| 120 | 8 | Publication time, Unix milliseconds |
| 128 | 32 | SHA-256 of all 4,096 bytes |
| 160 | 32 | SHA-256 of the complete native home descriptor with feature `0x4`; otherwise zero |
| 192 | 3,904 | Zero bytes |

The placement and key identifiers must equal the home descriptor. Conversion does not change either codec.
One directory root contains every collection and group root needed through `F`.
A call never combines a tip root from one CURRENT with an atom root from another CURRENT.
Immutable child pages can predate the current generation. Their creation generation must not exceed the held generation.
With feature `0x2`, `previous_generation = 0` declares that this checkpoint stream admits no journal batch yet.
A complete stream checkpoint sets a nonzero previous generation before the first append.
Feature `0x2` requires `0x1`. New artifacts use both bits; older writers refuse them before journal admission.

Native bit `0x10` requires native bit `0x4`.
Fresh empty LastDB homes publish `0x10` before the version-2 molecule-reference completeness record.
Older programs refuse this bit before a schema write.
The bit alone does not prove completeness.
A memory ack also requires the decoded version-2 proof and each exact authoritative schema path.
Generic conversion never grants `0x10`; imported proof bytes can be stale after a legacy writer.
Converted homes use the durable fallback until a separate complete-source bootstrap and writer fence.
Local backups preserve the original complete root and its feature bits.
A missing, old, or unreadable proof keeps the durable fallback.

Native bit `0x20` also requires native bit `0x4`.
It permits one complete ordered batch of at most 16,384 storage operations.
New native homes, including new conversion destinations, publish this bit in their initial synced `CURRENT` before journal admission.
Older readers refuse that root before any larger batch can reach its journal.
Existing homes without this bit retain the 4,096-operation limit; this release does not upgrade them implicitly.
Standalone artifacts cannot use the bit and retain the same 4,096-operation limit.

The held root selects the count limit for admission, recovery, locator reads, and map publication.
Context-free locator encode/decode and descriptor quotes retain the original limit.
Operation indexes and marker counts keep their existing `u32` fields.
The 128 MiB batch cap, suffix limits, part sizes, and dependency bounds remain unchanged.
The extension neither splits a logical batch nor removes ordered operations.
The `lastdb-disk-home` JSON command retains its separate 4,096-operation input limit.
The node can produce more stored operations from one logical request through the native capability.
The [7110c7f5 node receipts](proofs/layer2-completion-20261016/node-final-off-receipts.jsonl) record a complete 256-record durable update and normal reopen.

JOURNAL_TAIL is also exactly 4,096 bytes. Its proposed magic is `LDJTL1\0\0`.
Offsets 8 through 15 use the same version, length, and flags fields as CURRENT.
Offsets 16 through 31 contain the journal stream UUID.
Offsets 32, 40, 48, and 56 contain `first_batch`, `last_complete_batch`, durable encoded bytes, and durable publication time.
Offsets 64 through 95 contain the SHA-256 of the last complete batch marker.
The encoded-byte count is the sum of complete physical data-part and map bytes in the retained interval.
It excludes TAIL itself. Admission reserves uncompressed worst-case bytes, even when the physical files compress well.
Offsets 96 through 127 contain the SHA-256 of the complete TAIL object.
Offsets 128 through 143 contain the previous stream UUID, or zero for the first stream.
Offsets 144 through 151 contain the previous stream's complete checkpoint frontier, or zero.
Offsets 152 through 183 contain the previous stream's final durable TAIL digest, or zero. The rest is zero.
The tail names only complete durable batches. It never names a partially written last batch.

The [artifact journal implementation](lastdb-disk-journal.md) supplies complete batches and a bounded recovery reader.
The [resume implementation](lastdb-disk-journal-resume.md) adds stream changes under the same exclusive artifact lock.
Neither implementation activates the online home writer or conversion operations.

TAIL publication requires all named batch parts, maps, and markers to be durable first.
Its update uses a temporary file, a file barrier, atomic rename, and a parent-directory barrier.
`J` advances only after the final directory barrier succeeds.
TAIL is not a record-location cache. Ordinary reads use CURRENT and its locator extents.

A stream change requires CURRENT to reach the old stream's complete `J` first.
The writer then publishes an empty new TAIL with `last_complete_batch = F` and `first_batch = F + 1`.
The previous stream fields verify the exact change boundary.
The exact old TAIL is durable at `journal/<old-stream-uuid>/tail-<checkpoint-as-16-hex-digits>` before the new TAIL.
The predecessor digest covers all 4,096 old published bytes, including their stored checksum.
It next publishes CURRENT with the new checkpoint stream, then permits new admissions.
A crash between these publications is valid only when CURRENT's `F` equals that named change boundary.
Recovery uses the new TAIL from that boundary. A mismatch below the boundary refuses recovery.
A new writer starts a fresh stream before admission, including its initial open and every reopen.
An initial lifecycle checkpoint can retry initialization without reusing a frame identity.
An admitted checkpoint with a missing TAIL refuses recovery.

## 5. Directory, locator, overflow, and block-map pages

Every page is exactly 16,384 bytes. A paged object stores at most 512 pages in each 8 MiB part.
For page number `p`, the part is `floor(p / 512)` and its byte position is `(p mod 512) * 16,384`.
Paged parts have no part header or footer; every page carries its complete identity and integrity header.
Their lengths are exact page multiples, at most 8 MiB. Data parts use the separate header in Section 6.
A reference must identify a complete page within the selected part's durable length.
Each publication allocates fresh part numbers. It never appends to a published metadata part.
An object UUID names one tree across generations; page numbers never repeat under that UUID.
Thus a new root can reference older immutable pages through the same exact object path.
Part numbers fit `u32`; page numbers are at most `512 * (u32::MAX + 1) - 1`.
Exhaustion requires a bounded tree rewrite under a fresh UUID before the next admission.

| Offset | Bytes | Common page header |
|---|---:|---|
| 0 | 8 | Magic `LDPAGE1\0` |
| 8 | 2 | Format version, 1 |
| 10 | 1 | Kind: internal 1, locator leaf 2, directory leaf 3, key overflow 4, block map 5; metadata kinds in Appendix A |
| 11 | 1 | Flags: bit zero only for a block map with complete object bounds; otherwise zero |
| 12 | 2 | Header length, 128 |
| 14 | 1 | Tree level; zero for a leaf or non-tree page |
| 15 | 1 | Zero |
| 16 | 8 | Creation generation |
| 24 | 16 | Object UUID |
| 40 | 8 | Page number |
| 48 | 4 | Used payload bytes, at most 16,256 |
| 52 | 2 | Entry count |
| 54 | 2 | Entry width; zero for key overflow |
| 56 | 8 | Leftmost child for an internal page; otherwise zero |
| 64 | 8 | Next page: key overflow 4, source descriptor 9, or generation receipt 11; otherwise zero |
| 72 | 8 | Auxiliary total: overflow suffix bytes for kind 4, first block ordinal for kind 5, chain page count for kinds 9 and 11; otherwise zero |
| 80 | 32 | SHA-256 of the complete page |
| 112 | 16 | A flagged block map uses logical object bytes `u64`, then complete data-part count `u64`; otherwise zero |
| 128 | 16,256 | Payload and zero slack |

The next-page field is a `u64` page number in the same object UUID.
Its value is zero on the final chain page. Other page kinds require zero at offsets 64 through 71.
An overflow chain contains at most five pages. Source and receipt chains contain at most `u32::MAX` pages.
For kinds 9 and 11, offset 72 contains that exact complete chain count on every page.
It must equal the count in the descriptor prefix. A zero count is invalid.
The reader checks the declared count before work, then verifies that the final page ends the chain at that exact count.
Every link preserves page kind and object UUID and names a complete durable page.
Cycles, duplicate links, a premature zero link, or a nonzero final link cause an integrity error.

An internal entry is a 176-byte key descriptor followed by an eight-byte child page number.
The leftmost child lies below the first separator. Each separator is the full minimum key of its right child.
Equality selects the right child. Children use the same object UUID and have level one less than their parent.
Cycles, repeated ancestors, descending keys, overlapping child intervals, and out-of-file children cause an integrity error.
The publisher validates full entry order and child intervals before it checksums a page.
An ordinary reader checks the selected path and its adjacent full-key boundaries within its declared comparison bound.
The writer uses copy-on-write pages. It never changes a page reachable from a published generation.

A leaf entry is a 176-byte key descriptor followed by a 128-byte value.
Entries use strict full-key order. One leaf contains no duplicate keys.
Leaves use no sibling pointer in v1. A range cursor uses its bounded ancestor path to find the next leaf.
This avoids a stale sibling link after a copy-on-write split.

### Full-key descriptor and overflow

| Offset | Bytes | Key descriptor |
|---|---:|---|
| 0 | 4 | Full key byte count, at most 65,535 |
| 4 | 2 | Inline prefix count, `min(full_length, 128)` |
| 6 | 2 | Flags: bit zero means overflow; other bits are zero |
| 8 | 128 | Inline prefix, then zero bytes |
| 136 | 8 | First overflow page, or zero |
| 144 | 32 | SHA-256 of the full encoded key |

An overflow page contains the next exact suffix bytes in its payload.
The header's auxiliary length names the total suffix bytes in the chain.
The sum of page payload lengths must equal `full_length - inline_length`.
Only the final page has a zero next reference. At most five pages may be followed.
The key descriptor can share immutable overflow pages when every complete byte matches.
Within a tree, page zero is reserved for its initial root. Overflow links and child links never use page zero.

An unequal inline prefix can settle a comparison.
An equal prefix requires the necessary full suffix comparison, its page checks, and a full-key digest check.
A digest or fingerprint alone never establishes a key match or absence.
The requested key must equal the full leaf key before the reader trusts its extent.

### Directory key and value

A directory key is `u16 collection_name_length | collection_name_bytes | u16 group_number`.
Its full-key descriptor uses the same byte comparison and overflow rules.
The extra four bytes reduce the admitted collection-name ceiling to 65,531 bytes for this key.
This explicit derived ceiling controls conversion; it never truncates a collection name.

| Offset | Bytes | Directory leaf value |
|---|---:|---|
| 0 | 1 | Mode: bridge legacy 1, converted 2 |
| 1 | 1 | Flags, zero |
| 2 | 2 | Group number, 0 through 1,023 |
| 4 | 4 | Zero |
| 8 | 16 | Converted locator object UUID, or zero |
| 24 | 8 | Converted locator root page |
| 32 | 2 | Converted locator height, or zero |
| 34 | 6 | Zero |
| 40 | 16 | Immutable legacy source descriptor UUID, or zero |
| 56 | 8 | Captured legacy frontier |
| 64 | 16 | Bridge overlay locator object UUID, or zero |
| 80 | 8 | Bridge overlay root page |
| 88 | 2 | Bridge overlay height, or zero |
| 90 | 6 | Zero |
| 96 | 8 | Frontier at the complete converted boundary |
| 104 | 24 | Zero |

A converted entry has no legacy source or bridge overlay.
An empty locator uses a valid empty root leaf, not a missing file.
Bridge reads check the current overlay first. A valid overlay tombstone hides the old source.
Only the bridge mode can use the legacy group loader. Its receipt identifies that work explicitly.

### Locator leaf value

| Offset | Bytes | Locator leaf value |
|---|---:|---|
| 0 | 1 | Source: local journal 1, local pack 2, cloud only 3, tombstone 4 |
| 1 | 1 | Flags, zero |
| 2 | 2 | Zero |
| 4 | 8 | Mutation batch sequence |
| 12 | 4 | Operation number |
| 16 | 8 | Complete logical record length |
| 24 | 16 | Pack UUID, or journal stream UUID |
| 40 | 8 | First logical block ordinal within the object or journal batch |
| 48 | 4 | Position within the first decoded block |
| 52 | 4 | Number of payload blocks |
| 56 | 32 | SHA-256 of the complete record bytes |
| 88 | 8 | Cloud descriptor page, or zero |
| 96 | 8 | Authenticated cloud extent length, or zero |
| 104 | 24 | Zero |

For a journal source, the batch sequence selects the exact batch path under the stream UUID.
For a pack source, the object UUID selects the exact pack path.
The block count equals `ceil((first_position + record_length) / 4,096)`.
The first position is below 4,096; the last block can be short only at the object's logical end.
The extent must remain inside the published object's logical length.
A record never depends on another record to decode its bytes.

A tombstone retains its mutation version. Its record length, object UUID, block fields, digest, and cloud fields are zero.
A valid tombstone returns absence at that version. It does not authorize atom-history or shared-file deletion.
A cloud-only entry has no local payload fields. It must contain a valid cloud descriptor and complete record length and digest.

### Block map

New artifacts require CURRENT feature `0x1`.
Every block-map page sets flag bit zero and repeats the complete object's logical byte count and data-part count.
These values occupy common-header bytes 112 through 127 and receive the page checksum.
The full block count is `ceil(logical_object_bytes / 4,096)`.
The complete map-page count is `ceil(block_count / 677)`; map-part count is `ceil(map_page_count / 512)`.
Data parts use exact consecutive numbers starting at zero.
These fields let a backup enumerate the complete object without a directory walk or footer.

The reader validates complete record extents, selected map counts, final-block lengths, data-part bounds, and creation generations.
The values must agree across all map pages used by a call.
The writer finalizes and checksums only its private, unpublished map pages before the file and directory barriers.
It never changes a published map part.

Readers retain feature-zero artifact support with the original zero header bytes.
New journal construction refuses those older artifacts instead of mixing map formats.
Readers from before feature `0x1` refuse the new CURRENT before any payload read.
This artifact feature does not activate the production home fence or a LastDB query route.

A block-map page has 24-byte entries, at most 677 per page, and zero slack.
The page for block ordinal `b` is `floor(b / 677)`; the entry is `b mod 677`.
The header auxiliary value equals `page_number * 677`.
Each entry contains six `u32` values: part number, part offset, stored frame length, decoded length, flags, and zero.
An entry's frame must remain within its part. Frames cannot overlap or cross a part boundary.
Its physical start is at least 128, after the data part header.
Decoded length is 1 through 4,096, except an explicitly empty object contains no entries.
The reader reads only the map pages for the requested blocks. It never reads a complete block map or pack footer.
One batch's journal map becomes immutable when its complete marker is published.

## 6. Physical parts, frames, and records

A part starts with a 128-byte header:

| Offset | Bytes | Part header |
|---|---:|---|
| 0 | 8 | Magic `LDPART1\0` |
| 8 | 2 | Format version, 1 |
| 10 | 1 | Kind: pack 1, journal batch 2 |
| 11 | 1 | Flags, zero |
| 12 | 4 | Header length, 128 |
| 16 | 16 | Object UUID, or journal stream UUID |
| 32 | 8 | Journal batch sequence; zero for a pack |
| 40 | 4 | Part number |
| 44 | 4 | Complete physical length, 128 through 8,388,608 |
| 48 | 8 | First logical block ordinal |
| 56 | 8 | Logical block count in the part |
| 64 | 8 | Encryption key epoch identifier; zero for plain frames |
| 72 | 8 | Creation generation |
| 80 | 32 | SHA-256 of the header with this field zero |
| 112 | 16 | Zero |

A part contains independently encoded frames after the header. The block map supplies each exact physical frame position.
A frame is `80-byte header | stored bytes | 16-byte authentication tag when encrypted`.
It has no trailing padding. A frame never crosses a part boundary.

| Offset | Bytes | Block frame header |
|---|---:|---|
| 0 | 8 | Magic `LDBLK1\0\0` |
| 8 | 2 | Format version, 1 |
| 10 | 1 | Compression: raw 0, one independent zstd frame 1 |
| 11 | 1 | Encryption: plain 0, approved block encryption 1 |
| 12 | 4 | Complete stored frame length |
| 16 | 8 | Logical block ordinal |
| 24 | 4 | Decoded length, at most 4,096 |
| 28 | 4 | Stored payload length, at most 4,096 |
| 32 | 16 | Object UUID; must match the part |
| 48 | 8 | Journal batch sequence; zero for a pack |
| 56 | 8 | Encryption key epoch identifier |
| 64 | 4 | Plain frame CRC32 over header and payload, with this field zero; zero when encrypted |
| 68 | 12 | Zero |

Compression is selected only when its complete output is smaller than the raw block.
The writer uses no external dictionary. The decoder permits one zstd frame, no trailing bytes, and at most 4,096 output bytes.
The decoder checks the declared length before allocation and requires the exact decoded length afterwards.
The maximum stored frame length is 4,176 bytes plain or 4,192 bytes encrypted.

The proposed encrypted block uses AES-256-GCM with a per-object key from the current LastStore owner data key.
The proposed HKDF-SHA256 context is `lastdb/disk-pack-v1/block`.
The salt is `object_uuid | batch_sequence_le64`; a pack uses sequence zero.
The nonce is four zero bytes followed by the eight-byte block ordinal.
The complete 80-byte header is authenticated associated data.
The receiver verifies the authentication tag before decompression or record parse.
An object UUID, batch sequence, and block ordinal tuple must never be reused under a key epoch.
An interrupted object is never appended with new plaintext at a used ordinal; a fresh identity replaces it.

This block encryption is additional physical protection. It does not open, reseal, or replace owner-key atom encryption.
Plain LastStore homes can use plain frames around already encrypted atom content.
The exact cryptographic context and key-epoch interface require security review before Stage 2 activation.
The format refuses an encrypted extent when its key epoch is unavailable; it never reports that key as absent.

A complete record is `96-byte header | full storage key | unchanged stored body`.
The record can cross any number of logical blocks within its declared bound.

| Offset | Bytes | Record header |
|---|---:|---|
| 0 | 8 | Magic `LDREC1\0\0` |
| 8 | 2 | Format version, 1 |
| 10 | 1 | Operation: put 1; tombstones use locator state and journal operations |
| 11 | 1 | Flags, zero |
| 12 | 4 | Key length, at most 65,535 |
| 16 | 8 | Body length, at most `u32::MAX` |
| 24 | 8 | Mutation batch sequence |
| 32 | 4 | Operation number |
| 36 | 4 | Header length, 96 |
| 40 | 32 | SHA-256 of full encoded storage key |
| 72 | 16 | Collection identity digest: first 16 bytes of SHA-256 of the full collection name |
| 88 | 8 | Zero |

The locator's complete record digest covers the header, full key, and body.
The reader verifies the full key, collection identity, version, exact length, and record digest.
It also recomputes the addressed group from the exact key and the held placement codec.
The collection digest supplements the exact directory key; it never substitutes for that key.
Pack construction copies the stored body bytes. It does not create atoms or change their encryption bytes.

## 7. Complete mutation batches in the recovery journal

The journal is one ordered sequence of complete batches across all affected collections and groups.
A batch is an immutable logical stream in independently readable blocks:
`BEGIN | operation_0 | ... | operation_n | COMMIT`.
The payload parts and map use the rules above. Normal reads can address a record inside this stream directly.

BEGIN and COMMIT are each exactly 128 bytes:

| Offset | Bytes | BEGIN or COMMIT |
|---|---:|---|
| 0 | 8 | BEGIN magic `LDJBG1\0\0` or COMMIT magic `LDJCM1\0\0` |
| 8 | 2 | Format version, 1 |
| 10 | 2 | Marker length, 128 |
| 12 | 4 | Operation count, at most 4,096; a held native root with feature `0x20` permits 16,384 |
| 16 | 8 | Batch sequence |
| 24 | 8 | Previous complete batch sequence |
| 32 | 16 | Journal stream UUID |
| 48 | 8 | Complete logical stream length, including both markers |
| 56 | 8 | Original accepted write time, Unix milliseconds |
| 64 | 32 | COMMIT: SHA-256 of BEGIN and all complete operation bytes; BEGIN: zero |
| 96 | 32 | SHA-256 of this marker with this field zero |

The two markers must agree on sequence, predecessor, operation count, stream UUID, length, and time.
COMMIT must end exactly at the declared stream length.
The journal includes the stored record bytes and original mutation metadata needed for recovery.
It does not rely on the warm set or cloud sync to reconstruct accepted durable changes.

Each operation is `64-byte header | collection name | storage key | operation payload`:

| Offset | Bytes | Operation header |
|---|---:|---|
| 0 | 1 | Kind: put 1, delete 2, conversion-boundary 3; other kinds refused in v1 |
| 1 | 1 | Flags, zero |
| 2 | 2 | Group number |
| 4 | 4 | Operation number, starting at zero with no gaps |
| 8 | 4 | Collection-name length |
| 12 | 4 | Storage-key length |
| 16 | 8 | Payload length |
| 24 | 8 | Complete operation byte length |
| 32 | 32 | SHA-256 of header and all following operation bytes, with this field zero |

A put payload is the complete record from Section 6. Its duplicate key and version must agree with the operation.
A delete has no payload. It installs a versioned tombstone, including deletes of a key already absent.
A conversion-boundary payload is a 128-byte directory value; its collection and group are exact named coordinates.
Unlike a put, that payload references data outside its own journal batch.
Every referenced locator, pack, directory, source descriptor, and retained conversion reference must be durable before its COMMIT is appended.
The converter must acquire a durable conversion reference to that complete dependency closure before COMMIT.
This reference remains until CURRENT or a recovery publication includes the boundary and the required retention rules permit release.
TAIL cannot advance through a conversion-boundary batch without those file and directory barriers.
An ordinary mutation batch still follows Section 8's existing journal-first publication order.
Destructive home, collection, or history commands cannot use a v1 writer until they receive explicit journal operation formats.
They must refuse the enabled feature rather than bypass its journal.

Schema, atom, molecule, author-clock, device, and mutation-log metadata remain in their existing keyed stored records.
The journal batch must include the complete existing store mutation unit and its dependencies.
A batch marker does not claim atomicity for a larger API request split across multiple store batches.
The implementation must map the memory ack and mutation unit to this complete batch before it enables the writer.

## 8. Admission, flush, and atomic publication

Before a memory write and success ack, admission reserves all of these resources together:

1. Protected logical-record slots or the existing protected write state.
2. Complete future journal bytes, block-map pages, and batch markers.
3. Worst-case copy-on-write locator and directory pages, including splits and key overflow.
4. Space for temporary publication files and the replacement pack work already admitted.
5. Recovery-suffix bytes and batch count, including concurrent accepted but unappended work.

The reserve cannot assume immediate removal of an old file.
An admission failure returns a defined retryable resource-limit result before the memory change and ack.
The writer does not remove an unflushed record to make room.
If every warm slot is protected, a cold read uses its bounded call buffer or returns a resource-limit result.

The proposed publication sequence is exact:

1. For a conversion-boundary batch, first complete its external dependency and durable-reference barriers as Section 7 requires.
2. Serialize complete accepted batches in mutation-version order into new journal identities.
3. Finish each batch's COMMIT, part headers, immutable map, and any newly created parent directories.
4. Execute the data durability barrier on every journal part, marker-bearing part, and map file.
5. Execute directory barriers, from each newly created child directory to its already durable parent.
6. Write the new JOURNAL_TAIL temporary file. Execute its file barrier.
7. Rename it atomically over JOURNAL_TAIL on the same filesystem. Execute the parent-directory barrier.
8. Advance in-process `J` only after Step 7 succeeds.
9. Write replacement locator pages and the complete replacement root directory for selected complete batches through `F_new <= J`.
10. Execute file barriers on all new locator and directory objects, then their parent-directory barriers.
11. Write the new CURRENT temporary file and its immutable generation receipt. Execute both file barriers.
12. Execute the receipt directory barrier. Rename CURRENT atomically on the same filesystem.
13. Execute CURRENT's parent-directory barrier. Then exchange the in-process readable root for the new generation.
14. Mark those complete batches flushed. Release their write protection only when the committed reader can locate their complete records.

The platform barrier uses the supported strongest existing LastStore barrier.
On macOS, data uses `F_FULLFSYNC`; directory publication uses the existing directory barrier.
The implementation must check filesystem support for durable atomic publication. It refuses a filesystem that cannot satisfy the contract.
Neither a rename alone nor a successful write establishes durability.

The 500 ms schedule starts this work. A failed or delayed flush does not establish a 500 ms loss window.
Status exposes the oldest memory-acked batch without durable recovery data, `J - F`, reserved suffix bytes, and its oldest age.
A durable journal batch can remain newer than CURRENT. Its record protection remains until Step 14.

Pack materialization uses the same order:

1. Read complete journal records through a held generation or durable journal boundary.
2. Write new immutable pack parts and their immutable block map with fresh identities.
3. Execute every file barrier, then every new-directory barrier.
4. Write replacement locator and directory pages that point to those packs; execute their barriers.
5. Publish CURRENT with the sequence above. Only this publication makes the new pack locations readable.

An old journal prefix remains until the committed roots and all capture consumers release it.
A materializer does not change mutation versions, atom identities, or original write metadata.
It never updates a live locator page or published part in place.

Any barrier, rename, or space failure stops new admissions and leaves protected memory records intact.
The writer reports the failed stage and last durable frontier. It does not report a successful flush.
A publication outcome after a rename but before a directory barrier is uncertain.
The process must not continue writes against a guessed root; it requires supported recovery.
Whether that state exits the daemon or retains a read-only process is a pending release choice.

## 9. Exact reads, batches, ranges, and maximum work

Each call holds one generation. It collects exact coordinates before it performs disk work.
It shares directory pages, locator pages, overflow pages, map pages, and payload blocks within that call.
Four workers return their costs to that call's receipt. They do not use global counter differences as attribution.

For one local record, the physical bound is:

`CURRENT capture + directory path + locator path + comparison overflow + block-map pages + required payload frames`.

A tree path has at most eight pages. A binary page search uses at most seven key comparisons.
Two additional adjacent-boundary comparisons give a maximum of nine full-key comparisons per page.
Each comparison can require at most five overflow pages.
Thus one path uses at most `8 * (1 + 9 * 5) = 368` pages before call-local sharing.
The directory and locator paths together use at most 736 pages in this deliberately pessimistic large-key case.
Keys that fit inline require no overflow pages. These are bounds, not latency claims.
The exact record identity check also reads the full record key inside its payload extent.
Record payload work remains proportional to the requested byte count.

A batch groups requested blocks and pages before each dependency round.
It preserves request order and returns a result for every requested key, including valid absence and per-item errors.
Tip resolution precedes a complete shared atom batch. A UUID-only atom read can require the existing logical atom-locator dependency.
That logical locator is separate from this physical directory.

A range requires a named molecule and one hash. It addresses only its configured groups, currently at most 16.
Each group supplies an ordered cursor at the held generation. The call merges these cursors by the existing encoded key order.
The budget counts examined entries, tombstones, hidden records, pages, frames, and bytes, as well as returned records.
Each call states finite limits before it starts; a missing budget returns a resource-limit error.
A cursor token carries the generation, exact hash scope, last examined full key, and remaining group cursor positions.
The token must use the existing authenticated token mechanism or an explicit new reviewed codec.
If its generation expires, the reader returns an expired-continuation error. It does not silently continue at a newer root.
Budget exhaustion returns a continuation or an explicit resource-limit result, never a false end-of-range.

Before a disk result enters the warm set, the reader rechecks the local write version for every affected logical record.
A newer accepted write replaces the disk result. A disk miss cannot replace a newer local write or tombstone.
The call releases every decoded storage object and handle when it ends.
Long calls require an admission deadline and byte reserve. They cannot create an unlimited retained page set.

## 10. Startup recovery and failure results

Startup holds an exclusive home writer lock. A second writer or converter refuses the home.
Read-only access holds its generation reference and cannot alter recovery files.
The supported writer reads the descriptor, CURRENT, and JOURNAL_TAIL at exact paths before it admits requests.
Unknown formats or required features fail before it opens the home for writes.

Recovery reads only complete batches with `F < sequence <= J` in the named journal stream sequence.
It uses the tail's exact final batch, byte reserve, and count bound; it does not enumerate old files to discover the frontier.
The suffix may require two bounded stream passes: one validates every complete batch, and one writes its replacement roots.
Thus the declared recovery read bound is twice the encoded suffix bytes plus its map and page bytes.
It uses bounded block buffers, not a complete journal-body allocation.
Recovery publishes complete replacement roots through the valid durable frontier before it exposes the affected data.
Ordinary calls perform no replay.

A recovery suffix older than its age limit can still recover. The age limit refuses further admissions; it does not delete durable data.
A suffix above the release byte bound causes a recovery-limit error and requires an explicit compatible recovery command.
The normal node never silently raises its budget. The release procedure must show that normal admission cannot create that condition.
Recovery starts a fresh journal stream before new writes, so interrupted encrypted frame identities cannot be reused.
The previous stream remains addressable while any committed extent names it.

| Failure | Required result |
|---|---|
| Valid locator miss at the held generation | Absence, subject to the local-write version recheck |
| Valid current tombstone | Absence at its mutation version |
| Missing referenced page, part, or marker | Availability or integrity error, with object identity and position |
| Bad checksum, authentication, key equality, version, or extent bound | Integrity error; no absent-key success |
| Unknown format, codec, epoch, flag, or mandatory feature | Incompatible-format or unavailable-key error |
| Cyclic overflow or tree reference, excess height, or excess byte count | Integrity or resource-limit error before further work |
| Incomplete batch beyond durable TAIL | Unpublished data; leave it unreachable and remove it only through owned recovery cleanup |
| Missing or corrupt batch at or below durable TAIL | Refuse recovery; never skip the batch or truncate durable history |
| Corrupt CURRENT | Refuse ordinary startup; use a named retained generation through the recovery procedure |
| Expired range generation | Expired-continuation result |
| Admission reserve exhausted | Retryable resource-limit result before memory mutation and ack |
| Data or directory barrier failure | Stop admissions; retain protection; report the last durable frontier |

An operator-selected earlier generation is recovery work, not an automatic fallback.
It must replay all durable batches after that generation before it accepts writes.
If required batches or keys are unavailable, it refuses and names the missing dependency.
No quarantine option that discards committed batches is part of v1.

## 11. Generation lifetime, backups, and rollback

An immutable generation receipt retains the exact CURRENT bytes, its dependency roots, and its publication result.
Its dependency set includes directory objects, locator objects, overflow pages, block maps, journal batches, pack parts, and cloud descriptors.
It records each required physical part's exact durable length. Any reachable page retains that page's complete physical part.
An active call holds an in-process reference acquired atomically with its root capture.
A backup or rollback hold uses a durable named reference with its generation and purpose.
Reference publication uses a file barrier, atomic rename, and directory barrier before work relies on it.

The reclaimer computes reachability from CURRENT and every retained generation reference.
It can delete a physical object only after all call, backup, conversion, cloud-recovery, rollback, and grace references release it.
It reserves space for those old objects before it accepts replacement work.
It does not assume that a long call or slow backup finishes before the next pack publication.
The backup or rollback policy defines a finite retention period. It does not permit deletion of an active call's dependency.
Disk pressure limits new work first.

A physical-file backup holds one generation and its full dependency closure.
It must include every referenced journal batch even if a newer pack exists outside that held generation.
A source-free restore validates every required object and recreates valid local positions before it publishes CURRENT.
The logical snapshot and post-frontier mutation-log contract also remains supported.
Those logs retain original write time, device identity, atom identity, versions, tombstones, and required history.
One backup mode cannot silently replace the other mode's restore guarantee.

A binary rollback uses a binary that reads the committed v1 directory, journal, and pack format.
Retained old files alone cannot restore writes accepted after conversion.
A rollback to a binary without v1 support requires a completed reverse conversion or a restored legacy home with every later mutation replayed.
Until that proof exists, the release record identifies the bridge reader as the oldest supported rollback binary.
The primary upgrade procedure must save that compatible binary before it enables a new-format writer.

## 12. Version fence and mixed homes

The current home descriptor uses version 1. The proposed directory format uses its own format version 1.
The home descriptor needs a new required feature bit and a version that every older writer refuses.
Version 2 is a proposed home fence, subject to coordination with the separate journal design.
Stage 2 must allocate the final descriptor version and bit values together. It cannot assume that an earlier proposal reserves them.

| Binary | Legacy home | Bridge mixed home | Converted home |
|---|---|---|---|
| Current binary without the fence | Existing support | Refuse before any write | Refuse before any write |
| Bridge reader, new writer disabled | Existing support | Read all explicitly described modes; replay supported durable suffix | Read v1; preserve all dependencies |
| v1 reader and writer | Existing support until explicit activation | Journal every accepted store batch; honor each directory entry's mode | Direct bounded reads and v1 publication |
| Unknown future version | Only declared backward support | Refuse unknown mode or feature | Refuse unknown mandatory feature |

The bridge reader ships before the new writer or converter.
The descriptor fence becomes durable before the first new-format journal batch or conversion publication.
Every offline tool that opens as a writer must honor the fence and the complete batch rules.
A tool without support refuses. It cannot bypass the fence through an alternate open path.

A mixed home is valid only when every group mode appears in the committed root directory.
Stray files do not establish a mode. A converted entry never falls back to legacy files on damage or absence.
The placement, fanout, schema identity, key codec, and encryption identity stay unchanged.
The reader explicitly separates bridge legacy work from the new-format structural result in its receipts.

## 13. Bounded conversion with concurrent writes

The converter uses supported product operations on a fresh generated-data node for its first proof.
It does not copy or open the primary for that proof. The later primary procedure uses the supported upgrade process.

The proposed boundary protocol is:

1. Install the compatible bridge reader and the durable descriptor fence.
2. Under the writer gate, complete the old-format flush and capture frontier `B`.
3. Seal exact legacy source identities at `B`. Publish their immutable paged source descriptors.
4. Enable journal capture for every later accepted store batch before it receives a memory ack.
5. Release the writer gate. Legacy source files do not receive further writes.
6. Serve each unconverted group through its captured source plus the current journal-backed overlay tree.
7. Convert one named collection and group boundary at a time with the declared worker reserve.
8. Build a proposed complete locator from the captured source and the journal changes after `B`.
9. Under the publication gate, select complete durable frontier `C` and include every batch through `C` in the complete proposed group root.
10. Reserve the next boundary batch `K` and its publication generation. Set the converted boundary frontier to `K`.
11. Make the complete proposed root, its pack and descriptor dependencies, and its durable conversion reference durable.
12. Append the conversion-boundary batch and COMMIT, then publish TAIL through `K` with the required barriers.
13. Publish CURRENT through that complete boundary. Only then serve the group through the converted root.
14. Later batches update that converted root normally. Retain the old source until every required reference releases it.

The journal capture point, memory ack point, and publication gate require an implementation review.
A capture gap, an unjournaled direct writer, or an incomplete API-to-store batch mapping prevents conversion activation.
Accepted concurrent writes retain their protected memory state until the committed reader includes them.
The converter compares mutation versions. It never replaces a newer journal write with an older source value.

The source descriptor records exact source file identities, byte lengths, source format, checksum when available, and frontier `B`.
Its paged entries use fixed widths and a declared page count. The converter does not discover sources through an unbounded directory walk.
Appendix A defines the proposed source descriptor and resume receipt bytes.
The converter still needs an exact legacy path and source-order adapter for every supported source format.
An unavailable adapter prevents conversion; v1 ordinary converted-group reads do not use it.

Each selected old source is read once with bounded stream buffers.
For unsorted plain sources, the converter writes a bounded external sort of key, source version, and new extent metadata.
It streams record bodies to new packs as it reads them. It does not allocate a complete old file or group.
Merge work uses at most eight input buffers per worker. Large bodies do not enter the sort buffer.
For old sorted sources, a bounded footer reader supplies their metadata. The existing complete-footer loader is not the converter interface.
Duplicate legacy changes retain the current source-order semantics. A later delete remains a tombstone.
Required atom history and shared file data remain under their existing retention rules.

An interruption leaves the prior committed directory entry readable.
Before a durable boundary COMMIT, an interruption leaves conversion output protected by its named conversion reference.
After TAIL reaches `K`, startup recovery must replay that complete conversion operation before it exposes the affected data.
Recovery verifies the complete named dependency closure before it installs the directory value.
A missing dependency below durable TAIL refuses recovery. It cannot fall back to the legacy source and hide later writes.
Rollback uses the compatible bridge binary, which must decode the conversion-boundary operation and its dependencies.
Retained legacy files alone do not reverse the boundary or restore writes accepted afterwards.
A progress receipt records completed source identities, output object identities, durable frontier, and a checksum of the conversion plan.
A resume uses exactly those identities or restarts the selected boundary with fresh output identities.
It never rereads a completed source to infer missing progress, and it never publishes partial output as a complete group.
Unreferenced temporary outputs require exact-path owned cleanup after their receipts are preserved.
An unsupported old key, name, record, or source codec produces an explicit conversion refusal.

## 14. Cloud-only locations and restore dependencies

Local positions and cloud positions are separate records.
A local repack does not update a cloud snapshot, remove mutation logs, or authorize deletion of local payload bytes.
The existing backup pack map locates an original file inside a cloud pack; it does not locate arbitrary records inside that original file.

A proposed cloud descriptor occupies one or more common pages and contains:

- A versioned cloud provider and authenticated object identity.
- Object generation, encryption key epoch, and complete authenticated object length.
- The exact independent frame offset, stored length, decoded length, and complete record digest.
- The pack and block-map dependencies needed to recover the local position.
- The cloud snapshot frontier and required post-frontier mutation-log boundary.

The descriptor must support a bounded fetch of the required independent frames.
It must authenticate the object identity and exact frame bytes before record decode.
A cloud-only locator is invalid until the supported cloud reader can satisfy those requirements.
If the current cloud format requires a complete snapshot fetch, retain the local payload.
An available whole-snapshot backup is sufficient for that restore path but does not establish a cheap read by key.

Appendix A defines the proposed cloud descriptor bytes. Provider identifiers and authentication adapters require the cloud-reader stage.
Stage 2 local v1 must refuse source kind 3 until those adapters are frozen and implemented.
No local reclaim decision can depend on a deferred cloud-only entry.
Source-free restore must demonstrate complete snapshot plus mutation-log recovery before Stage 4 completes.

## 15. Review decisions and completion evidence

These unresolved decisions prevent activation of the relevant stage:

| Decision | Required before |
|---|---|
| Freeze page, block, part, tree, and worker values | Stage 2 reader and writer |
| Allocate descriptor version and required feature bits | Bridge release and first new-format write |
| Approve exact block encryption context, nonce identity, and epoch lookup | Encrypted writer |
| Map memory acks and every direct writer to complete store mutation batches | Writer and converter activation |
| Approve runtime batch, suffix, age, protected capacity, and aggregate call budgets | Writer and read integration |
| Choose exit or read-only refusal after an uncertain durability result | Writer release |
| Freeze authenticated range continuation bytes and generation expiry policy | Range integration |
| Review source descriptor bytes and implement every exact legacy adapter | Converter |
| Review cloud descriptor bytes and implement authenticated independent cloud fetch | Cloud-only entry and local payload removal |
| Select backup and rollback generation retention and disk reserve policy | Restore, conversion, and primary preparation |
| Add formats for destructive store operations that currently bypass ordinary batches | Those commands with v1 enabled |

Stage 1 format review checks byte boundaries, integer overflow, checksum domains, full-key equality, and every failure result.
It also checks that an existing valid key remains representable or causes a named conversion refusal.
This draft supplies local byte layouts and an exact publication order.
The cloud and legacy adapters, token codec, and home-version allocation remain visible blockers.
This draft does not claim that any new format is implemented.

Later product proof uses fresh nodes with generated data and ordinary supported commands.
Receipts must show exact, absent, multi-field, batch, range, restart, pack replacement, conversion, and source-free restore results.
They must also show zero ordinary converted-group reconstruction, full-footer reads, and journal replay.
The receipt reports page counts, payload bytes, dependency rounds, shared-block reads, and call time as data grows.
Status must respect the 10,000-record warm-set cap and expose the oldest memory ack without durable journal data.
No test suite, fixture harness, primary copy, or primary upgrade forms part of this draft.

## Appendix A. Proposed metadata byte layouts

All objects below use the common page header, integrity rules, and immutable 8 MiB paged parts.
Metadata kinds are cloud descriptor 6, cloud-part leaf 7, cloud-part descriptor 8,
legacy source descriptor 9, conversion receipt 10, generation receipt 11, durable reference 12, and dependency inventory leaf 13.
Each field list states offsets within the page payload, which starts at byte 128.
Every undeclared payload byte is zero. Each descriptor page has entry count one and entry width zero.

### A1. Cloud descriptor

A cloud descriptor page has a 256-byte fixed payload followed by two exact UTF-8 byte strings.
Each string is at most 4,096 bytes. Its used payload length is `256 + provider_locator_length + snapshot_identity_length`.
Strings are provider identifiers, not local paths. Unknown provider or extent codec identifiers refuse the cloud-only entry.

| Payload offset | Bytes | Field |
|---|---:|---|
| 0 | 2 | Provider identifier; allocated by the cloud reader |
| 2 | 2 | Extent codec; proposed 1 means independent v1 pack frames |
| 4 | 4 | Flags, zero |
| 8 | 8 | Snapshot frontier |
| 16 | 8 | First required post-frontier mutation batch |
| 24 | 8 | Encryption key epoch |
| 32 | 16 | Payload object UUID |
| 48 | 8 | Complete logical record length |
| 56 | 8 | First logical block ordinal |
| 64 | 4 | First position in the decoded block |
| 68 | 4 | Payload block count |
| 72 | 16 | Block-map object UUID |
| 88 | 8 | First block-map page; ordinal addressing starts from this page |
| 96 | 16 | Cloud-part directory object UUID |
| 112 | 8 | Cloud-part directory root page |
| 120 | 2 | Cloud-part directory height, at most eight |
| 122 | 2 | Zero |
| 124 | 4 | Provider-locator byte count |
| 128 | 4 | Snapshot-identity byte count |
| 132 | 4 | Zero |
| 136 | 32 | Complete record SHA-256 |
| 168 | 32 | Authenticated snapshot manifest identity digest |
| 200 | 32 | Required mutation-log manifest identity digest; zero only when no logs are required |
| 232 | 24 | Zero |
| 256 | Variable | Provider locator, then snapshot identity |

The cloud-part directory uses the common internal tree pages and kind-7 leaves.
Its key is an eight-byte lowercase ASCII hexadecimal part number.
Its 128-byte value contains a descriptor page number in bytes 0 through 7 and zero bytes afterwards.
That descriptor is in the same cloud-part directory object.

A kind-8 cloud-part descriptor has a 128-byte fixed payload, then its exact object name and object version.
Each string is at most 4,096 bytes; the complete used payload is at most 8,320 bytes.
An empty object version is permitted only when the authenticated provider identity is immutable without that version.

| Payload offset | Bytes | Field |
|---|---:|---|
| 0 | 4 | Physical part number |
| 4 | 4 | Object-name byte count |
| 8 | 4 | Object-version byte count |
| 12 | 4 | Flags, zero |
| 16 | 8 | Complete cloud object length |
| 24 | 8 | Byte position of the physical part within that cloud object |
| 32 | 8 | Complete physical part length, at most 8 MiB |
| 40 | 8 | Encryption key epoch |
| 48 | 32 | Complete physical part SHA-256 for restore |
| 80 | 32 | Authenticated backup manifest entry digest |
| 112 | 16 | Zero |
| 128 | Variable | Exact object name, then exact object version |

For one cloud frame, the fetch starts at `cloud_part_position + block_map.part_offset`.
The fetch length is exactly the map's stored frame length. Every addition remains within both part and cloud object bounds.
The provider adapter verifies membership in the authenticated snapshot manifest and the immutable object identity.
The frame's independent authentication and record checks then verify the requested bytes.
An unkeyed page or part digest alone cannot authenticate a cloud result.
A whole-object compression or encryption wrapper without independent byte extents is unsupported for a cloud-only entry.

### A2. Legacy source descriptor

A kind-9 source descriptor is a bounded page chain. Its header auxiliary next field names the next descriptor page.
Each page begins with the same 128-byte descriptor prefix, then at most 53 entries of 304 bytes each.
The common entry width is 304; its used payload length is `128 + entry_count * 304`.
The header auxiliary total names the complete descriptor page count, at most `u32::MAX`.
The complete source entry count is also at most `u32::MAX`.
The sum of entry counts must equal the declared source count, with no missing ordinal.

| Payload offset | Bytes | Repeated descriptor prefix |
|---|---:|---|
| 0 | 8 | Captured legacy frontier `B` |
| 8 | 2 | Descriptor format, 1 |
| 10 | 2 | Flags, zero |
| 12 | 2 | Exact group number |
| 14 | 2 | Zero |
| 16 | 32 | Exact collection-name SHA-256 |
| 48 | 8 | Total source entries |
| 56 | 8 | First source ordinal in this page |
| 64 | 8 | Complete descriptor page count |
| 72 | 16 | Captured home identity |
| 88 | 32 | Conversion plan digest |
| 120 | 8 | Zero |

An entry contains a 176-byte full-key descriptor for the exact legacy relative file name and a 128-byte source value.
The name can use the existing bounded key-overflow rule.
This is the sole path exception to Section 3: an existing relative source name can include current legacy subdirectories.
The adapter validates the exact captured path under the named collection and group before it opens it.
It refuses absolute paths, `.` or `..` components, symlinks outside that captured source root, or unsupported path shapes.
No name supplies a new output path.

| Value offset | Bytes | Source value |
|---|---:|---|
| 0 | 1 | Source format: plain segment 1, encrypted frame 2, sorted block file 3, sorted physical part 4 |
| 1 | 1 | Flags: bit zero means complete file digest present; others zero |
| 2 | 2 | Zero |
| 4 | 4 | Physical source part number, or zero |
| 8 | 8 | Exact legacy source-order ordinal |
| 16 | 16 | Source file UUID when its format supplies one; otherwise zero |
| 32 | 8 | Exact captured file length |
| 40 | 8 | Included byte start |
| 48 | 8 | Included byte length |
| 56 | 8 | Last source sequence when available; otherwise zero |
| 64 | 32 | Complete captured-file SHA-256 when present; otherwise zero |
| 96 | 8 | Captured modification time, Unix nanoseconds; descriptive metadata only |
| 104 | 24 | Zero |

Included start plus length must remain within the captured file length.
The descriptor orders sources by the explicit source-order ordinal, not their filename or modification time.
The converter does not invent a checksum for an old format that lacks one.
It can calculate and record one as it reads that source once.
The exact adapter must reproduce current legacy replay order and record boundaries for its source format.
Unsupported or ambiguous order blocks conversion of the whole named boundary.

### A3. Conversion resume receipt

A kind-10 receipt has exactly 256 used payload bytes:

| Payload offset | Bytes | Field |
|---|---:|---|
| 0 | 16 | Conversion plan UUID |
| 16 | 8 | Captured frontier `B` |
| 24 | 8 | Last incorporated complete durable frontier |
| 32 | 32 | Exact collection-name SHA-256 |
| 64 | 2 | Group number |
| 66 | 1 | State: source work 1, merge 2, ready for publication 3, published 4 |
| 67 | 1 | Flags, zero |
| 68 | 4 | Zero |
| 72 | 16 | Legacy descriptor object UUID |
| 88 | 8 | Legacy descriptor first page |
| 96 | 8 | Last completed source ordinal; `u64::MAX` means none |
| 104 | 8 | Incomplete source ordinal; `u64::MAX` means none |
| 112 | 8 | Durable source cursor at a complete record boundary |
| 120 | 16 | Output pack directory object UUID |
| 136 | 8 | Output pack directory root page |
| 144 | 2 | Output pack directory height |
| 146 | 6 | Zero |
| 152 | 16 | Metadata spool manifest object UUID |
| 168 | 8 | Metadata spool manifest first page |
| 176 | 8 | Reserved publication generation; zero before ready-for-publication state |
| 184 | 16 | Proposed complete locator object UUID, or zero |
| 200 | 8 | Proposed complete locator root page |
| 208 | 2 | Proposed complete locator height, or zero |
| 210 | 6 | Zero |
| 216 | 32 | Complete conversion-plan digest |
| 248 | 8 | Zero |

The output directory and spool manifest use kind-13 leaves with exact immutable output identities as keys.
Their 128-byte values contain one 80-byte generation dependency entry followed by 48 zero bytes.
The plan digest covers the captured source descriptor, group, baseline frontier, format values, and converter budgets.
A different plan cannot consume the receipt.
The receipt advances only after the named output parts and spool pages receive file and directory barriers.
An interrupted record resumes from its last durable complete-record boundary with fresh output identities.
A completed source uses its retained output; the converter does not reread it.
The converter may reread an incomplete record after an interruption. That exception is bounded by the declared record size.

### A4. Generation receipt and durable reference

A generation receipt uses kind-11 pages at `generations/<u64-generation>/part-<u32>`.
Its first page number is zero. Its object UUID appears in every page header and in each durable reference.
Its first page contains a 128-byte prefix, then the exact 4,096 CURRENT bytes, then dependency entries.
Later pages contain the same 128-byte prefix without CURRENT, then dependency entries.
The page header auxiliary next field forms the exact chain. The declared page count bounds it.

The 128-byte prefix contains, in order:
`generation:u64`, `frontier:u64`, `dependency_count:u64`, `first_dependency_ordinal:u64`,
`page_entries:u32`, `current_bytes:u32`, `current_sha256:[u8;32]`, `receipt_page_count:u64`,
`publication_time_ms:u64`, `required_features:u64`, and 32 zero bytes.
Thus these fields occupy payload bytes 0 through 127 with no padding.
`current_bytes` is 4,096 on the first page and zero on other pages.
Each complete dependency entry is exactly 80 bytes:
`kind:u8`, `flags:u8`, `zero:u16`, `part_number:u32`, `object_uuid:[u8;16]`,
`journal_batch:u64`, `durable_length:u64`, `complete_part_sha256:[u8;32]`, and `creation_generation:u64`.
Kinds identify directory, locator, block-map, journal-data, pack-data, cloud-descriptor, or conversion-source parts.
Their proposed numeric values are respectively 1 through 7.
For a legacy source dependency, the UUID names its exact source descriptor and the batch field names its source ordinal.
The descriptor supplies the validated existing path. Its captured length supplies the dependency length.
For every other local dependency, the UUID, kind, batch, and part number determine the exact new-format path.
The checksum-present flag distinguishes a verified whole-part digest from zero.
This receipt is operational metadata. An ordinary read does not enumerate its dependency entries.
An incremental receipt may conservatively retain prior dependency parts and private intermediate tree parts.
These extra references prevent premature deletion. They do not change the committed directory's readable keys.
A compaction can rebuild the live dependency closure under the required generation and durable-reference rules.

The first receipt page contains at most 150 dependency entries.
Every later page contains at most 201 entries. The exact used payload length follows its prefix and count.
Receipt publication precedes CURRENT publication as Section 8 requires.
The first page's embedded CURRENT digest must equal the prefix digest and the selected generation's CURRENT bytes.

A durable backup or rollback reference uses one kind-12 page at `references/<reference-uuid>`.
Its 128-byte payload contains, in order:
`reference_uuid:[u8;16]`, `generation:u64`, `receipt_uuid:[u8;16]`, `receipt_first_page:u64`,
`purpose:u8`, seven zero bytes, `created_time_ms:u64`, `expiry_time_ms:u64`,
`current_sha256:[u8;32]`, and 24 zero bytes.
Purpose is backup 1, rollback 2, conversion 3, or cloud recovery 4. Unknown purposes refuse the reference.
For backup, rollback, and cloud recovery, the generation and receipt identity must match the validated kind-11 generation receipt.
For conversion, the receipt identity instead names a kind-10 ready-for-publication conversion receipt.
Its reserved generation must equal the reference generation. Its output inventory holds the complete external dependency closure.
The conversion reference's CURRENT digest is zero until a planned CURRENT exists, or equals that planned CURRENT's digest.
This purpose-specific reference does not require the future journal batch or a published generation receipt before COMMIT.
After CURRENT includes the boundary, a generation reference replaces this conversion reference before the converter releases it.
Expiry prevents new work from acquiring the reference. It does not release an active call or incomplete required backup.
The supported owner command releases that work and removes its durable reference with a directory barrier.
The reclaimer never infers release from elapsed time alone.
