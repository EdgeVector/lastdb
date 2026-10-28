# Disk format V2 compatibility contract

This document records the current file contract and the rules for a compatible change.
The physical format remains V2. Its component version numbers have separate meanings.
The [native node design](lastdb-disk-native-node.md) describes the memory and request boundaries.

## Files and local authority

```text
<data>/laststore-pack-node-v1
<data>/disk-run/laststore-run-home-v2
<data>/disk-run/LOCK
<data>/disk-run/CURRENT
<data>/disk-run/runs/<UUID>.ldrun
<data>/disk-cas/laststore-run-home-v2
<data>/disk-cas/LOCK
<data>/disk-cas/CURRENT
<data>/disk-cas/runs/<UUID>.ldrun
```

`disk-run` holds the main records. `disk-cas` holds the `cas_blobs` records.
The separate files permit different cloud destinations without a format conversion.
Each authenticated `CURRENT` names the complete readable files of its own home.
It records the home identity, physical generation, logical frontier, ordered run sets, and any incomplete base merge.
The frontier advances for a complete native commit. Compaction advances the generation without a new logical frontier.

Ordinary reads use one captured committed state and the logical warm set.
They use neither the private write queue nor the mutation log.
A range reads matching ordered keys from the required runs and resolves newer values and tombstones first.
The [LastStore engine](../vendor/laststore/README.md) describes the file reads and compaction.

## Accepted version tuple

| Component | Current writer | Current reader |
| --- | --- | --- |
| Node marker | `format=4`, `home=disk-run`, `cas=disk-cas`, `ranges=typed-v1`, each followed by LF | That exact typed marker, or the exact legacy `format=3`, `home=disk-run`, `cas=disk-cas` marker |
| Home descriptor | 64 bytes; magic `LDRHOME3`; descriptor version 1; home UUID; CRC32 | The same descriptor; `LDRHOME2` is refused |
| `CURRENT` payload | Magic `LDRCUR2\0` inside one encrypted frame | `LDRCUR2\0`, or `LDRCUR1\0` without a base; the latter opens as tiered runs |
| Run footer | 64 bytes; magic `LDRUN\x01\0\0`; version 1; run UUID; top-block position; counts; CRC32 | The same footer, with counts checked against the authenticated top block |
| Encryption frame | Magic `LSF1`; version 2; raw or zstd flag; authenticated header; ciphertext; 16-byte tag | Version 2 with known flags only |
| Stored value | Tag 0 for original bytes; tag 1 for compact JSON | Tags 0 and 1 only |
| Logical key format | `7;typed-ranges=v1` in a new default home | Typed 7, or supported legacy 6 and `6;delete-replay-cutoff=v1`, as selected by the node marker |
| Client backup profile | Profile 2 for typed ranges; profile 1 for legacy ranges | Both profiles, with the exact native identities, hashes, totals, and required features |

Each marker is an exact byte contract. Its filename alone does not prove compatibility.
The logical delete replay cutoff requires its corresponding stored cutoff record.
The node checks the logical format and account proof before ordinary access or writer cleanup.
The [typed range contract](designs/typed-range-field-contract.md) defines the complete scalar and molecule profiles.

A fresh default home uses blinded hashes and typed ordered ranges inside encrypted blocks.
An explicit legacy home keeps its recorded key encodings and text order.
Environment settings do not convert an existing home. Incompatible typed settings receive a refusal.
An absent or empty home can initialize. Startup does not migrate an older home or resume an offline conversion.

An explicit offline typed conversion remains separate from startup and does not restore the removed physical V1 engine.

## Record and encryption bytes

A run key contains a two-byte big-endian collection length, the collection bytes, and the complete storage key.
Records use strict ascending byte order. Duplicate keys within a commit resolve to its last mutation before the run write.
Each block stores common key prefixes once and uses unsigned LEB128 lengths.
A record stores a tombstone or its tagged value.
Tag 1 must reproduce the exact original compact JSON bytes; otherwise the writer uses tag 0.
The tag 1 key dictionary and element codes cannot change under that tag.

The [value codec](../vendor/laststore/src/disk_run/value/mod.rs) defines those codes and the fixed dictionary.
The [filter codec](../vendor/laststore/src/disk_run/bloom.rs) fixes its hash and seven-probe rule.

Main and CAS home keys use HKDF-SHA256 with separate labels: `lastdb/run-home/v2` and `lastdb/run-home/v2/cas`.
Each frame key uses its UUID as the HKDF salt and `laststore/frame/aes-256-gcm/v1` as the label.
A block uses its run UUID and block counter. `CURRENT` uses a fresh random frame UUID for every publication.

The frame uses AES-256-GCM. Its complete header is authenticated.
The nonce contains four zero bytes followed by the eight-byte big-endian counter.
Data counters start at zero; lookup counters start at `1 << 62`; the top counter is `1 << 63`.
The writer uses zstd level 3 only when the compressed bytes and length prefix are smaller.

Keys, lookup contents, atoms, tips, counters, and control values all receive block encryption.
Atoms use plain logical JSON within those encrypted blocks, without another atom envelope.
Portable file ciphertext retains its separate format inside CAS records.
Descriptors, footers, frame headers, file lengths, and file counts remain readable.
The private identity seed remains a separate local file and never enters a native backup.
The [at-rest threat model](security/at-rest-threat-model.md) states that separate key boundary.

## Commit, crash, and cleanup

The writer completes the new run, flushes its file, renames it, and flushes the run directory.
It then writes and flushes a temporary `CURRENT`, renames that file over `CURRENT`, and flushes the home directory.
Only then does it publish the new readable root.
The node sends the ack after the complete disk commit and clean logical publication.

A crash before publication leaves the previous `CURRENT` as the authority.
A crash at publication can leave either complete selection. Reopen accepts the authenticated selection that exists.
An uncertain `CURRENT` replacement stops further writes to that run home until reopen.
Open removes temporary files and runs that `CURRENT` does not name.
Missing named files, wrong sizes, unsupported versions, and authentication failures receive errors.
Local recovery does not replay a mutation log and does not select an older root to hide corruption.

A compound mutation uses the order: CAS additions, main changes, then CAS deletions.
Each home has its own publication boundary. The pair is not one filesystem transaction.
A crash between those boundaries can leave unreferenced CAS bytes, but cannot publish a reference before its file exists.
An error after the main commit can also leave a failed CAS deletion for separate cleanup.

Compaction uses the same durable file and `CURRENT` publication order.
Each step reserves space before an output frame, footer, or `CURRENT` write.
The shared ledger covers filesystem capacity across both homes and checks each configured home cap.
Written bytes replace the unwritten reservation. The step retains its publication allowance through the required file and directory barriers.

An admission refusal leaves the previous `CURRENT` readable. Output owners attempt to remove unpublished files; reopen removes any unnamed leftovers.
An uncertain publication retains the required files and stops that run-home writer until reopen.
Old reads keep their exact files until the last holder releases them.
Backup holds retain bytes independently. Local compaction cannot promise erasure from a retained backup.

## Limits and compatible tuning

| Limit | Current value or rule |
| --- | --- |
| Complete physical key | At most 64 KiB, including collection prefix |
| Tagged stored value | At most 32 MiB; the home accepts at most 32 MiB minus one byte before its value tag |
| Decoded frame or top block | At most 64 MiB |
| `CURRENT` file | At most 64 MiB; its writer also checks the payload before encryption |
| Manifest shape | At most 32 tiered levels and 4096 runs per level, merge set, or base |
| Native commit | At most 16,384 physical mutations |
| Logical atom content | 256 KiB default; configured range 1 KiB through 1 MiB |
| Logical warm set | 256 MiB default byte budget; no former 10,000-record retention cap |
| Pending native writes | 512 MiB shared memory allowance |
| Ordinary read work | Separate limits for keys, pages, blocks, and cumulative bytes; an explicit resource error ends excess work |
| Backup work | 96 MiB memory, four streams with 1 MiB buffers, and 1 GiB durable spool policy |

These limits do not define a maximum for the complete process memory size.
The physical value limit does not enlarge the logical atom limit.
The read work limit does not define the logical warm-set size or the database record count.

The current block targets are 16 KiB, with ten filter bits per key.
A tiered merge becomes due at eight runs. A tiered step reads at most 64 input runs.
Base work starts at the configured tiered or stale-byte thresholds.
Those targets, schedules, resource allowances, and disk-space reservations are policy, rather than a new file format.
Any new output must still fit the hard limits of every supported reader.
The 64 KiB stale-byte floor and 1 MiB tiered-byte floor remain separate.

File bytes can temporarily exceed the settled size while writes, old reads, or backup holds retain files.
Neither a settled size ratio nor one benchmark gives a universal peak size bound.

## Cloud and compatibility changes

The cloud accepts opaque byte objects, hashes, lengths, access rules, and receipts.
The LastDB client owns file selection, native format checks, encryption keys, restore, and mutation replay.
The client retains the same encrypted run bytes for both a snapshot and the optional sync mutation log.
The mutation log supports cloud backup and replay; it is not an internal local recovery journal.
A log gap requires a new complete snapshot before a later log frontier can become protected.
An incomplete upload never replaces the preceding accepted recovery point.

The client profile and encrypted recovery metadata describe the physical and logical contracts.
The service does not decode them. A native format change does not require cloud knowledge of LastDB files.
The current backup feature set is 176: record references 16, large batches 32, and atom records 128.
Those feature bits occur in client backup metadata, not in the local home descriptor.

A compatible change preserves every accepted byte meaning and reads existing supported files without conversion.
A change to key order, value tags, the dictionary, encryption labels, or required semantics needs an explicit version decision.
Use a new applicable tag or marker and an explicit reader/writer compatibility rule before a writer emits different bytes.
A backup feature bit alone cannot protect a local home from an incompatible older program.
Keep unknown-version refusal and startup admission checks. Do not infer a format from application data.

CI supplies product builds, disk measurements, and crash/reopen checks.
The [cloud recovery operation](cloud-backup-recovery-ci.md) checks automatic snapshots, logs, restart, and source-free recovery through both providers.
Its accepted-main result is separate from recipe syntax checks and PR source review.
The primary and its archive remain outside these checks.
