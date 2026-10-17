# Local native storage completion, October 16, 2026

The corrected candidate completes the local backup, restore, compaction, and forward/reverse conversion checks below.
The final node-only query guard receives a separate reopen check.
The first candidate's two defects and their receipts remain recorded below.

## Component versions

The corrected recovery checks use source `55c6d9aedf20bc66f47669df1114d95fde5d0fb4`.

- `lastdbd` SHA-256: `c6d9aac4a84028a586d11a1824a6c6b0846eba4115d3c8b1e38af33ab59d21a9`.
- `lastdb-disk-recovery` SHA-256: `b0f8cb7c408c83194d0f58c6bceb11f38eed0acf104dbbc69d03c651b8410759`.

The later node-only query guard changes no backup, restore, or converter source.
Its final node hash and reopen receipt remain pending below.

The first candidate uses these separate versions:

- Source: `d1fae6469f6ea0244840e604d8212700660106e1`.
- `lastdbd` SHA-256: `bd18e500cd961a1867664ed843c16389921bb44a8e8cd8d2adadeee192e5a2d6`.
- `lastdb-disk-recovery` SHA-256: `17aff1a5ec70f2e6d4605d7df5074206647096840cca5a50bd9a35cf2f3010c1`.

## Local scope

The product commands use fresh generated homes with their own node identities.
They use no primary data, primary copy, archive, shared service, or cloud account.
The request files use the ordinary schema, mutation, and query routes.
The schemas share five canonical fields and use two different lookup keys.
The node binds those fields through proteins.

## First candidate results

The first native node creates 24 records through the ID schema.
Both schemas return those records through their different keys.
Their field metadata names the same shared atoms.
One batch updates record 000 and deletes record 023 through the alternate key.
Both schemas then return 23 records and the same updated fields.
Both bounded ranges return records 010 and 011 with a continuation.
The two absent-key queries return no records.

The live backup records generation 228 and last batch 113.
It copies 1,009 files and 16,977,194 bytes.
After the node stops, its original native directory moves to `source-unavailable`.
The restore command receives only the bundle and new destination paths.
It restores the same file count, byte count, generation, and last batch.
The original generated node identity and encryption keys stay in their original node home.
The reopened node returns the expected two-key data, ranges, update, deletion, and absences.

The source's clean shutdown advances its root after the backup cut.
Five `keep_small` records differ between that later source and the restored cut.
The other 985 record bodies, keys, and physical versions match.
See [the exact record comparison](native-record-comparison.json).

### Restore directory defect

The restored cut contains recent journal data and no live packed file.
The restore omits the empty `packs` directory.
Compaction preview succeeds, but execution returns an error before a replacement root can publish.
The writer reports failure and refuses a false durability result.
The node stops with exit status 1, and its failed storage remains retained.

See [the error body](native-compact-failure.txt), [the log](native-compact-failure.log), and [the writer state](native-compact-failure-status.json).
The source fix creates and syncs all four required native directories before the restored root becomes available.
The capacity quote includes those empty directories.
The exact data-file inventory check stays intact.

### Forward field-codec defect

The first forward conversion copies 939 records and completes its saved conversion record.
The original node home selects the new native storage.
Exact reads return 23 records, but their older field values appear as encrypted text.
Both bounded ranges return no records.
See [the failed query receipt](forward-failure-query.json).

The factory omits the atom content key for a native backend.
The opaque converter preserves older field encryption correctly, but the native read cannot open it.
The source fix gives native plain frames the same field codec as plain hash-group storage.
That codec also reads older native data whose fields lack the inner encryption envelope.
The corrected product checks below cover those two stored forms.

## Corrected candidate results

| Product path | Actual receipt |
|---|---|
| Shared data through two protein keys | [Matching atom IDs](final-shared-atoms.json) through different molecule IDs |
| Older native content, source-free restore | [1,009 files; 16,977,194 bytes](final-old-native-restore.json); [both keys and ranges correct](final-old-native-query.json) |
| Native compaction after restore | [960 live records; 30 tombstones](final-old-native-compact.json); parts 1,008 → 805; 1,012 files removed |
| New native write and local backup | [879 files; 16,068,100 bytes](final-native-backup.json), generation 240, last batch 118 |
| New native source-free restore | [Same file count and bytes](final-new-native-restore.json); [all 24 latest records through both keys](final-new-native-restored-query.json) |
| Legacy to native conversion | [939 records copied](final-forward-convert.json); [plaintext values and both ranges preserved](final-forward-query.json) |
| Latest native data to legacy conversion | [1,008 records; 409,319 opaque bytes](final-reverse-convert.json); [later update and new record preserved](final-reverse-query.json) |

The exact-key requests name records 000 through 024.
After the alternate-key deletion, both schemas return 23 records and omit record 023.
The later native batch changes record 000 and creates record 024.
Both schemas then return 24 records, omit record 023, and retain the latest fields.
Both bounded ranges return 010 and 011 with a continuation.
Both absent-key queries return no records.

The logical response digest includes the statuses, keys, field values, range continuation, and absence results.
It excludes timings, receipts, and other storage metadata.

| Before and after | Matching SHA-256 |
|---|---|
| Fresh legacy data → converted native data | `f4e0ba89a293328f90b0d67d25e91232a550c3e5155296568fd16faece38085d` |
| Latest native data → restored native data | `acc61ba2335897e0f520fbecc046398631c5683b65d8ddf15cdc77825c362c11` |
| Later converted-native data → reverse-converted legacy data | `acc61ba2335897e0f520fbecc046398631c5683b65d8ddf15cdc77825c362c11` |

The compaction receipt records files 1,020 → 814 and bytes 17,347,123 → 14,701,451.
The explicit zero grace applies only to this generated home.
A query request overlaps the pending compaction request and returns the complete two-key result.
This receipt does not measure device-cold performance or prove an exact read across the publication instant.

## Final node-only reopen

Pending the final query-guard node binary.
This check reuses the stopped restored native home and the same generated identity.
It repeats the complete key/range batch and title/body/title projections.
It changes no backup or conversion result above.

## Cloud boundary

These results concern Layer 2 local storage.
Native cloud upload and bootstrap integration remains absent Layer 3 work.
The startup refusal remains active for a native home with cloud configuration.
Local backup does not establish cloud recovery or primary upgrade readiness.

The factory rejects native cloud configuration in `factory/local.rs:141` and `:149`.
The native opener supplies no `LastStoreNamespacedStore` backup source.
The cloud snapshot publisher still requires that source type.
The generic logical snapshot path reads each complete namespace through `scan_prefix`.
The native implementation limits that result to 10,000 records and does not supply a held, bounded generation export.

## Operational commands and cleanup

See [the product command sequence](operations.md).
The corrected native and reverse-converted legacy nodes stop with exit status 0.
Their [shutdown receipts](final-shutdown-summary.json) report a complete flush.
The generated scratch root remains retained for the final node-only reopen check.
It contains no primary data or archive data.
