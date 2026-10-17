# Local native storage completion, October 16, 2026

CAUTION: The first candidate exposes two defects below.
The corrected candidate must complete the remaining checks before this report states local completion.

## Frozen candidate

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
The new candidate must repeat the product checks with those two stored forms.

## Corrected candidate results

| Product path | Actual receipt |
|---|---|
| Shared data through two protein keys | Pending |
| Native local backup | Pending |
| Restore without the source path | Pending |
| Native compaction after restore | Pending |
| Legacy to native conversion | Pending |
| Latest native data to legacy conversion | Pending |

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
The final result records each owned node's clean stop and scratch cleanup state.
