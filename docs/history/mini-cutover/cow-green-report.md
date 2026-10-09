GREEN

# Mini cutover Phase 2 CoW GREEN report

- Captured at: `2026-07-18T01:15:15Z`
- Source home (offline): `/Users/example/lastdb-cloudtest`
- Copy mode: `clone`
- Work dir: `/tmp/mini-cutover-cow-green-run`
- Snapshot entries: 6062
- Samples checked/matched: 45/45
- Schema catalog entries (snapshot / laststore): 12 / 12
- Compact after restore: True
- Reopen after compact: True
- Durability window: LastStore batch_put is transactional per batch; flush persists before reopen. Crash mid-batch may lose the current batch only (no silent corruption expected).

## Namespaces

| namespace | snapshot entries | laststore entries |
| --- | ---: | ---: |
| `org_sync_targets` | 0 | 0 |
| `main` | 6049 | 6049 |
| `public_keys` | 1 | 1 |
| `schemas` | 12 | 12 |

## Notes

- restored logical snapshot into EncryptingNamespacedStore → LastStore
- reopen after restore+flush verified sample keys

## Criteria

- Open CoW sled home (inventory) without touching primary.
- Logical export decrypts readable rows via identity.key.
- Restore into Last Store under the encrypting seam.
- Sampled key/value parity vs snapshot (deterministic samples).
- Schema catalog entry count match.
- Compact + reopen still serves samples.

## Primary status

Primary remains sled. This report is CoW/offline only.

