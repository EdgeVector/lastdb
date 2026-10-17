# Local native storage completion, October 16, 2026

CAUTION: The product checks below await the benchmark quiet window.
No completion result appears in this file until the actual command receipts exist.

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

## Results

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
