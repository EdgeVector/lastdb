# Order-log is history-adjacent (not rebuildable residue)

**Status:** won't-undo for `milestone-lastdb-ideal-storage-disk-matches-map`  
**Related:** `docs/lastdb-ideal-storage-shape.md`, plane role `HistoryAdjacent`

## What this is

Order-log mass is the append-only mutation / order chain used for history and
sync-adjacent reads. On disk it appears as:

| Collection | Role |
|---|---|
| `field_update_order_log` | history-adjacent |
| `field_update_order_count` | history-adjacent |
| `field_update_order_legacy` | history-adjacent |

Key prefixes (logical `main` dual-read):

- `mord:` → dual-reads `field_update_order_log` (write target `tips`)
- `moc:` → dual-reads `field_update_order_count` (write target `tips`)
- `mo:` → **tips only** after the 2026-07-31 zero-hit soak; `field_update_order_legacy`
  is **not** a live dual-read candidate (`PRUNED_ZERO_HIT_MAIN_PREFIXES`)

## What this is **not**

- **Not** rebuildable index residue (`mhr:` / `mhk:` / `mhi:` / `schemaidx:` …).
- **Not** tip residue dual-read (`mh:` / `tv:` still dual-read headers/versions;
  `mk:` is tips-only after the field_tips prune).
- **Not** protein SOT (`protein:` / `molprot:` / `fldprot:` / `pfq:`).

The index-plane drain (`lastdb_local_maintain drain-index-residue`) **must**
skip order-log prefixes. Tests assert `classify_index_residue_copy` returns
`SkipWrongFamily` for `mord:` / `moc:` / `mo:`.

## Operator view

`lastdb status` / `lastdb status --json` rolls these collections under
**history-adjacent**, not tip-residue or indexes. Terminal disk-map proof treats
remaining order-log bytes as **retained mass**, not a failed residue drain.

## Future compaction gate (not this milestone)

Any future GC / compaction of order-log requires an explicit product proof that:

1. every consumer of order-log history has a rebuild path **or** accepts loss,
2. dual-read legacy hits for those prefixes stay at zero on CoW samples, and
3. Tom (or a documented automatic policy) approves the aside/delete after backup
   receipt.

Until then: **retain**. No blind order-log GC from ideal-storage residue cards.
