# Aside reclaim procedure (Tom-gated)

| Field | Value |
|-------|--------|
| **Status** | Procedure only — **never automatic** |
| **Design** | `docs/lastdb-ideal-storage-shape.md` PR-2 / K13 |
| **Code** | `classify_collection_plane` → `Aside`; `lastdb status` surfaces mass |

## What counts as aside

LastStore collection directory names containing `.aside` under the store data
root, historically:

```text
~/.lastdb/data/data/sync_outbox.aside-legacy-20260723T224347Z/
```

These are **quarantined dumps**, not product SOT and not cold-sync still in the
live write path. Status attributes them under plane `aside`.

## Invariants

1. **No agent or maintain job deletes aside automatically.** Classification and
   status are read-only attribution.
2. Reclaim is **Tom-gated one-shot** after a backup receipt exists for the home.
3. Prefer CoW / offline copy for any destructive dry-run; never use live
   `~/.lastdb` as the first proof surface for a delete script.
4. Phase-1 cold plane stays under the **same** LastStore data root (K13). Aside
   is directory-level quarantine, not a second LastStore root.

## Operator checklist (manual)

1. Confirm backup durability is healthy (`lastdb status` → Backup durability /
   last_backup_commit age) or hold an explicit off-machine copy receipt.
2. Confirm Sync posture (`Sync: disabled` is common on primary) — aside outbox
   is not required for local mutation ack.
3. Measure: `du -sh ~/.lastdb/data/data/*.aside*`
4. Optional: rename aside dir out of the data tree first (still Tom-gated) so a
   mistaken process cannot reopen it as a collection.
5. Only after Tom explicit OK: `rm -rf` the aside path (or move to trash).
6. Re-run `lastdb status` — plane `aside` line should disappear.

## Not in scope here

- Auto-rename of live `sync_*` → `cold_*` collections
- GC of order log / tip residue compact (separate PRs)
- Primary restart as part of reclaim

## Related: tip-residue compact (not aside)

Empty `field_tip_headers` / `field_tip_versions` after copy-verify drain use
`lastdb_local_maintain drain-tip-residue` (CoW first). See
`field-tip-headers-residue-compact.md`. That path drops an empty collection
after drain; it is not the same as deleting a `.aside-*` quarantine dump.
