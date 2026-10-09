# field_tip_headers tip-residue compact (CoW first)

| Field | Value |
|-------|--------|
| **Status** | Operator procedure — offline maintain, never first-pass on primary |
| **Design** | `docs/lastdb-ideal-storage-shape.md` PR-5 / tip-residue exit |
| **CLI** | `lastdb_local_maintain drain-tip-residue --collection headers` |
| **Code** | `LastStoreNamespacedStore::drain_tip_residue_collection` + `drop_empty_collection` |
| **Unit proof** | `drain_tip_residue_copies_deletes_and_drops_empty_collection` in `fold_db/crates/core/src/storage/laststore/legacy_collection_fanout_cost_tests.rs` |

## Why this exists

`field_tip_headers` is the dual-read fallback collection for `mh:` tip headers.
Canonical writes already land in `tips`. After dual-read
`legacy_hits` for headers stay at zero under real load, the residue directory is
dead mass: copy-verify any remaining keys into `tips`, delete legacy rows, then
compact + drop the empty collection.

Primary health (read-only check, do not open exclusive):

```bash
lastdb status
# Look for:
#   tip-residue: … [field_tip_headers]
#   Dual-read: … legacy_hits=… (… headers=0 …)
```

Zero `headers=` legacy hits is the green light for a CoW drain. Non-zero hits
mean dual-read still serves residue — drain only after soak + product OK.

## Invariants

1. **Never** use live `~/.lastdb` as the first proof surface. Always APFS CoW
   (or another offline copy) first.
2. Default CLI mode is **dry-run**. Pass `--execute` only after the dry-run
   report looks right.
3. Primary exclusive open requires `--i-know-this-is-primary` (still Tom-gated;
   not part of this card's proof).
4. Aside reclaim of *other* dumps (e.g. `sync_outbox.aside-*`) is a separate
   Tom-gated procedure — see `aside-reclaim-procedure.md`. Dropping an empty
   `field_tip_headers` collection after drain is not an aside dump delete.
5. Do **not** restart primary `lastdbd` for this work.

## Operator checklist (CoW)

```bash
# 1) Binary (host-track / Mini current install)
MAINTAIN="${LASTDB_CURRENT:-$HOME/.lastdb/current}/lastdb_local_maintain"
test -x "$MAINTAIN"

# 2) APFS CoW of the real home (same volume as ~/.lastdb)
COW_HOME="${COW_HOME:-$HOME/.cache/lastdb-cow-field-tip-headers-residue}"
rm -rf "$COW_HOME"
mkdir -p "$COW_HOME"
# identity (if present) + data plane
for f in identity.key config.json; do
  [ -e "$HOME/.lastdb/$f" ] && cp -c -R "$HOME/.lastdb/$f" "$COW_HOME/$f" || true
done
cp -c -R "$HOME/.lastdb/data" "$COW_HOME/data"

# 3) Dry-run one page (default collection = headers)
"$MAINTAIN" --home "$COW_HOME" drain-tip-residue \
  --collection headers --json

# 4) Execute pages until done=true, then drop empty collection
#    Repeat with --after / --after-hex from the previous report when needed.
"$MAINTAIN" --home "$COW_HOME" drain-tip-residue \
  --collection headers --execute --drop-empty-collection --json --limit 5000

# 5) Confirm residue gone on the CoW tree
du -sh "$COW_HOME/data/data/field_tip_headers" 2>/dev/null || echo "field_tip_headers absent (expected after drop)"
# Optional: boot an ephemeral lastdbd against COW_HOME and re-check dual-read
# tip-residue headers path / metrics stay zero under a short smoke query load.
```

Report fields that matter:

| Field | Meaning |
|-------|---------|
| `keys_scanned` | Page size walked in residue |
| `copied_to_tips` | Residue-only keys copied into `tips` first |
| `tips_already_won` | Both present → delete residue, keep tips |
| `deleted_from_legacy` | Rows removed from `field_tip_headers` |
| `done` | Page was short ⇒ end of collection |
| `collection_dropped` | Empty collection compacted + removed |

## Crash order

Per key under `--execute`:

1. If tips lacks the id → put tips, then delete residue.
2. If tips already has the id → delete residue only (tips wins).
3. After the final short page with `--drop-empty-collection` → compact shards,
   re-check empty, remove the collection directory.

## VERIFY (this card)

- Unit: `cargo test -p fold_db --lib drain_tip_residue_copies_deletes_and_drops_empty_collection`
- CoW: dry-run + execute + `collection_dropped=true` (or already-empty drop).
- Primary untouched: no exclusive open of `~/.lastdb` without Tom clearance.
- Dual-read on primary already reporting `headers=0` is the soak precondition,
  not a substitute for CoW execute proof.

## Related

- `docs/security/tip-family-dual-read-sunset.md` — dual-read arm sunset evidence
  (`class=residue` probe + cold-drain bar)
- `fold_db/docs/aside-reclaim-procedure.md` — Tom-gated aside dump delete
- `fold_db/docs/ideal-storage-primary-gated-apply.md` — primary gated apply
- `docs/lastdb-ideal-storage-shape.md` — PR-5 tip residue metrics + compact gates
