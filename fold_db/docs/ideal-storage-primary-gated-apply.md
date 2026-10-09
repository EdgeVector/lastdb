# Ideal storage: primary gated apply (CoW → safe-upgrade)

**Card:** `lastdb-ideal-storage-primary-gated-apply`  
**Milestone:** `milestone-lastdb-ideal-storage-disk-matches-map`  
**North Star:** `north-star-lastdb-ideal-storage-shape`

## Rule (won't-undo)

The **primary** Mini home (`~/.lastdb`) is never the first surface that proves a
disk-map residue drain or a new Mini binary. Order:

1. **CoW proof** on an APFS clone (or full copy) of the real home  
2. **GREEN** dry-run / bounded execute of tip, protein, and index drains  
3. **Situation preflight** for `safe-upgrade` / restart (CAS grind, rekey, …)  
4. **`lastdb-safe-upgrade`** with the candidate binary (probe-only first)  
5. Live install only after probe GREEN  
6. **Post-rollout** `lastdb status --json` for plane map + dual-read  
7. Aside reclaim only with **Tom + backup receipt** (never auto)

Order-log mass stays **history-adjacent** — see
`fold_db/docs/order-log-history-adjacent.md`. This path never GC's order-log.

## CoW proof harness

```bash
# From a fold worktree on main (or a release candidate):
cargo build -p lastdb_node --bin lastdb_local_maintain --bin lastdb

MAINTAIN=$PWD/target/debug/lastdb_local_maintain \
LASTDB=$PWD/target/debug/lastdb \
LIMIT=200 \
  ./scripts/ideal-storage-plane-residue-cow-proof.sh

# Optional: mutate only the CoW home (still refuses primary)
EXECUTE_COW=1 MAINTAIN=… LASTDB=… \
  ./scripts/ideal-storage-plane-residue-cow-proof.sh
```

Proof JSON lands under `~/.lastdb-test-copies/proofs/plane-residue-cow-*.json`.

The script:

- refuses `~/.lastdb` / `~/.folddb` as the work home  
- clones primary → throwaway CoW  
- dry-runs tip / protein / index residue pages  
- in execute mode, may pass drop-empty flags only for CoW residue sources that
  have already drained; it never performs aside delete  
- does **not** restart or upgrade the primary

## Primary rollout

```bash
# Situation fence (must be green for live cutover)
situations preflight --action safe-upgrade --system lastdbd

# Probe candidate against CoW of real data (never live-first)
bash ~/.last-stack/skills/lastdb-safe-upgrade/scripts/safe-upgrade-lastdb.sh \
  --candidate /path/to/lastdbd-built-from-main \
  --probe-only

# Live only after GREEN + Situation allows + Tom/agent authorized
bash ~/.last-stack/skills/lastdb-safe-upgrade/scripts/safe-upgrade-lastdb.sh \
  --candidate /path/to/lastdbd-built-from-main \
  --yes
```

If Situations blocks `safe-upgrade` (e.g. cloud-sync CAS verify grind until
`backup_manifest_counter > 495`), **stop**. Do not `--force-situation` without
Tom. Keep the CoW proof; complete live apply when the fence lifts.

## Post-rollout bar (terminal proof input)

On the primary after cutover (read-only):

```bash
lastdb status --json
# planes.residue_named / tip-residue bytes
# dual_read.legacy_hits_by_plane / legacy_hits_by_collection
# headers/versions tip residue legacy hits should be ~0 after full drain + settle samples
# field_tips/mk is already pruned from live dual-read after the 2026-07-31 zero-hit soak
```

Collapsed-plane legacy hits remaining non-zero after a full CoW execute +
primary binary cutover are a **follow-up drain**, not a green light for aside
delete.

## Maintain verbs (offline / CoW)

| Verb | Target |
|------|--------|
| `drain-tip-residue --collection headers\|versions` | → `tips` |
| `drain-protein-residue` | tips → `proteins` |
| `drain-index-residue --source tips\|…` | → `indexes` (skips order-log) |

All default dry-run; primary path requires `--i-know-this-is-primary` and should
still prefer CoW first.

`drain-field-tips` / `--collection tips` is retired. `mk:` now reads the
canonical `tips` plane only; any old `field_tips` directory is cold historical
residue, not a live fallback that current tip lookup requires.
