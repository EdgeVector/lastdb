# Ideal storage: primary gated apply (dry run → safe-upgrade)

**Card:** `lastdb-ideal-storage-primary-gated-apply`  
**Milestone:** `milestone-lastdb-ideal-storage-disk-matches-map`  
**North Star:** `north-star-lastdb-ideal-storage-shape`

## Rule (won't-undo)

No agent, script, or tool copies the primary Mini home (`~/.lastdb`), and none
boots a node on a copy of it (Tom, 2026-10-12). A new Mini binary is proved on
synthetic data in a fresh empty home. A disk-map residue drain starts with its
dry run, which is the default. Order:

1. **Synthetic probe** of the candidate binary (`lastdb-safe-upgrade --probe-only`)  
2. **GREEN** dry-run of tip, protein, and index drains  
3. **Situation preflight** for `safe-upgrade` / restart (CAS grind, rekey, …)  
4. **`lastdb-safe-upgrade`** with the candidate binary (probe-only first)  
5. Live install only after probe GREEN  
6. **Post-rollout** `lastdb status --json` for plane map + dual-read  
7. Aside reclaim only with **Tom + backup receipt** (never auto)

Order-log mass stays **history-adjacent** — see
`fold_db/docs/order-log-history-adjacent.md`. This path never GC's order-log.

## No CoW proof harness

The harness `scripts/ideal-storage-plane-residue-cow-proof.sh` cloned the
primary home. It is deleted. A copy of the primary fills the disk and runs the
machine into swap. Run each maintain verb as a dry run first (see the table
below). A drain that needs the real primary data meets it at the Tom-gated live
step, not on a copy.

## Primary rollout

```bash
# Situation fence (must be green for live cutover)
situations preflight --action safe-upgrade --system lastdbd

# Probe candidate on synthetic data in a fresh empty home (never live-first)
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
Tom. Keep the green probe; complete live apply when the fence lifts.

## Post-rollout bar (terminal proof input)

On the primary after cutover (read-only):

```bash
lastdb status --json
# planes.residue_named / tip-residue bytes
# dual_read.legacy_hits_by_plane / legacy_hits_by_collection
# headers/versions tip residue legacy hits should be ~0 after full drain + settle samples
# field_tips/mk is already pruned from live dual-read after the 2026-07-31 zero-hit soak
```

Collapsed-plane legacy hits remaining non-zero after a full execute +
primary binary cutover are a **follow-up drain**, not a green light for aside
delete.

## Maintain verbs (offline)

| Verb | Target |
|------|--------|
| `drain-tip-residue --collection headers\|versions` | → `tips` |
| `drain-protein-residue` | tips → `proteins` |
| `drain-index-residue --source tips\|…` | → `indexes` (skips order-log) |

All default dry-run; primary path requires `--i-know-this-is-primary` and
starts with the dry run.

`drain-field-tips` / `--collection tips` is retired. `mk:` now reads the
canonical `tips` plane only; any old `field_tips` directory is cold historical
residue, not a live fallback that current tip lookup requires.
