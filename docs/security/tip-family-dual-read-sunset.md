# Tip-family dual-read sunset (`mh:` / `tv:` → tips only)

Date: 2026-08-07

Process: `brain get sop-migration-code-sunset --type sop`  
Card: `lastdb-retire-tip-family-dual-read-arms`  
North Star: `north-star-open-cutovers-drained`

## Scope

This note records the evidence used to **delete live dual-read fallback** for
tip-family keys on logical main:

| Prefix | Write target | Legacy collection (cold only) | Live dual-read |
|--------|--------------|-------------------------------|----------------|
| `mk:`  | `tips`       | `field_tips` (aside / absent) | deleted 2026-07-31 (fold #1215) |
| `mh:`  | `tips`       | `field_tip_headers`           | deleted 2026-08-05 (fold #1259) |
| `tv:`  | `tips`       | `field_tip_versions`          | deleted 2026-08-05 (fold #1259) |

After those PRs, `main_collections_for_key` for `mk:`/`mo:`/`mh:`/`tv:` is
**write-target `tips` only** (`PRUNED_ZERO_HIT_*`). Point gets and scans never
re-open `field_tip_headers` / `field_tip_versions`.

**Status (2026-08-07):** the node-local half of this sunset is complete; the
`class=format` half is not, and cannot be closed by measurement. When first
written (2026-08-06, fold #1370) this note claimed the surface was "retired
end-to-end"; that was premature — #1370 deleted nothing. See
`papercut-sunset-card-closed-on-a-docs-only-pr-with-a-pre-fix-probe-measure`.

## class=residue trigger (both clauses)

### 1) Probe zero across dwell + restart

`dual_read` tip-family arms are process-global counters (reset on restart).
They must stay at zero across at least one clean `lastdbd` restart **and** a
soak under real load.

**Primary measure (2026-08-07T01:17Z UTC)** after a clean restart at
`2026-08-06T17:13:08Z` (uptime ~8h; build `0.23.3-313-g9f8b20434`):

```
Dual-read: gets=3073240 target_hits=2090832
  legacy_hits=790172
  tip_residue mk/mh/tv=0 field_tips=0 headers=0 versions=0
  other=790172 served by: field_update_order_log / field_update_order_count
  planes: history_adjacent=790172
```

Tip-family residue arms are **all zero** across >3M gets in this uptime window.
The non-zero `legacy_hits` headline is **history-adjacent order-log fallthrough**
(by design until instrument PR #1363 is live on the primary binary); it is not
tip-family dual-read debt. See
`papercut-dual-read-legacy-hits-counts-by-design-planes`.

Earlier same-day baseline (SOP standing table): tip-family arms 0 across
2,332,968 gets with the post-#1363 counter semantics on fold main.

### 2) Cold collections drained or proven absent

Zero live reads is not zero data. On the same primary host at measure time:

```
tip-residue: 4.9 MiB  [field_tip_headers]
history-adjacent plane also lists: field_tip_versions=7.4 MiB
on-disk dirs present under ~/.lastdb/data/data/: field_tip_headers, field_tip_versions
```

**Live dual-read is already off.** Reclaim is explicit offline
`lastdb_local_maintain drain-tip-residue` (CoW first), never first-pass on
primary. Procedure: `fold_db/docs/field-tip-headers-residue-compact.md`.
Primary exclusive open still requires `--i-know-this-is-primary` (Tom-gated).

### CoW proof — RUN 2026-08-07

Harness: `scripts/ideal-storage-plane-residue-cow-proof.sh`, run id
`tipdrain-dryrun-20260807`, against an APFS clone of the real primary.
Proof JSON: `~/.lastdb-test-copies/proofs/plane-residue-cow-tipdrain-dryrun-20260807.json`
(`failed_gates: []`, `primary_mutated: false`).

| Surface | Evidence |
|---------|----------|
| CoW home | `~/.lastdb-test-copies/plane-residue-tipdrain-dryrun-20260807` |
| headers | dry-run `scanned=0` → execute `collection_dropped=true` |
| versions | dry-run `scanned=0` → execute `collection_dropped=true` |
| Reclaimed | 4.9 MiB + 7.4 MiB = **12.3 MiB**, collections absent after |
| Post-drop read | home re-opens; re-run `scanned=0 done=true` exit 0; `tips` intact at 4.8 GiB |
| Primary | **untouched** |

**The decisive fact: both collections held zero live rows.**
`drain_tip_residue_collection` enumerates with `list_prefix_paged(collection,
"", …)` — an *empty* prefix, so no filtering — and propagates open errors. A
`scanned=0` from it therefore means the collection is genuinely empty, not
unread. All 17 drains in the harness (tip, protein, index) reported
`scanned=0`.

So the 12.3 MiB was allocated extents behind previously-deleted records, not
data. The drain had nothing to copy, and deleting the read-side arms stranded
nothing. Zero live reads is not zero data — but here it was also zero data,
and that had to be measured rather than assumed.

## What was removed (code)

- Live candidate lists for `mh:` / `tv:` no longer include
  `field_tip_headers` / `field_tip_versions` (fold #1259).
- `mk:` / `mo:` similarly pruned earlier (fold #1215).
- Unit bar: `tip_residue_dual_read_delete_tests` in
  `fold_db/crates/core/src/storage/laststore/tip_residue_dual_read_delete_tests.rs`.

## Deleted 2026-08-07

- `mk_legacy_hits` / `mh_legacy_hits` / `tv_legacy_hits`,
  `field_tips_legacy_hits` / `field_tip_headers_legacy_hits` /
  `field_tip_versions_legacy_hits`, `other_legacy_hits`, and
  `tip_residue_legacy_hits()` — always-zero on every home, since the code no
  longer consults those collections anywhere. `legacy_hits_by_collection`
  answers the same question without a hand-maintained bucket per collection.
- The matching `DualReadHealth` gauges, `/api/status` wire fields, gauge
  contract rows, and gauge-gate entries.
- `tip_family_prefix()`, and with it the `key` parameter of
  `record_dual_read_get` — which let the logical-main `get` path stop cloning
  every key into a `Vec` purely to feed a counter.

## Remains — and is `class=format`, not `class=residue`

- `TIP_RESIDUE_LEGACY_COLLECTIONS` / `TIP_RESIDUE_KEY_PREFIXES` /
  `classify_tip_residue_copy` / `apply_tip_residue_copy_page` /
  `drain_tip_residue_collection` / `lastdb_local_maintain drain-tip-residue`
- `PRUNED_ZERO_HIT_LEGACY_COLLECTIONS` defensive list (must not re-open)

Mini ships publicly. A home written by an older Mini may still hold tip-family
rows, and that is **unobservable from here** — no counter on our primary can
ever authorize deleting the reclaim path for someone else's data. These carry
`class=format` markers keyed to the upgrade floor (`decisions-log`,
2026-08-06), not to a residue probe. Waiting for a number here would wait
forever.

## Not done: the primary cold drop

The primary's own `field_tip_headers` / `field_tip_versions` (12.3 MiB) are
still present. `drain-tip-residue` needs an exclusive offline open, so
dropping them requires stopping `lastdbd` — a restart, which is Tom's call or
a `lastdb-safe-upgrade` window, never an agent's. The CoW run above proves the
drop is safe and lossless when that window comes. Nothing depends on it: the
collections are unreachable by the product read path, so this is disk reclaim,
not correctness.

## If a future home still has cold tip residue

1. Do **not** re-enable dual-read.
2. Run CoW `drain-tip-residue --collection headers|versions` per
   `field-tip-headers-residue-compact.md`.
3. Only then drop empty collections on that home.
4. Primary apply: same CLI with Tom-gated `--i-know-this-is-primary`.

## CoW proof

**When:** 2026-08-07T01:50Z approx (pickup run `2026-08-07T01-11-26-579Z`)  
**Binary:** worktree-built `lastdb_local_maintain` (release) from fold @ card branch  
**Home:** APFS clonefile of `~/.lastdb` → `~/.cache/lastdb-cow-field-tip-headers-residue`  
(primary **not** opened exclusive)

Pre-drain CoW sizes:

| Collection | Size |
|------------|------|
| `field_tip_headers` | 4.9 MiB |
| `field_tip_versions` | 7.4 MiB |

Execute reports (`--execute --drop-empty-collection --json --limit 5000`):

**headers**

```json
{
  "legacy_collection": "field_tip_headers",
  "dry_run": false,
  "keys_scanned": 0,
  "copied_to_tips": 0,
  "tips_already_won": 0,
  "deleted_from_legacy": 0,
  "skipped": 0,
  "after": null,
  "done": true,
  "collection_dropped": true
}
```

**versions**

```json
{
  "legacy_collection": "field_tip_versions",
  "dry_run": false,
  "keys_scanned": 0,
  "copied_to_tips": 0,
  "tips_already_won": 0,
  "deleted_from_legacy": 0,
  "skipped": 0,
  "after": null,
  "done": true,
  "collection_dropped": true
}
```

Post-drain: both collection directories **absent** under the CoW
`data/data/` tree. Interpretation: cold mass was empty of logical keys
(allocated segment residue only); drain still required to drop the empty
collections before tip-residue code/counters can be deleted.

**Primary apply** remains Tom-gated (`--i-know-this-is-primary`). Follow-up card
owns primary exclusive drain + deletion of `TIP_RESIDUE_*` / always-zero tip
counters once primary collections are gone.
