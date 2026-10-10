//! Clap subcommands for `lastdb db`: space reclaim, GC and compaction verbs.

use super::*;

#[derive(Subcommand, Debug)]
pub(crate) enum DbReclaimCommand {
    /// Compact allowlisted LastStore collections (reclaim superseded segs).
    ///
    /// Omit `--collection` (or pass `--all`) to walk every allowlisted plane
    /// except `cas_blobs` in one pass. Cloud Sync is paused once if any plane
    /// needs isolation, then restored. Pass `--collection <name>` to compact
    /// a single plane; `cas_blobs` is reached only that way.
    ///
    /// Default is dry-run (report live keys + bytes). Pass `--execute` to
    /// rewrite live keys and drop dead segment history. Execute skips while a
    /// backup cut is held (same packing lock as automatic self-compaction).
    ///
    /// Execute on captured user-state planes (`atoms`, `schemas`,
    /// `schema_states`, `proteins`, `field_update_order_log`,
    /// `field_update_order_count`, `cas_blobs`) pauses Cloud Sync for the
    /// rewrite and restores it afterwards if it was on. `tips` and `metadata`
    /// compact are capture-neutral (physical shard rewrite + suppress) and do
    /// not pause.
    /// Capture-free planes also skip that.
    /// A crash mid-compact leaves sync paused — run `lastdb cloud on`.
    /// Dry-run does not touch cloud.
    ///
    /// Allowlist:
    /// `schemas`, `schema_states`, `schema_index`, `tips`, `metadata`,
    /// `idempotency`, `change_feed`, `sync_pin_log`, `sync_capture_reexport`,
    /// `atoms`, `atom_locators`, `atom_ref_edges`, `atom_ref_edges_v2`,
    /// `molecule_ref_edges`, `blob_ref_edges`, `proteins`,
    /// `field_update_order_log`, `field_update_order_count`, `indexes`,
    /// `keep_small`, `cas_blobs`.
    /// Atom execute records the retired chunk shas for the next signed
    /// manifest receipt.
    ///
    /// `sync_pin_log` is usually the largest plane in the store. Its rows are
    /// deleted after cloud confirms them, but a delete is an append, so only a
    /// compaction returns the bytes. The daemon self-compacts it on two
    /// triggers: enough confirmed truncations in this process
    /// (`LASTDB_PIN_LOG_COMPACT_AFTER_ROWS`), or the plane exceeding its
    /// on-disk cap (`LASTDB_PIN_LOG_COMPACT_MAX_BYTES`, default 16 MiB). The
    /// second is what bounds the plane — the row counter is process-local, so a
    /// restart zeroes it, and on 2026-08-17 that left 20.4 GiB behind one live
    /// record with compaction permitted and never attempted.
    ///
    /// `keep_small` holds the single storage-meters gauge row (`keep_small:meters`)
    /// that lived in `metadata` until 2026-09-21, when a per-write flush wrote
    /// 39 GB of identical snapshots into one hash group and the primary could
    /// not load it (papercut-lastdb-primary-transient-30gb-footprint-spike-guard-restart-loop-20260921).
    /// It is capture-free and rebuildable, so the residual sweep compacts it
    /// like `idempotency`. The legacy `metadata` group is reclaimed by
    /// `lastdb db reclaim-keep-small-legacy`, never by loading it.
    ///
    /// `sync_capture_reexport` is the same shape with a smaller cap. It holds one
    /// crash-safe intent marker per captured write in flight, so its live set is
    /// a handful of records; because a `LastStore` delete is an append, the plane
    /// still gains two records per write forever. It reached 3.14 GiB behind a
    /// live set of zero before self-compaction existed. The daemon compacts it on
    /// the sync cycle once it exceeds `LASTDB_CAPTURE_REEXPORT_COMPACT_MAX_BYTES`
    /// (default 16 MiB).
    ///
    /// `idempotency` and `change_feed` are also append/delete churn planes whose
    /// physical records are omitted from mutation-log capture. The daemon's
    /// residual sweep compacts them above
    /// `LASTDB_RESIDUAL_PLANE_COMPACT_MAX_BYTES` (default 64 MiB).
    ///
    /// `metadata` holds captured node state with no reconstruction contract.
    /// Its physical compact is capture-neutral through capture suppression;
    /// ordinary metadata writes remain captured.
    ///
    /// `atom_locators` is the churn plane with a real live set: one small
    /// `aloc:` row per live atom, rewritten in place whenever an atom is
    /// re-addressed, and a rewrite is an append. It reached 1.45 GiB on the
    /// primary 2026-08-17 while not even being on the allowlist, so this
    /// command answered "not on the compact allowlist" and no other verb
    /// returned a byte. The daemon now compacts it on the sync cycle once its
    /// expected reclaim (filesystem allocation overhang plus dead-record
    /// residue) reaches
    /// `LASTDB_ATOM_LOCATORS_COMPACT_MIN_OVERHANG_BPS` (default 1500 = 15%,
    /// probed hourly). Compacting it is capture-free because `aloc:` rows are
    /// omitted from the mutation log by key prefix, not by namespace.
    ///
    /// `atom_ref_edges` and `atom_ref_edges_v2` hold the rebuildable legacy and
    /// compact reverse-reference shadow indexes. The residual sweep compacts
    /// them above `LASTDB_RESIDUAL_PLANE_COMPACT_MAX_BYTES` (default 64 MiB).
    /// `molecule_ref_edges` and `blob_ref_edges` hold the rebuildable active
    /// molecule and local-blob liveness sets. The same capture-free residual
    /// sweep compacts them.
    ///
    /// `tips` is captured user state whose physical compact is capture-neutral
    /// (shard rewrite + suppress) and does not pause Cloud Sync. The daemon
    /// self-compacts it on expected reclaim — filesystem allocation overhang
    /// plus the store's dead-record residue (superseded and deleted records
    /// and their delete markers, the bytes a delete leaves behind): ratio
    /// `LASTDB_TIPS_COMPACT_MIN_OVERHANG_BPS` (default 1500 = 15%) AND floor
    /// `LASTDB_TIPS_COMPACT_MIN_OVERHANG_BYTES` (default 512 MiB), probed
    /// hourly, never while a backup cut is held. `LASTDB_TIPS_COMPACT_MAX_BYTES`
    /// (default 3 GiB) is a status-only alarm on `automatic_compactions`, not
    /// a trigger. Zero on either overhang knob disables unattended tips compact.
    ///
    /// `atoms` self-compacts on the same expected-reclaim trigger: ratio
    /// `LASTDB_ATOMS_COMPACT_MIN_OVERHANG_BPS` (default 1000 = 10%) AND floor
    /// `LASTDB_ATOMS_COMPACT_MIN_OVERHANG_BYTES` (default 512 MiB), probed
    /// hourly; `LASTDB_ATOMS_COMPACT_MAX_BYTES` (default 3 GiB) is a
    /// status-only alarm. The automatic path holds the photograph packing
    /// lock and runs the signed retirement-provenance rewrite under
    /// capture-suppress; it does not pause Cloud Sync. Owner `--execute`
    /// still pauses captured user-state planes including atoms. Zero
    /// disables the trigger.
    ///
    /// The live photograph keep-set is a copy of disk. Historical atom
    /// keep-set UUIDs enter `pending_shas` after packing-lock stamp-before-cut,
    /// or after this compact succeeds when `seed_committed_history` is on
    /// (`in_progress` stays local inventory). Owner `--execute` on atoms
    /// rewrites ~3.6 GiB and is **not** the one-shot; use
    /// `lastdb db stamp-purged-atom-retirements`.
    ///
    /// Before each ~6h photograph, a compact-if-dirty pass
    /// (`LASTDB_PHOTOGRAPH_COMPACT_INTERVAL_SECS`, default 21600) reuses the
    /// expected-reclaim trigger, bounded by
    /// `LASTDB_PHOTOGRAPH_COMPACT_BUDGET_SECS` (default 300). Planes that
    /// miss the budget wait for the next photograph cycle. The photograph
    /// proceeds when the budget ends. Continuous ~120s snapshot+log cuts do
    /// not wait on this pass.
    ///
    /// `proteins` is captured source-of-truth (no reconstruct contract).
    /// Owner `--execute` uses the captured-plane pause/restore route.
    ///
    /// `field_update_order_log` and `field_update_order_count` hold captured
    /// `mord:` / `moc:` rows. `lastdb db compact-order-log --execute` deletes
    /// those rows and does not write a new log. This command then returns the
    /// dead segment bytes on each collection. `--execute` pauses
    /// Cloud Sync if it was on. The daemon self-compacts
    /// `field_update_order_log` above
    /// `LASTDB_FIELD_UPDATE_ORDER_LOG_COMPACT_MAX_BYTES` (default 256 MiB,
    /// probed hourly) under the same live pause and photograph lock. Zero
    /// disables the trigger. `field_update_order_count` stays owner-compacted.
    ///
    /// `indexes` holds only capture-skipped dead residue prefixes (`mhr:` /
    /// `mhk:` / `mhi:` / `schema_atoms:` / `idx:` / `schemaidx:`). Compacting
    /// it does not pause Cloud Sync. The residual self-compact sweep also
    /// arms it. Compact only drops superseded segs — live residue keys stay
    /// until a separate delete/drain.
    ///
    /// `cas_blobs` holds the local file-blob rows (sealed under the file key).
    /// A blob delete is an append, so only this verb returns the bytes. It is
    /// owner-compacted only; the daemon never self-compacts it. A run reads
    /// every group and rewrites only the groups that hold dead bytes, with
    /// about three copies of one group in memory (a group can reach tens of
    /// MB). Compact it alone, never under host memory pressure.
    ///
    /// Keep this list in step with `COMPACT_ALLOWLIST`; the
    /// `compact_help_lists_every_allowlisted_collection` test in
    /// `lastdb_node`'s lib pins it.
    Compact {
        /// One collection (e.g. `schemas`). Omit with `--all` (or omit both)
        /// to compact every allowlisted plane except `cas_blobs`.
        #[arg(long)]
        collection: Option<String>,
        /// Compact every allowlisted collection but `cas_blobs` in one pass.
        #[arg(long)]
        all: bool,
        /// Actually compact (without this flag, only reports sizes).
        #[arg(long)]
        execute: bool,
        /// Emit raw JSON only.
        #[arg(long)]
        json: bool,
    },
    /// Trim or purge mutation-history rows.
    ///
    /// Default is dry-run (preview). Pass `--execute` to actually delete.
    /// Current tips (`mk:`) and atoms are never deleted.
    ///
    /// - `--keep-last 1` (default): drop older events, keep newest 1 per field key
    /// - `--keep-last 0`: full purge — delete all history (latest-only = tip only)
    ClearHistory {
        /// Limit to one schema name (default: all schemas with history).
        #[arg(long)]
        schema: Option<String>,
        /// Keep the newest N history events per field key.
        /// `0` = purge all history (no version log; tip is the only version).
        #[arg(long, default_value_t = 1)]
        keep_last: usize,
        /// Actually delete (without this flag, only reports what would be removed).
        #[arg(long)]
        execute: bool,
        /// Emit raw JSON only.
        #[arg(long)]
        json: bool,
    },
    /// GC: prune tip-version history for **tombstoned** tips, then delete
    /// unreferenced `atom:` rows.
    ///
    /// Soft-delete used to leave `tv:` chains pinning every old body atom so
    /// plain orphan GC could not free them. This command now:
    /// 1. For each eligible `mk:` tip, drop the `tv:` chain and clear
    ///    `prev_tip_id`. Default: tombstoned tips only (live tips keep `as_of`
    ///    history). With `--prune-live-history`: every tip with a non-empty
    ///    chain (reclaim path for append-heavy live records; drops `as_of`).
    /// 2. Delete `atom:` rows no remaining tip/tv/history still references.
    ///
    /// Default is dry-run. Pass `--execute` to apply.
    GcAtoms {
        /// Limit deletion candidates to one installed source schema.
        #[arg(long)]
        schema: Option<String>,
        #[arg(long)]
        execute: bool,
        /// Also prune tip-version history on **live** tips (not only
        /// tombstoned). Frees body atoms pinned by append-heavy rewrites;
        /// sacrifices `as_of` depth.
        #[arg(long)]
        prune_live_history: bool,
        #[arg(long)]
        json: bool,
    },
    /// GC: delete local file-blob rows no live atom references.
    ///
    /// The reclaim path for the sealed bytes a purge orphans: purge destroys
    /// the `$lastdb_file` pointer but blobs are shared across records, so
    /// only this reachability sweep may remove the rows (`cas_blobs` +
    /// resident `cas_blob:`). Undated rows are stamped on the first pass and
    /// become reclaimable on a later one. Cloud tiers (B2/R2) are untouched.
    ///
    /// Default is dry-run. Pass `--execute` to apply.
    GcFileBlobs {
        #[arg(long)]
        execute: bool,
        #[arg(long)]
        json: bool,
    },
    /// GC: delete empty, unbound `protein:` rows.
    ///
    /// Historical clients leaked one empty protein per failed bind probe.
    /// Bound proteins are protected by their member list and by `molprot:`
    /// back-refs. Default is dry-run. Pass `--execute` to apply.
    GcProteins {
        #[arg(long)]
        execute: bool,
        #[arg(long)]
        json: bool,
    },
    /// Measure (and optionally delete) legacy `ref:{molecule}` whole-molecule
    /// blobs — pre-per-key layout residue. The live read path never
    /// dual-reads these. Default is dry-run. Pass `--execute` to apply.
    PurgeRefBlobs {
        #[arg(long)]
        execute: bool,
        #[arg(long)]
        json: bool,
    },
    /// Drop the dead `metadata` hash group that held the keep-small snapshot
    /// before it moved to its own plane (2026-09-21: 39 GB of superseded
    /// copies of one key, loaded whole on the first write after boot, which
    /// looped the primary). Proves from the group's id sidecar that it holds
    /// only `keep_small:meters`; never loads it. Default is dry-run. Pass
    /// `--execute` to remove the directory.
    ReclaimKeepSmallLegacy {
        #[arg(long)]
        execute: bool,
        #[arg(long)]
        json: bool,
    },
    /// Drop the current `keep_small` group only when its sidecar proves it
    /// holds the one rebuildable `keep_small:meters` snapshot. This returns
    /// superseded snapshot bytes without loading the cold group. Default is a
    /// dry run. Pass `--execute` to remove the directory.
    ReclaimKeepSmallSnapshot {
        #[arg(long)]
        execute: bool,
        #[arg(long)]
        json: bool,
    },
    /// Stamp committed atom successor-history SHAs into pending purged
    /// retirements so the next photograph cut can drop them with a receipt.
    ///
    /// Default is dry-run (no sidecar write). `--execute` stamps groups that
    /// still have a verified local atom chunk. Missing groups are not stamped.
    /// Skips while a backup cut is held. Owner `compact --collection atoms
    /// --execute` rewrites ~3.6 GiB and is not this one-shot.
    StampPurgedAtomRetirements {
        /// Actually write pending_shas (without this flag, report only).
        #[arg(long)]
        execute: bool,
        /// Emit raw JSON only.
        #[arg(long)]
        json: bool,
    },
    /// Delete retired `schemaidx:` full-atom-copy secondary index keys.
    ///
    /// New writes no longer create schemaidx. This reclaims leftover bulk from
    /// older binaries (often ~equal to atom: size).
    PurgeSchemaidx {
        /// Emit raw JSON only.
        #[arg(long)]
        json: bool,
    },
    /// Reap tips of a dropped schema identity.
    ///
    /// Inspects bounded `schemaidx:` and `mk:` pages for one dropped identity.
    /// Never scans `atom:`. Execute is closed until the delete and meter debit
    /// use a replayable journal. Use `--cursor` to resume a dry run.
    ///
    /// `--field` computes `deterministic_molecule_uuid(schema, field)` and
    /// ranges those `mk:` prefixes. Use this when reverse edges name no
    /// molecules (LastGit after a pre-receipt catalog cut).
    ReapDroppedSchema {
        /// Canonical name or identity hash of the dropped schema.
        #[arg(long)]
        schema: String,
        /// Field name on the dropped schema. Repeatable. Computes the
        /// molecule UUID from `{schema}:{field}`.
        #[arg(long = "field")]
        fields: Vec<String>,
        #[arg(long)]
        execute: bool,
        /// Max work units this pass (1..=4096; tip proof needs at least 2).
        #[arg(long, default_value_t = 4096)]
        max_ops: u64,
        /// Opaque resume token from the previous page (invalid after restart).
        #[arg(long)]
        cursor: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Read the durable atom hard-delete audit trail (purge + gc-atoms), oldest first.
    ///
    /// Answers "was this atom body deleted on purpose, or lost?" from the store's
    /// own evidence rather than a ~15h log window. The negative answer is the
    /// strong one: no row covering the window means no delete path removed it.
    ///
    /// Rows carry counts, never atom content, atom uuids (which are content
    /// hashes), or the purged record key (a `key_fingerprint` stands in).
    DeleteLedger {
        /// Return at most N rows, oldest first. 0 (default) returns everything.
        #[arg(long, default_value_t = 0)]
        limit: u64,
        #[arg(long)]
        json: bool,
    },
}
