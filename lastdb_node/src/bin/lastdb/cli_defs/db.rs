//! Clap subcommands for `lastdb db`: storage inspection, repair and maintenance verbs.

use super::*;

// lint:file-size-ok one clap enum (a single type cannot span files); moved verbatim from cli_defs.rs

#[derive(Subcommand, Debug)]
pub(crate) enum DbCommand {
    /// Decrypt/read live store: main key-class + per-schema atom/history sizes.
    ///
    /// **Heavy op:** full-prefix walks over multi-GiB stores can take minutes
    /// and load many cold shards (see `lastdb ops`). Prefer `--out PATH` over
    /// shell redirect (`> file`) so a client deadline cannot leave a 0-byte
    /// product file that looks like an empty store.
    ///
    /// **Not read-only:** this command also durably writes attribution rows
    /// (schema/system/retention root walks) before it reports the summary.
    /// The writes are idempotent, so repeat runs are safe.
    ///
    /// Client deadline defaults to the admin-scan budget (600s, or
    /// `LASTDB_UDS_ADMIN_TIMEOUT_SECS`). Raise with `--timeout <secs>` on this
    /// command **and** the matching server env when walks exceed the default.
    Inventory {
        /// Emit raw JSON only (no human table).
        #[arg(long)]
        json: bool,
        /// Write inventory JSON to this path (temp file + rename; never a
        /// half-empty product file on failure). Prefer this over shell
        /// redirect so a timed-out run does not leave a 0-byte inventory.
        #[arg(long)]
        out: Option<PathBuf>,
        /// Client socket deadline for this inventory walk, in seconds.
        /// Defaults to the admin-scan budget (`LASTDB_UDS_ADMIN_TIMEOUT_SECS`
        /// or 600). The server deadline must be at least this high for the
        /// walk to finish — raise that env on the daemon too when you raise
        /// the client flag past the default.
        #[arg(long, value_name = "SECS")]
        timeout: Option<u64>,
    },
    /// Per-schema logical atom sizes (catalog names joined).
    ///
    /// **Heavy op:** walks live `atom:` rows only — cheaper than
    /// `db inventory` (no history / order-log / tip-format), but still an
    /// admin scan. Prefer `--out PATH` over shell redirect.
    ///
    /// Do not run this as a routine against a daily-driver primary.
    Schemas {
        /// Emit raw JSON only (no human table).
        #[arg(long)]
        json: bool,
        /// Write the report JSON to this path (temp file + rename).
        #[arg(long)]
        out: Option<PathBuf>,
        /// Client socket deadline in seconds. Defaults to the admin-scan
        /// budget (`LASTDB_UDS_ADMIN_TIMEOUT_SECS` or 600).
        #[arg(long, value_name = "SECS")]
        timeout: Option<u64>,
        /// Cap the human table (JSON stays complete). Default 50.
        #[arg(long, default_value_t = 50)]
        limit: usize,
    },
    /// Repair one installed schema's field-to-molecule metadata.
    ///
    /// The command changes no atom data. It performs a dry run unless
    /// `--execute` is present. Execute also requires the fingerprint returned
    /// by a prior dry run.
    ///
    /// Omit `--map-file` to inspect: the command then prints the installed
    /// map, shows which field molecules still hold live rows, and lists
    /// candidate maps from other installed schemas. Use that report to write
    /// the map file, or pass `--write-map-file` to save the suggested map.
    RepairSchemaMoleculeMap {
        /// Installed schema name or identity hash.
        #[arg(long)]
        schema: String,
        /// JSON object that maps every field name to one molecule UUID.
        #[arg(long)]
        map_file: Option<PathBuf>,
        /// Write the suggested map from an inspect run to this path.
        #[arg(long, conflicts_with = "map_file")]
        write_map_file: Option<PathBuf>,
        /// Apply the metadata change. Omit for a dry run.
        #[arg(long)]
        execute: bool,
        /// Current fingerprint from the reviewed dry run.
        #[arg(long, requires = "execute")]
        expected_current_fingerprint: Option<String>,
        /// Emit raw JSON only.
        #[arg(long)]
        json: bool,
    },
    /// Repair sparse declared key-field molecules in one HashRange partition.
    ///
    /// The declared range-field molecule supplies the member list. The command
    /// is a dry run unless `--execute` is present.
    RepairHashrangeKeyFields {
        /// Installed schema name or identity hash.
        #[arg(long)]
        schema: String,
        /// Exact API-form hash partition. This command never crosses it.
        #[arg(long)]
        hash: String,
        /// Apply the planned normal mutations. Omit for a dry run.
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
    /// Point-read one exact tip of a dropped receipt molecule.
    /// Returns a key fingerprint and presence booleans, without subject data.
    ProbeDroppedTip {
        #[arg(long)]
        schema: String,
        #[arg(long)]
        molecule: String,
        #[arg(long)]
        key_hash: String,
        #[arg(long, default_value = "")]
        key_range: String,
        /// Select one storage form by the prior probe fingerprint.
        #[arg(long)]
        expected_key_fingerprint: Option<String>,
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
    /// Remove live `mk:` tips whose atom body is unreachable by every reader route.
    ///
    /// Default is dry-run. Pass `--execute` to rewrite affected molecules.
    RepairDanglingTips {
        #[arg(long)]
        execute: bool,
        /// Bound how many tips this invocation scans.
        #[arg(long)]
        max_ops: Option<usize>,
        /// Raw `mk:` rows per range page.
        #[arg(long)]
        tip_page: Option<usize>,
        /// Include up to N unresolved detail rows in the report (default:
        /// none, or 1000 with `--schema`).
        #[arg(long)]
        audit_limit: Option<usize>,
        /// Walk only this schema's tips (catalog name, descriptive name such
        /// as `BoardCards`, or identity hash). One prefix range per field
        /// molecule, so the cost is the schema's rows, not the whole store.
        #[arg(long)]
        schema: Option<String>,
        /// With `--schema`: walk only this HashKey (partition), e.g. a board.
        #[arg(long, requires = "schema")]
        hash_key: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Show the bounded identities behind skipped atom reads on this daemon.
    /// Owner socket only. The result can contain user keys.
    UnresolvedAtoms {
        #[arg(long)]
        json: bool,
    },
    /// Drain legacy live tip-version (`tv:`) chains without collection compact.
    ///
    /// Bounded reclaim for history left after tip history became write-opt-in.
    /// Default is dry-run. Pass `--execute` to clear chains. Prefer
    /// `--from-checkpoint` for the resumable automatic-drain path.
    DrainTipHistory {
        #[arg(long)]
        execute: bool,
        /// Max `mk:` tips examined this pass (default 256).
        #[arg(long)]
        max_keys: Option<usize>,
        /// Optional cap on tips pruned this pass (defaults to max_keys).
        /// `--max-ops` is accepted as the same budget, the write-budget name
        /// the other cleanup verbs (reap-dropped-schema, repair) use.
        #[arg(long, visible_alias = "max-ops")]
        max_prunes: Option<usize>,
        /// Exclusive resume cursor: full storage key of the last tip walked.
        #[arg(long)]
        after_key: Option<String>,
        /// Resume from and advance the durable automatic-drain checkpoint.
        #[arg(long)]
        from_checkpoint: bool,
        #[arg(long)]
        json: bool,
    },
    /// Drop superseded `tv:` / tip-chain versions older than 7 days on **live**
    /// records. Tombstoned (deleted) heads are left alone. Default is dry-run.
    /// The CLI follows bounded daemon cursors to the end. Execute skips while
    /// a backup cut is held.
    RetainSupersededVersions {
        #[arg(long)]
        execute: bool,
        /// Max `mk:` tips examined per daemon pass (default 256).
        #[arg(long)]
        max_keys: Option<usize>,
        /// Optional cap on tips truncated per pass (defaults to max_keys).
        /// `--max-ops` is accepted as the same budget, the write-budget name
        /// the other cleanup verbs (reap-dropped-schema, repair) use.
        #[arg(long, visible_alias = "max-ops")]
        max_prunes: Option<usize>,
        /// Exclusive resume cursor: full storage key of the last tip walked.
        #[arg(long)]
        after_key: Option<String>,
        /// Resume from and advance the durable retention checkpoint.
        #[arg(long)]
        from_checkpoint: bool,
        /// Retention window in seconds. Default 7 days. `0` uses the default.
        #[arg(long)]
        retention_seconds: Option<u64>,
        #[arg(long)]
        json: bool,
    },
    /// Remove versions of a live record under the 7-day rule while the daemon is stopped.
    ///
    /// Rewrites tip files directly. Does not admit records into the warm set.
    /// Refuses when `folddb.sock` accepts a connection. Default counts only.
    /// `--execute` rewrites. Stop the daemon before this command. Start the
    /// same daemon after it. A tombstoned head is left alone. A version with
    /// no live head is left alone.
    RetainSupersededVersionsOffline {
        #[arg(long)]
        execute: bool,
        /// `written_at` cutoff in nanoseconds. Default is now minus 7 days.
        #[arg(long)]
        version_cutoff_nanos: Option<u64>,
        #[arg(long)]
        json: bool,
    },
    /// Sample the locator-only tip population (always read-only).
    ///
    /// Tips whose body is reachable only through the `aloc:` locator — not via
    /// the tip-derived partition prefix or flat key. Default sample is 4096 tips
    /// so this is safe on a live primary; raise `--max-tips` for a fuller walk.
    /// Result is cached for `lastdb status` / `/api/status`.
    ///
    /// The budget is spread across 17 windows partitioning the `mk:` keyspace,
    /// not spent on the first `--max-tips` keys: this class clusters by molecule
    /// id, so a consecutive read of the head reports 0‰ on a store whose true
    /// rate is 250‰.
    ProbeLocatorOnly {
        /// Cap how many tips this call classifies, across all windows
        /// (default 4096).
        #[arg(long)]
        max_tips: Option<usize>,
        /// Raw `mk:` rows per range page.
        #[arg(long)]
        tip_page: Option<usize>,
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
    /// Move `photos/Photo.file_bytes` (inline base64) into local CAS blobs.
    ///
    /// Default is dry-run. Pass `--execute` to write CAS + clear the field.
    /// Follow with `gc-atoms --execute` to drop tombstoned tip history + orphan atoms.
    MigratePhotoBlobs {
        #[arg(long)]
        execute: bool,
        #[arg(long)]
        json: bool,
    },
    /// Rewrite legacy fat `mk:` tip values to thin (atom_uuid + written_at + device_id).
    ///
    /// Same keys, thinner payloads. Default is dry-run. Pass `--execute` to rewrite.
    /// Safe under dual-read: readers accept both formats; local writes already emit thin.
    MigrateThinTips {
        #[arg(long)]
        execute: bool,
        #[arg(long)]
        json: bool,
    },
    /// Rewrite sealed values to an explicit binary `ENB:` target.
    ///
    /// One allowlisted plane per call: atoms, tips, indexes, metadata,
    /// change_feed, atom_locators, field_update_order_log. Default is dry-run.
    /// Pass `--execute` to rewrite in place under the same key (no dual-key
    /// copies). Bounded (`--max-rows` / `--max-secs`) and resumable; a killed
    /// run continues from the durable checkpoint. `--progress` reports the
    /// checkpoint without scanning. `--target binary` forbids compression;
    /// `--target binary-compress` permits deflate at 256 B. When omitted, the
    /// server keeps the legacy binary-compress behavior. The target does not consult the
    /// process write switches, so the verb is provable with those switches off.
    ///
    /// **Heavy op.** Prefer `--out PATH` over shell redirect. This is not a
    /// cheap status gauge. Do not run as a routine against the live primary;
    /// owner-gated there. Sequence one plane at a time, smallest first, with a
    /// backup cut published between planes and never while a cut is staged.
    ///
    /// `cas_blobs` is not on the allowlist (exempt).
    ResealAtRest {
        /// One allowlisted collection (required).
        #[arg(long)]
        collection: String,
        /// Target envelope policy. Omit to preserve legacy behavior.
        #[arg(long, value_enum, value_name = "TARGET")]
        target: Option<ResealAtRestTargetArg>,
        /// Rewrite rows. Without this flag the call is a dry-run plan.
        #[arg(long)]
        execute: bool,
        /// Cap on rows decided this invocation.
        #[arg(long)]
        max_rows: Option<usize>,
        /// Cap on wall time this invocation, in seconds.
        #[arg(long)]
        max_secs: Option<u64>,
        /// Print the durable checkpoint and exit. No scan, no writes.
        #[arg(long)]
        progress: bool,
        /// Drop the durable checkpoint and start at the first key.
        #[arg(long)]
        restart: bool,
        /// Emit raw JSON only.
        #[arg(long)]
        json: bool,
        /// Write the report JSON to this path (temp file + rename).
        #[arg(long)]
        out: Option<PathBuf>,
        /// Client socket deadline in seconds. Defaults to the admin-scan budget.
        #[arg(long, value_name = "SECS")]
        timeout: Option<u64>,
    },
    /// Remove un-enveloped rows from one encrypted plane and return their bytes.
    ///
    /// A row with no `ENC:`/`ENZ:`/`ENB:` envelope in a namespace LastDB
    /// encrypts already reads as absent (drop-dual-read, 2026-09-14); reads
    /// never delete it, so it stays on disk and rides into every backup and
    /// restore. This verb is the bounded reaper. One sealed plane per call:
    /// atoms, tips, indexes, metadata, change_feed, atom_locators,
    /// field_update_order_log. Default is dry-run. Pass `--execute` to delete.
    /// Bounded (`--max-rows` / `--max-secs`) and resumable from a durable
    /// checkpoint; `--progress` reports it without scanning.
    ///
    /// A sealed row is never touched, even one that does not open under this
    /// node's key. Every plaintext-by-policy namespace (schemas, db_catalog,
    /// molecule_keys, …) and the reserved marker namespace are refused by name.
    ///
    /// **Heavy op, on request only.** On a healthy home the correct count is
    /// zero; run the dry-run first and read a non-zero count as a signal (a
    /// restore that replayed unsealed) before you execute. Do not schedule.
    ReapUnsealed {
        /// One allowlisted, encrypted collection (required).
        #[arg(long)]
        collection: String,
        /// Delete rows. Without this flag the call is a dry-run plan.
        #[arg(long)]
        execute: bool,
        /// Cap on rows decided this invocation.
        #[arg(long)]
        max_rows: Option<usize>,
        /// Cap on wall time this invocation, in seconds.
        #[arg(long)]
        max_secs: Option<u64>,
        /// Print the durable checkpoint and exit. No scan, no writes.
        #[arg(long)]
        progress: bool,
        /// Drop the durable checkpoint and start at the first key.
        #[arg(long)]
        restart: bool,
        /// Emit raw JSON only.
        #[arg(long)]
        json: bool,
        /// Write the report JSON to this path (temp file + rename).
        #[arg(long)]
        out: Option<PathBuf>,
        /// Client socket deadline in seconds. Defaults to the admin-scan budget.
        #[arg(long, value_name = "SECS")]
        timeout: Option<u64>,
    },
    /// Dual-write tip-referenced atom bodies to partition-prefixed keys (+ locators).
    ///
    /// Default is dry-run. Pass `--execute` to write. Optional `--remove-flat`
    /// deletes the flat key only after the prefixed body is verified.
    ///
    /// `--audit` classifies every tip that resolves to no atom body — the
    /// `missing_body` count on its own cannot say whether a body still exists at
    /// an address the pass did not try, which is exactly what `--remove-flat`
    /// needs to know before it deletes flat keys.
    RekeyAtomPartitionPrefix {
        #[arg(long)]
        execute: bool,
        #[arg(long)]
        remove_flat: bool,
        #[arg(long)]
        max_ops: Option<usize>,
        /// Tips per range page (default 1024).
        ///
        /// Since the pass batches per page, a page costs a fixed number of store
        /// round trips *at any page size* — so this is the knob that trades
        /// resident memory against wall clock, and it is the only one. A larger
        /// page amortizes those round trips over more tips; it also makes the
        /// page's body read (`get_many` over the flat bodies needing a copy)
        /// materialize proportionally more atom content at once, and bodies run
        /// to `LASTDB_MAX_ATOM_CONTENT_BYTES`. Raise it to go faster on a node
        /// with headroom; lower it on one whose resident budget is the binding
        /// constraint. Measure both before changing it on a long run.
        #[arg(long)]
        tip_page: Option<usize>,
        /// Classify unresolved tips (locator-based) and print per-tip detail.
        /// Read-only: adds point reads, never writes.
        #[arg(long)]
        audit: bool,
        /// Cap on printed detail rows. Classification counters always cover
        /// every unresolved tip. Only meaningful with `--audit`.
        #[arg(long, default_value_t = 2000)]
        audit_limit: usize,
        /// Report the durable checkpoint and exit. No scan, no writes — the only
        /// mode that is safe to point at a live primary to ask "how far along?".
        #[arg(long)]
        progress: bool,
        /// Keep issuing `--execute` passes until the checkpoint reports the
        /// migration complete, treating a lost socket as retryable.
        ///
        /// A supervised restart mid-migration is an expected event, not a
        /// terminal error: a shell loop that aborted after five consecutive
        /// socket failures is what silently parked this migration at 8.8% on
        /// 2026-07-29. Requires `--execute`.
        #[arg(long)]
        until_complete: bool,
        /// Compact the `atoms` plane after a complete flat-key retirement pass.
        ///
        /// LastStore deletes append tombstones, so `--remove-flat` alone can
        /// grow the plane. This flag keeps logical retirement and physical byte
        /// return in one operator command. Requires `--execute --remove-flat
        /// --until-complete` and cannot be combined with `--json`.
        #[arg(
            long,
            requires_all = ["execute", "remove_flat", "until_complete"],
            conflicts_with = "json"
        )]
        compact_after: bool,
        #[arg(long)]
        json: bool,
    },
    /// Count `mk:` records whose `meta.tombstoned` flag disagrees with their
    /// atom content, and optionally stamp the flag onto them.
    ///
    /// A record written before the flag existed reads as `tombstoned: false`
    /// while its atom content is a tombstone. The page window is spent on such a
    /// row and the content gate rejects it afterwards, so a page comes back
    /// short. `content_tombstoned_meta_false` is how many such rows remain.
    ///
    /// Reads every atom body under the selected molecules — narrow it with
    /// `--schema`. Default is dry-run; `--execute` stamps the flag (idempotent,
    /// so an interrupted run resumes by re-running).
    ///
    /// `--max-keys` bounds one daemon POST, not the whole walk. The CLI follows
    /// `more_remaining` until the store is exhausted unless `--once` is set.
    TombstoneFlagAudit {
        /// Audit only this schema's field molecules (default: whole store).
        #[arg(long)]
        schema: Option<String>,
        /// Stamp `meta.tombstoned` on records whose content says tombstone.
        #[arg(long)]
        execute: bool,
        /// Records one daemon call decides before returning a resume cursor.
        /// This is a per-call cap, not a total. Lower it if a pass approaches
        /// the control socket's read deadline.
        #[arg(long)]
        max_keys: Option<usize>,
        /// Resume after a prior page's `next_after_key`.
        #[arg(long = "after-key", alias = "after")]
        after_key: Option<String>,
        /// Issue one daemon POST, print that page (including `more_remaining`
        /// and `next_after_key`), then exit. Without this flag the CLI follows
        /// cursors until the store is exhausted. During a multi-pass `--json`
        /// walk, per-pass progress is written to stderr while stdout remains a
        /// single final JSON document.
        #[arg(long)]
        once: bool,
        /// Emit raw JSON only.
        #[arg(long)]
        json: bool,
    },
    /// Dry-run or execute a bounded, resumable hard drain of legacy
    /// tombstone-content tips. The CLI follows daemon cursors until complete.
    DrainLegacyTombstones {
        /// Drain only this schema's field molecules (default: whole store).
        #[arg(long)]
        schema: Option<String>,
        /// Hard-erase matching slots and queue Search tombstones.
        #[arg(long)]
        execute: bool,
        /// Records one daemon call decides before returning a resume cursor.
        #[arg(long)]
        max_keys: Option<usize>,
        /// Emit raw aggregate JSON only.
        #[arg(long)]
        json: bool,
    },
    /// Survey legacy/plain HashKey-encoding tips and classify safe forks.
    LegacyKeyForkAudit {
        #[arg(long)]
        max_keys: Option<usize>,
        #[arg(long)]
        json: bool,
    },
    /// Drain forked legacy/plain tips whose current twin resolves to a live atom.
    /// Dry-run by default; a legacy-only key is never deleted.
    DrainLegacyKeyForks {
        #[arg(long)]
        execute: bool,
        #[arg(long)]
        max_keys: Option<usize>,
        #[arg(long)]
        json: bool,
    },
    /// Detect molecules whose `update_order` append-log is shorter than their
    /// live key set — a truncated log that no point read can see.
    ///
    /// `update_order` only grows: a write appends an entry every time a key's
    /// value changes and never removes one, so `moc:{M} >= count(mk:{M}:…)`
    /// holds for every molecule that has a log. Below that bound, entries were
    /// lost. Because a truncation restamps `moc:` and deletes the `mord:` rows
    /// above it together, the log stays self-consistent and only `SampleN`
    /// (the sole reader that walks the log) ever sees the hole — the `mk:`
    /// count is the only witness left.
    ///
    /// Read-only, and cheap: the molecule uuid comes out of the key, so no atom
    /// body is ever fetched. Exits non-zero when a short molecule is found.
    OrderLogAudit {
        /// `mk:` records one daemon call walks before returning a resume
        /// cursor. Soft — a call always finishes the molecule it is inside.
        #[arg(long)]
        max_keys: Option<usize>,
        /// Emit raw JSON only.
        #[arg(long)]
        json: bool,
    },
    /// Bounded, resumable, read-only pin-log plane audit (`sync_pin_log`).
    ///
    /// Classifies entry rows as confirmed-orphan (frontier ≤ durable published
    /// F for their writer) vs genuinely pending. Never deletes. Returns at
    /// most one bounded page and an explicit resume cursor when more remains.
    PinLogAudit {
        /// Pin-log keys this bounded audit page may walk.
        #[arg(long)]
        max_keys: Option<usize>,
        /// Resume after a prior page's `next_after_key`.
        #[arg(long)]
        after_key: Option<String>,
        /// Emit the page report as raw JSON only.
        #[arg(long)]
        json: bool,
    },
    /// Measure append-only order-log excess (stale entries + zero-live residue)
    /// with exact stored bytes. Read-only; follows bounded daemon cursors.
    OrderLogBloatAudit {
        /// `mk:` records one daemon call walks before returning a resume cursor.
        #[arg(long)]
        max_keys: Option<usize>,
        /// Emit raw aggregate JSON only.
        #[arg(long)]
        json: bool,
    },
    /// Delete the order log of a zero-live molecule, a bloated molecule, and
    /// a clean molecule. Does not write a new log. Dry-run writes nothing.
    /// `retention_seconds` does not keep rows. The CLI follows bounded daemon
    /// cursors to the end. Execute skips while a backup cut is held.
    CompactOrderLog {
        /// Delete the selected logs. Without this flag the command writes nothing.
        #[arg(long)]
        execute: bool,
        /// `mk:` records one daemon call walks before returning a cursor.
        #[arg(long)]
        max_keys: Option<usize>,
        /// Echoed on the report. It does not keep rows.
        #[arg(long)]
        retention_seconds: Option<u64>,
        /// Emit raw aggregate JSON only.
        #[arg(long)]
        json: bool,
    },
    /// The verb writes nothing. The command id stays until a later removal.
    /// The CLI follows bounded daemon cursors to the end.
    RepairOrderLogShortfall {
        /// The verb writes nothing.
        #[arg(long)]
        execute: bool,
        /// `mk:` records one daemon call walks before returning a cursor.
        #[arg(long)]
        max_keys: Option<usize>,
        /// Emit raw aggregate JSON only.
        #[arg(long)]
        json: bool,
    },
    /// List one molecule's live `mk:` storage keys (decoded hash/range +
    /// collision flag). Read-only diagnostic for H1 vs H2 order-log shortfall.
    MoleculeKeys {
        /// Molecule uuid (the `M` in `mk:{M}:…`).
        #[arg(long)]
        molecule: String,
        /// Cap rows returned (default 10_000).
        #[arg(long)]
        max_keys: Option<usize>,
        /// Emit raw JSON only.
        #[arg(long)]
        json: bool,
    },
    /// Drain plane residue: move rows sitting outside their canonical plane
    /// collection (e.g. `conflict:` rows in legacy `sync_conflicts`) into the
    /// canonical home, page by page, on the LIVE node.
    ///
    /// Every page is copy-then-delete per key with target-wins semantics, so
    /// reads stay correct throughout and an interrupted run resumes with
    /// `--after` (or just re-runs). Default is a dry-run of one page; pass
    /// `--execute --until-complete` to drain the whole source collection.
    /// This is what turns dual-read legacy fall-throughs (measured 87%
    /// `sync_conflicts` on 2026-07-30) back into canonical-collection hits.
    DrainPlaneResidue {
        /// Plane family: tip | protein | index | conflict | order-log.
        #[arg(long)]
        family: String,
        /// Collection to scan (e.g. sync_conflicts, field_update_order_log).
        #[arg(long)]
        source: String,
        /// Canonical target collection. Defaults per family
        /// (tips / proteins / indexes).
        #[arg(long)]
        target: Option<String>,
        /// Apply the copy+delete. Without it, one dry-run page is reported.
        #[arg(long)]
        execute: bool,
        /// Resume cursor from a previous page's `after`.
        #[arg(long)]
        after: Option<String>,
        /// Rows one daemon call decides (default 1000).
        #[arg(long)]
        limit: Option<usize>,
        /// After the last page, drop the source collection if it is empty.
        #[arg(long)]
        drop_empty_source: bool,
        /// Keep issuing pages until the source is exhausted.
        #[arg(long)]
        until_complete: bool,
        /// Emit raw JSON only.
        #[arg(long)]
        json: bool,
    },
    /// Fetch exactly one `$lastdb_file` pointer's remote CAS object on demand.
    ///
    /// The pointer JSON must carry file blob access metadata. The daemon caches
    /// the verified bytes locally; this command writes the bytes to --out, or
    /// to stdout when --out is omitted. A blob stored with `put-blob-local` is
    /// read from the local plane; a node without cloud sync answers 404 when
    /// the blob is not stored on it.
    FetchFileBlob {
        /// JSON file containing the `$lastdb_file` field value.
        #[arg(long)]
        pointer_json: PathBuf,
        /// Write fetched bytes to this path instead of stdout.
        #[arg(long)]
        out: Option<PathBuf>,
        /// Emit the daemon's JSON report instead of raw bytes / human summary.
        #[arg(long)]
        json: bool,
        /// Request the verified plaintext as a binary socket response.
        #[arg(long)]
        raw: bool,
    },
    /// Upload one personal file blob through the app-facing DB route.
    ///
    /// The running daemon must be cloud-sync capable and connected. The command
    /// stores the returned `$lastdb_file` pointer in the requested schema field.
    PutFileBlob {
        /// Schema name to mutate, for example `files/File`.
        #[arg(long)]
        schema: String,
        /// Field that should receive the `$lastdb_file` pointer.
        #[arg(long)]
        field: String,
        /// Hash key for the target record.
        #[arg(long)]
        key_hash: String,
        /// Optional range key for the target record.
        #[arg(long)]
        key_range: Option<String>,
        /// Plaintext file bytes to upload.
        #[arg(long)]
        bytes: PathBuf,
        /// Mutation type to write: create, update, delete, or purge.
        #[arg(long, default_value = "update")]
        mutation_type: String,
        /// Optional display filename for the pointer.
        #[arg(long)]
        name: Option<String>,
        /// Optional media type for the pointer.
        #[arg(long)]
        media_type: Option<String>,
        /// Keep a local plaintext CAS copy after upload.
        #[arg(long)]
        cache_local_plaintext: bool,
        /// Optional JSON object merged into the mutation fields.
        #[arg(long)]
        additional_fields_json: Option<PathBuf>,
        /// Write the returned `$lastdb_file` pointer JSON to this path.
        #[arg(long)]
        pointer_out: Option<PathBuf>,
        /// Send raw bytes over the socket instead of base64 in a JSON body.
        #[arg(long)]
        raw: bool,
    },
    /// Store one blob in this node's own `cas_blobs` plane and print its pointer.
    ///
    /// Needs no cloud sync. Writes no record: put the printed `$lastdb_file`
    /// pointer, as the whole value of a field of type `Any`, into a record of
    /// your own. A pointer inside a JSON string is not a reference: the blob is
    /// reclaimed once its row is older than 600 s. Identical bytes give an
    /// identical pointer. Reads the bytes from --file, or from stdin when stdin
    /// is a pipe. The row is on disk before the pointer is printed. A blob is at
    /// most 16 MiB unless the daemon's owner sets
    /// `LASTDB_LOCAL_FILE_BLOB_MAX_BYTES`; the daemon answers 413 above it, so
    /// store larger files as slabs. Read the bytes back with `db fetch-file-blob`.
    PutBlobLocal(PutBlobLocalArgs),
    /// Fork a shared `$lastdb_file` pointer into this node's personal blob scope.
    ///
    /// The daemon fetches the source pointer bytes, uploads them with a fresh
    /// personal file-blob access record, and rewrites the selected local field.
    ForkFileBlob {
        /// Schema containing the local file field to rewrite.
        #[arg(long)]
        schema: String,
        /// Field to rewrite with the forked `$lastdb_file` pointer.
        #[arg(long)]
        field: String,
        /// Hash-key value for simple hash-key schemas.
        #[arg(long, conflicts_with = "key_json")]
        key: Option<String>,
        /// JSON file containing a KeyValue object, e.g. {"hash":"id","range":null}.
        #[arg(long)]
        key_json: Option<PathBuf>,
        /// JSON file containing the source `$lastdb_file` field value.
        #[arg(long)]
        pointer_json: PathBuf,
        /// Override the pointer's file name in the rewritten local field.
        #[arg(long)]
        name: Option<String>,
        /// Override the pointer's media type in the rewritten local field.
        #[arg(long)]
        media_type: Option<String>,
        /// Seed local CAS with plaintext after the fork write.
        #[arg(long)]
        cache_local_plaintext: bool,
        /// Emit raw JSON only.
        #[arg(long)]
        json: bool,
    },
    // NOTE: offline freelist compact was a sled-only tool and is gone with
    // the sled engine. Live `lastdb db *` verbs talk to the running daemon;
    // use Last Store tooling (`lastdb status`, restore, cloud heal) instead.
}
