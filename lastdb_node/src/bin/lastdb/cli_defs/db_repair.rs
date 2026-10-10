//! Clap subcommands for `lastdb db`: repair and legacy-drain verbs.

use super::*;

#[derive(Subcommand, Debug)]
pub(crate) enum DbRepairCommand {
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
}
