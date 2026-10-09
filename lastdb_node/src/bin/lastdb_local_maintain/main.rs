//! Offline local-DB maintenance for Mini (Last Store only).
//!
//! Residue drains for ideal-storage cutover (`drain-tip-residue`,
//! `drain-protein-residue`, `drain-index-residue`). Historical sled-only
//! verbs (inventory/compact/export/repair-org-sync) were deleted with the
//! sled engine — use live `lastdb` status / cloud heal / restore instead.
// lint:file-size-ok moved verbatim from lastdb_local_maintain.rs; cohesive unit, split further in a later pass

use std::path::PathBuf;

use clap::{Parser, Subcommand, ValueEnum};
use fold_db::storage::{
    PlaneResidueDrainOptions, PlaneResidueFamily, INDEX_RESIDUE_LEGACY_COLLECTIONS,
};
use lastdb_node::atom_gc_reap::ReapPolicy;

mod atom_gc;
mod home;
mod residue;

use atom_gc::{atom_gc_audit, atom_gc_reap, AtomGcReapArgs};
use residue::{
    drain_plane, drain_tip_residue, index_plane_inventory, reclaim_index_residue,
    reclaim_keep_small_legacy, PlaneDrainArgs, ReclaimIndexResidueArgs, TipDrainArgs,
};

#[derive(Parser, Debug)]
#[command(name = "lastdb_local_maintain")]
struct Args {
    #[arg(long)]
    home: PathBuf,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum TipResidueCollection {
    Headers,
    Versions,
}

impl TipResidueCollection {
    fn as_str(self) -> &'static str {
        match self {
            Self::Headers => "field_tip_headers",
            Self::Versions => "field_tip_versions",
        }
    }
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum IndexResidueSource {
    Tips,
    FieldHashrangePageIndex,
    FieldHashrangeHashIndex,
    FieldHashrangeComplete,
    SchemaAtomIndex,
    LegacySchemaSecondaryIndex,
}

impl IndexResidueSource {
    fn as_str(self) -> &'static str {
        match self {
            Self::Tips => "tips",
            Self::FieldHashrangePageIndex => "field_hashrange_page_index",
            Self::FieldHashrangeHashIndex => "field_hashrange_hash_index",
            Self::FieldHashrangeComplete => "field_hashrange_complete",
            Self::SchemaAtomIndex => "schema_atom_index",
            Self::LegacySchemaSecondaryIndex => "legacy_schema_secondary_index",
        }
    }
}

// Shared `Drain*` prefix is intentional: clap maps these to `drain-*`
// subcommands operators discover together. Renaming variants would churn CLI.
#[allow(clippy::enum_variant_names)]
#[derive(Subcommand, Debug)]
enum Cmd {
    /// Copy/delete one page of tip residue into canonical `tips` (CoW first).
    ///
    /// Supports the still-live `field_tip_headers` / `field_tip_versions`
    /// fallbacks. `field_tips` / `mk:` was pruned from live dual-read on
    /// 2026-07-31 after a zero-hit primary soak.
    /// Default is dry-run. Never auto-deletes aside dumps.
    DrainTipResidue {
        /// Legacy tip-residue collection to drain (default: headers).
        #[arg(long, value_enum, default_value_t = TipResidueCollection::Headers)]
        collection: TipResidueCollection,
        #[arg(long)]
        execute: bool,
        #[arg(long)]
        after: Option<String>,
        /// Hex-encoded resume cursor (use when the key contains embedded NULs —
        /// argv cannot carry `\0`). Mutually exclusive with `--after`.
        #[arg(long)]
        after_hex: Option<String>,
        #[arg(long, default_value_t = 1000)]
        limit: usize,
        #[arg(long)]
        drop_empty_collection: bool,
        #[arg(long)]
        i_know_this_is_primary: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    /// Retired alias for the old `mk:` / `field_tips` dual-read fallback.
    DrainFieldTips {
        #[arg(long)]
        execute: bool,
        #[arg(long)]
        after: Option<String>,
        /// Hex-encoded resume cursor (use when the key contains embedded NULs —
        /// argv cannot carry `\0`). Mutually exclusive with `--after`.
        #[arg(long)]
        after_hex: Option<String>,
        #[arg(long, default_value_t = 1000)]
        limit: usize,
        #[arg(long)]
        drop_empty_collection: bool,
        #[arg(long)]
        i_know_this_is_primary: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    /// Copy protein-family keys from legacy `tips` into `proteins` (CoW first).
    ///
    /// `--prefix` is required (e.g. `protein:`, `molprot:`, `fldprot:`, `pfq:`)
    /// so the walk does not scan the whole tips collection.
    DrainProteinResidue {
        /// Key prefix to walk (required for tips source).
        #[arg(long)]
        prefix: String,
        #[arg(long)]
        execute: bool,
        #[arg(long)]
        after: Option<String>,
        /// Hex-encoded resume cursor (use when the key contains embedded NULs —
        /// argv cannot carry `\0`). Mutually exclusive with `--after`.
        #[arg(long)]
        after_hex: Option<String>,
        #[arg(long, default_value_t = 1000)]
        limit: usize,
        #[arg(long)]
        i_know_this_is_primary: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    /// Copy rebuildable index keys into `indexes` from tips or legacy splits.
    ///
    /// Order-log prefixes (`mord:` / `moc:` / `mo:`) are never drained here.
    /// When `--source tips`, pass `--prefix` (e.g. `mhr:`) — full tips scan refused.
    DrainIndexResidue {
        #[arg(long, value_enum, default_value_t = IndexResidueSource::Tips)]
        source: IndexResidueSource,
        /// Key prefix when source is `tips` (e.g. `mhr:`, `mhk:`, `schemaidx:`).
        #[arg(long)]
        prefix: Option<String>,
        #[arg(long)]
        execute: bool,
        #[arg(long)]
        after: Option<String>,
        /// Hex-encoded resume cursor for keys with embedded NULs. See protein drain.
        #[arg(long)]
        after_hex: Option<String>,
        #[arg(long, default_value_t = 1000)]
        limit: usize,
        #[arg(long)]
        drop_empty_source: bool,
        #[arg(long)]
        i_know_this_is_primary: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    /// Measure the `indexes` plane per key prefix (read-only).
    ///
    /// `compact --collection indexes` reports one live-key total for the whole
    /// plane, which cannot say how much of it is retired residue. This walks
    /// each prefix separately and reports keys + stored bytes, so a reclaim can
    /// be sized before it is run. Follows cursors to the end of each prefix.
    IndexPlaneInventory {
        /// Rows per page (cursor walk continues automatically).
        #[arg(long, default_value_t = 2000)]
        limit: usize,
        #[arg(long)]
        i_know_this_is_primary: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    /// Drop the dead `metadata` hash group that held the keep-small snapshot
    /// before it moved to its own plane (2026-09-21 primary restart loop:
    /// 39 GB of superseded `keep_small:meters` copies, loaded whole on the
    /// first write after boot). Offline twin of
    /// `lastdb db reclaim-keep-small-legacy`, for a home whose daemon cannot
    /// boot. Proves from the group's id sidecar that it holds only that key;
    /// never loads it. Default is dry-run.
    ReclaimKeepSmallLegacy {
        #[arg(long)]
        execute: bool,
        #[arg(long)]
        i_know_this_is_primary: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    /// Delete retired derived-index rows (`mhr:` / `mhi:` / `mhk:`) from `indexes`.
    ///
    /// These are engine-derived HashRange secondaries whose co-write and read
    /// preference are compiled off, so nothing rebuilds them and no read wants
    /// them — the authoritative `mk:` tips already serve those paths. The
    /// prefix set is DERIVED from those flags: re-enable one and its prefixes
    /// stop being reclaimable here.
    ///
    /// Default is a dry-run measurement.
    ///
    /// **`--execute` grows the plane before it shrinks it.** `LastStore::delete`
    /// is an append, so each removed row adds a tombstone carrying its own key,
    /// and these keys are long. Measured on a copy of the primary: deleting
    /// 737,485 rows holding 252 MiB took the `indexes` directory from 406 MiB
    /// to ~675 MiB while it ran. Budget headroom for that, and treat
    /// `lastdb db compact --collection indexes --execute` as part of the
    /// operation rather than an optional follow-up — it is the step that
    /// returns the bytes.
    ///
    /// The execute walk is IO-bound and far slower than the dry run over the
    /// same rows — 737k rows dry-ran in 12 s and had not finished executing
    /// after 20 min. The cause is not established; page size is the obvious
    /// dial but has not been shown to be the one that matters.
    ///
    /// CoW first — standing rule
    /// `preference-lastdb-no-engine-derived-hashrange-secondaries`.
    ReclaimIndexResidue {
        /// Limit to one prefix (default: every reclaimable prefix).
        #[arg(long)]
        prefix: Option<String>,
        #[arg(long)]
        execute: bool,
        /// Rows per page (cursor walk continues automatically).
        #[arg(long, default_value_t = 10_000)]
        limit: usize,
        #[arg(long)]
        i_know_this_is_primary: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    /// Audit atom GC candidates on an offline CoW clone.
    ///
    /// Read-only. Refuses primary/legacy homes unless `--i-know-this-is-primary`
    /// is passed. The report inventories referenced and unreferenced atom body
    /// keys, duplicate UUID groups that can be compared by raw value hash, and
    /// whether Card / BoardCards / MilestoneCards schema metadata is visible in
    /// the copied home.
    AtomGcAudit {
        /// Include at most N duplicate UUID group detail rows.
        #[arg(long, default_value_t = 100)]
        detail_limit: usize,
        #[arg(long)]
        i_know_this_is_primary: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    /// Reclaim redundant atom-body copies the audit proved safe (CoW only).
    ///
    /// Default is a dry-run plan; `--execute` deletes. Only removes a copy when
    /// an identical-content survivor stays reachable by the read ladder — every
    /// other case is reported ambiguous and left alone. Primary execution is not
    /// available from this command at all: `--i-know-this-is-primary` permits a
    /// primary *plan*, and is refused outright together with `--execute`.
    AtomGcReap {
        /// Delete. Without this the command only prints what it would do.
        #[arg(long)]
        execute: bool,
        /// Also delete bodies no live row references.
        ///
        /// Off by default: a duplicate delete is provably lossless, an orphan
        /// delete is only as good as the reference scan.
        #[arg(long)]
        reap_unreferenced_orphans: bool,
        /// Include at most N per-group decision rows in the report.
        #[arg(long, default_value_t = 100)]
        detail_limit: usize,
        #[arg(long)]
        i_know_this_is_primary: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
}

/// Decode optional plain/`--after-hex` resume cursors. Hex is required when the
/// key id contains embedded NULs (argv cannot carry `\0`; Python subprocess
/// raises "embedded null byte").
fn resolve_after_cursor(
    after: Option<String>,
    after_hex: Option<String>,
) -> Result<Option<String>, String> {
    match (after, after_hex) {
        (Some(_), Some(_)) => Err("pass only one of --after / --after-hex".into()),
        (Some(a), None) => Ok(Some(a)),
        (None, None) => Ok(None),
        (None, Some(h)) => {
            let raw = h.trim();
            if raw.is_empty() {
                return Ok(None);
            }
            let bytes = hex::decode(raw).map_err(|e| format!("--after-hex decode: {e}"))?;
            String::from_utf8(bytes)
                .map(Some)
                .map_err(|e| format!("--after-hex is not UTF-8 after decode: {e}"))
        }
    }
}

fn main() {
    if let Err(e) = run() {
        eprintln!("lastdb_local_maintain FAILED: {e}");
        std::process::exit(1);
    }
}

// lint:fn-size-ok moved verbatim from lastdb_local_maintain.rs; splitting this function is separate work
fn run() -> Result<(), String> {
    let args = Args::parse();
    match args.cmd {
        Cmd::DrainTipResidue {
            collection,
            execute,
            after,
            after_hex,
            limit,
            drop_empty_collection,
            i_know_this_is_primary,
            json,
        } => {
            let after = resolve_after_cursor(after, after_hex)?;
            drain_tip_residue(TipDrainArgs {
                home: &args.home,
                legacy_collection: collection.as_str(),
                execute,
                after,
                limit,
                drop_empty_collection,
                i_know_this_is_primary,
                json,
            })
        }
        Cmd::DrainFieldTips { .. } => Err(
            "drain-field-tips is retired: mk:/field_tips was pruned from live dual-read \
             after the 2026-07-31 zero-hit primary soak; drain remaining tip residue with \
             drain-tip-residue --collection headers|versions"
                .to_string(),
        ),
        Cmd::DrainProteinResidue {
            prefix,
            execute,
            after,
            after_hex,
            limit,
            i_know_this_is_primary,
            json,
        } => {
            let after = resolve_after_cursor(after, after_hex)?;
            drain_plane(PlaneDrainArgs {
                home: &args.home,
                options: PlaneResidueDrainOptions {
                    family: PlaneResidueFamily::Protein,
                    source_collection: "tips".into(),
                    target_collection: "proteins".into(),
                    key_prefix: Some(prefix),
                    after,
                    limit,
                    execute,
                    drop_empty_source: false,
                },
                i_know_this_is_primary,
                json,
            })
        }
        Cmd::DrainIndexResidue {
            source,
            prefix,
            execute,
            after,
            after_hex,
            limit,
            drop_empty_source,
            i_know_this_is_primary,
            json,
        } => {
            let source_collection = source.as_str().to_string();
            if source_collection != "tips"
                && !INDEX_RESIDUE_LEGACY_COLLECTIONS.contains(&source_collection.as_str())
            {
                return Err(format!("unsupported index source {source_collection}"));
            }
            let after = resolve_after_cursor(after, after_hex)?;
            drain_plane(PlaneDrainArgs {
                home: &args.home,
                options: PlaneResidueDrainOptions {
                    family: PlaneResidueFamily::Index,
                    source_collection,
                    target_collection: "indexes".into(),
                    key_prefix: prefix,
                    after,
                    limit,
                    execute,
                    drop_empty_source,
                },
                i_know_this_is_primary,
                json,
            })
        }
        Cmd::IndexPlaneInventory {
            limit,
            i_know_this_is_primary,
            json,
        } => index_plane_inventory(&args.home, limit, i_know_this_is_primary, json),
        Cmd::ReclaimKeepSmallLegacy {
            execute,
            i_know_this_is_primary,
            json,
        } => reclaim_keep_small_legacy(&args.home, execute, i_know_this_is_primary, json),
        Cmd::ReclaimIndexResidue {
            prefix,
            execute,
            limit,
            i_know_this_is_primary,
            json,
        } => reclaim_index_residue(&ReclaimIndexResidueArgs {
            home: &args.home,
            prefix,
            execute,
            limit,
            i_know_this_is_primary,
            json,
        }),
        Cmd::AtomGcAudit {
            detail_limit,
            i_know_this_is_primary,
            json,
        } => atom_gc_audit(&args.home, detail_limit, i_know_this_is_primary, json),
        Cmd::AtomGcReap {
            execute,
            reap_unreferenced_orphans,
            detail_limit,
            i_know_this_is_primary,
            json,
        } => atom_gc_reap(&AtomGcReapArgs {
            home: &args.home,
            execute,
            policy: ReapPolicy {
                reap_unreferenced_orphans,
            },
            detail_limit,
            i_know_this_is_primary,
            json,
        }),
    }
}
