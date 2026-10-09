//! Part of `lastdb_local_maintain`.
// lint:file-size-ok moved verbatim from lastdb_local_maintain.rs; cohesive unit, split further in a later pass

use std::path::Path;

use super::home::{open_laststore, resolve_laststore_root};
use fold_db::db_operations::atom_store::retired_index_reclaim_prefixes;
use fold_db::storage::laststore::high_water_path_for_store_root;
use fold_db::storage::traits::NamespacedStore;
use fold_db::storage::{
    IndexResidueReclaimOptions, LastStoreNamespacedStore, PlaneResidueDrainOptions,
    PlaneResidueFamily, TipResidueDrainOptions, INDEX_RESIDUE_KEY_PREFIXES,
    TIP_RESIDUE_LEGACY_COLLECTIONS,
};
use lastdb_node::offline_home::refuse_primary;
use serde::Serialize;

pub(crate) struct TipDrainArgs<'a> {
    pub(crate) home: &'a Path,
    pub(crate) legacy_collection: &'a str,
    pub(crate) execute: bool,
    pub(crate) after: Option<String>,
    pub(crate) limit: usize,
    pub(crate) drop_empty_collection: bool,
    pub(crate) i_know_this_is_primary: bool,
    pub(crate) json: bool,
}

pub(crate) struct PlaneDrainArgs<'a> {
    pub(crate) home: &'a Path,
    pub(crate) options: PlaneResidueDrainOptions,
    pub(crate) i_know_this_is_primary: bool,
    pub(crate) json: bool,
}

pub(crate) fn drain_tip_residue(args: TipDrainArgs<'_>) -> Result<(), String> {
    if !TIP_RESIDUE_LEGACY_COLLECTIONS.contains(&args.legacy_collection) {
        return Err(format!(
            "unsupported tip-residue collection {}; expected one of {:?}",
            args.legacy_collection, TIP_RESIDUE_LEGACY_COLLECTIONS
        ));
    }
    if !args.i_know_this_is_primary {
        refuse_primary(args.home)?;
    }
    let store_root = resolve_laststore_root(args.home)?;
    if !args.i_know_this_is_primary {
        refuse_primary(&store_root)?;
    }

    let high_water = high_water_path_for_store_root(&store_root);
    let store = LastStoreNamespacedStore::open_with_options_and_high_water(
        &store_root,
        laststore::LastStoreOptions::hash_group(),
        high_water,
    )
    .map_err(|e| format!("open LastStore {}: {e}", store_root.display()))?;

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("tokio: {e}"))?;
    let report = runtime
        .block_on(store.drain_tip_residue_collection(TipResidueDrainOptions {
            legacy_collection: args.legacy_collection.to_string(),
            after: args.after,
            limit: args.limit,
            execute: args.execute,
            drop_empty_collection: args.drop_empty_collection,
        }))
        .map_err(|e| format!("drain {}: {e}", args.legacy_collection))?;

    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).map_err(|e| format!("json report: {e}"))?
        );
    } else {
        println!(
            "{} drain {}: scanned={} copied_to_tips={} tips_already_won={} \
             deleted_from_legacy={} skipped={} done={} after={} collection_dropped={}",
            args.legacy_collection,
            if report.dry_run { "dry-run" } else { "execute" },
            report.keys_scanned,
            report.copied_to_tips,
            report.tips_already_won,
            report.deleted_from_legacy,
            report.skipped,
            report.done,
            report.after.as_deref().unwrap_or(""),
            report.collection_dropped
        );
    }
    Ok(())
}

pub(crate) fn drain_plane(args: PlaneDrainArgs<'_>) -> Result<(), String> {
    if !args.i_know_this_is_primary {
        refuse_primary(args.home)?;
    }
    let store_root = resolve_laststore_root(args.home)?;
    if !args.i_know_this_is_primary {
        refuse_primary(&store_root)?;
    }

    let high_water = high_water_path_for_store_root(&store_root);
    let store = LastStoreNamespacedStore::open_with_options_and_high_water(
        &store_root,
        laststore::LastStoreOptions::hash_group(),
        high_water,
    )
    .map_err(|e| format!("open LastStore {}: {e}", store_root.display()))?;

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("tokio: {e}"))?;
    let label = format!(
        "{} {}→{}",
        args.options.family_label(),
        args.options.source_collection,
        args.options.target_collection
    );
    let report = runtime
        .block_on(store.drain_plane_residue_collection(args.options))
        .map_err(|e| format!("drain {label}: {e}"))?;

    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).map_err(|e| format!("json report: {e}"))?
        );
    } else {
        println!(
            "{} drain {}: scanned={} copied_to_target={} target_already_won={} \
             deleted_from_source={} skipped={} done={} after={} source_dropped={}",
            report.family,
            if report.dry_run { "dry-run" } else { "execute" },
            report.keys_scanned,
            report.copied_to_target,
            report.target_already_won,
            report.deleted_from_source,
            report.skipped,
            report.done,
            report.after.as_deref().unwrap_or(""),
            report.source_dropped
        );
    }
    Ok(())
}

pub(crate) struct ReclaimIndexResidueArgs<'a> {
    pub(crate) home: &'a Path,
    pub(crate) prefix: Option<String>,
    pub(crate) execute: bool,
    pub(crate) limit: usize,
    pub(crate) i_know_this_is_primary: bool,
    pub(crate) json: bool,
}

#[derive(Debug, Serialize)]
pub(crate) struct IndexPrefixRow {
    prefix: String,
    keys: u64,
    bytes: u64,
    reclaimable: bool,
}

#[derive(Debug, Serialize)]
pub(crate) struct IndexPlaneInventoryReport {
    ok: bool,
    home: String,
    per_prefix: Vec<IndexPrefixRow>,
    total_keys: u64,
    total_bytes: u64,
    reclaimable_keys: u64,
    reclaimable_bytes: u64,
    /// Prefixes this build would delete, derived from the engine flags.
    reclaimable_prefixes: Vec<String>,
}

/// Offline `reclaim-keep-small-legacy`: same store-level proof and drop as
/// the daemon verb, on an exclusively opened home. The daemon must not be
/// running against this home (LastStore is single-writer), which is also the
/// only situation this verb exists for.
pub(crate) fn reclaim_keep_small_legacy(
    home: &Path,
    execute: bool,
    i_know_this_is_primary: bool,
    json: bool,
) -> Result<(), String> {
    let store = open_laststore(home, i_know_this_is_primary)?;
    let report = store
        .drop_dead_hash_group(fold_db::storage::laststore::DeadHashGroupDropOptions {
            collection: "metadata".to_string(),
            expected_only_id: fold_db::db_operations::KEEP_SMALL_SNAPSHOT_KEY.to_string(),
            dry_run: !execute,
        })
        .map_err(|e| format!("reclaim-keep-small-legacy refused: {e}"))?;
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).map_err(|e| format!("serialize: {e}"))?
        );
        return Ok(());
    }
    let mode = if execute { "EXECUTED" } else { "DRY RUN" };
    println!("Legacy keep-small group reclaim (offline) — {mode}");
    println!("  group dir:     {}", report.dir);
    println!("  segments:      {}", report.segments);
    println!("  on_disk_bytes: {}", report.on_disk_bytes);
    println!("  ids recorded:  {:?}", report.ids);
    println!("  dropped:       {}", report.dropped);
    println!("  already_absent: {}", report.already_absent);
    if report.already_absent {
        println!("The retired group is already gone; there is nothing to reclaim here.");
        println!("Live keep_small residue: `lastdb db reclaim-keep-small-snapshot`.");
    } else if !execute {
        println!("Re-run with --execute to remove the group directory and return the bytes.");
    }
    Ok(())
}

pub(crate) fn index_plane_inventory(
    home: &Path,
    limit: usize,
    i_know_this_is_primary: bool,
    json: bool,
) -> Result<(), String> {
    let store = open_laststore(home, i_know_this_is_primary)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("tokio: {e}"))?;

    let reclaimable = retired_index_reclaim_prefixes();
    let mut per_prefix = Vec::new();
    for prefix in INDEX_RESIDUE_KEY_PREFIXES {
        let mut keys = 0u64;
        let mut bytes = 0u64;
        let mut after: Option<String> = None;
        loop {
            let page = runtime
                .block_on(store.index_plane_prefix_inventory(
                    (*prefix).to_string(),
                    after.clone(),
                    limit,
                ))
                .map_err(|e| format!("inventory {prefix}: {e}"))?;
            keys = keys.saturating_add(page.keys);
            bytes = bytes.saturating_add(page.bytes);
            if page.done {
                break;
            }
            // A non-done page always carries a cursor; treat its absence as the
            // end rather than looping forever on the same page.
            let Some(next) = page.after else { break };
            after = Some(next);
        }
        per_prefix.push(IndexPrefixRow {
            prefix: (*prefix).to_string(),
            keys,
            bytes,
            reclaimable: reclaimable.contains(prefix),
        });
    }

    let report = IndexPlaneInventoryReport {
        ok: true,
        home: home.display().to_string(),
        total_keys: per_prefix.iter().map(|r| r.keys).sum(),
        total_bytes: per_prefix.iter().map(|r| r.bytes).sum(),
        reclaimable_keys: per_prefix
            .iter()
            .filter(|r| r.reclaimable)
            .map(|r| r.keys)
            .sum(),
        reclaimable_bytes: per_prefix
            .iter()
            .filter(|r| r.reclaimable)
            .map(|r| r.bytes)
            .sum(),
        reclaimable_prefixes: reclaimable.iter().map(|p| (*p).to_string()).collect(),
        per_prefix,
    };

    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).map_err(|e| format!("json report: {e}"))?
        );
    } else {
        println!("indexes plane inventory ({})", report.home);
        for row in &report.per_prefix {
            println!(
                "  {:<16} keys={:<10} bytes={:<14} {}",
                row.prefix,
                row.keys,
                row.bytes,
                if row.reclaimable {
                    "RECLAIMABLE"
                } else {
                    "keep"
                }
            );
        }
        println!(
            "  total keys={} bytes={} | reclaimable keys={} bytes={} ({})",
            report.total_keys,
            report.total_bytes,
            report.reclaimable_keys,
            report.reclaimable_bytes,
            report.reclaimable_prefixes.join(" ")
        );
    }
    Ok(())
}

#[derive(Debug, Serialize)]
pub(crate) struct ReclaimIndexResidueReport {
    ok: bool,
    dry_run: bool,
    home: String,
    per_prefix: Vec<fold_db::storage::IndexResidueReclaimReport>,
    keys_deleted: u64,
    bytes_freed_approx: u64,
    next_step: &'static str,
}

// lint:fn-size-ok moved verbatim from lastdb_local_maintain.rs; splitting this function is separate work
pub(crate) fn reclaim_index_residue(args: &ReclaimIndexResidueArgs<'_>) -> Result<(), String> {
    let reclaimable = retired_index_reclaim_prefixes();
    if reclaimable.is_empty() {
        return Err(
            "no reclaimable index prefixes in this build — a derived HashRange \
             index has been re-enabled, so its rows are load-bearing again"
                .to_string(),
        );
    }
    let prefixes: Vec<String> = match &args.prefix {
        Some(one) => {
            if !reclaimable.contains(&one.as_str()) {
                return Err(format!(
                    "prefix {one} is not reclaimable in this build; expected one of {reclaimable:?}"
                ));
            }
            vec![one.clone()]
        }
        None => reclaimable.iter().map(|p| (*p).to_string()).collect(),
    };

    let store = open_laststore(args.home, args.i_know_this_is_primary)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("tokio: {e}"))?;

    let mut per_prefix = Vec::new();
    for prefix in prefixes {
        let mut total = fold_db::storage::IndexResidueReclaimReport {
            prefix: prefix.clone(),
            dry_run: !args.execute,
            done: false,
            ..Default::default()
        };
        let mut after: Option<String> = None;
        loop {
            let page = runtime
                .block_on(
                    store.reclaim_retired_index_residue(IndexResidueReclaimOptions {
                        prefix: prefix.clone(),
                        after: after.clone(),
                        limit: args.limit,
                        execute: args.execute,
                    }),
                )
                .map_err(|e| format!("reclaim {prefix}: {e}"))?;
            total.keys_scanned = total.keys_scanned.saturating_add(page.keys_scanned);
            total.keys_deleted = total.keys_deleted.saturating_add(page.keys_deleted);
            total.bytes_freed_approx = total
                .bytes_freed_approx
                .saturating_add(page.bytes_freed_approx);
            total.skipped = total.skipped.saturating_add(page.skipped);
            if page.done {
                total.done = true;
                break;
            }
            // On a dry run the rows stay put, so the cursor is the only thing
            // that advances — without it this would re-read page one forever.
            let Some(next) = page.after else {
                total.done = true;
                break;
            };
            after = Some(next);
        }
        per_prefix.push(total);
    }

    let report = ReclaimIndexResidueReport {
        ok: true,
        dry_run: !args.execute,
        home: args.home.display().to_string(),
        keys_deleted: per_prefix.iter().map(|r| r.keys_deleted).sum(),
        bytes_freed_approx: per_prefix.iter().map(|r| r.bytes_freed_approx).sum(),
        next_step: "lastdb db compact --collection indexes --execute",
        per_prefix,
    };

    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).map_err(|e| format!("json report: {e}"))?
        );
    } else {
        println!(
            "reclaim-index-residue {} ({})",
            if report.dry_run { "dry-run" } else { "execute" },
            report.home
        );
        for row in &report.per_prefix {
            println!(
                "  {:<8} scanned={:<10} deleted={:<10} bytes~{:<14} skipped={} done={}",
                row.prefix,
                row.keys_scanned,
                row.keys_deleted,
                row.bytes_freed_approx,
                row.skipped,
                row.done
            );
        }
        println!(
            "  total deleted={} bytes~{} — deletes are appends; next: {}",
            report.keys_deleted, report.bytes_freed_approx, report.next_step
        );
    }
    Ok(())
}

pub(crate) trait FamilyLabel {
    fn family_label(&self) -> &'static str;
}

impl FamilyLabel for PlaneResidueDrainOptions {
    fn family_label(&self) -> &'static str {
        match self.family {
            PlaneResidueFamily::Tip => "tip",
            PlaneResidueFamily::Protein => "protein",
            PlaneResidueFamily::Index => "index",
            PlaneResidueFamily::Conflict => "conflict",
            PlaneResidueFamily::OrderLog => "order_log",
        }
    }
}
