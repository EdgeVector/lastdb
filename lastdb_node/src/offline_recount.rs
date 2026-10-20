//! `lastdb db recount`: rebuild every atom counter of a STOPPED home.
//!
//! The recount reads the tip records and the conflict records, counts the
//! references of each atom, and writes the exact count into the counter record.
//! See `fold_db::db_operations::AtomStore::recount_counters`. It is the one
//! scan the counter design keeps. No gate and no write path uses it.
//!
//! The home is protected before anything is opened: no daemon is alive for it,
//! neither socket accepts a connection, the last session ended cleanly with a
//! shutdown flush receipt, and this process holds the offline maintenance lock.
//! A home that a daemon serves is refused. The primary home is refused unless
//! the caller passes `--i-know-this-is-primary`.

use std::path::{Path, PathBuf};
use std::time::Instant;

use fold_db::db_operations::{RecountOptions, RecountReport};
use fold_db::storage::config::{DatabaseConfig, StorageEngine};
use serde::Serialize;

use crate::offline_home::{load_e2e_keys, preflight_stopped_home, refuse_primary, Preflight};

/// What the caller asked for.
pub struct RecountArgs<'a> {
    pub home: &'a Path,
    /// Write the changes. Without it the command only reports them.
    pub execute: bool,
    /// Storage scope prefixes to recount, besides the personal scope.
    pub scopes: Vec<String>,
    /// Recount each scope that the database catalog names, besides the
    /// personal scope.
    pub all_scopes: bool,
    pub shards: u32,
    pub i_know_this_is_primary: bool,
    pub json: bool,
}

#[derive(Serialize)]
struct Output<'a> {
    ok: bool,
    mode: &'static str,
    home: String,
    preflight: &'a Preflight,
    elapsed_secs: u64,
    recount: &'a RecountReport,
}

/// Run the recount on a stopped home and print the report.
pub fn recount_offline(args: &RecountArgs<'_>) -> Result<(), String> {
    if !args.i_know_this_is_primary {
        refuse_primary(args.home).map_err(|e| format!("{e}; or pass --i-know-this-is-primary"))?;
    }
    let store_root = args.home.join("data");
    if !store_root.join("laststore-pack-node-v1").is_file() {
        return Err(
            "recount requires a native LastDB node home (data/laststore-pack-node-v1)".into(),
        );
    }
    let (preflight, _lock) = preflight_stopped_home(args.home, &store_root)?;
    let mut scopes = vec![None];
    scopes.extend(args.scopes.iter().cloned().map(Some));
    let options = RecountOptions {
        execute: args.execute,
        scopes,
        shards: args.shards.max(1),
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        // Unoptimized builds keep the factory's futures on the stack.
        .thread_stack_size(16 * 1024 * 1024)
        .build()
        .map_err(|e| format!("tokio: {e}"))?;
    let started = Instant::now();
    let report = runtime.block_on(run_recount(args.home, store_root, options, args.all_scopes))?;
    print_report(args, &preflight, started.elapsed().as_secs(), &report)
}

/// Open the home the way the sibling offline tools do (no cloud sync), run the
/// recount, then finish every local flush barrier even when the recount fails.
async fn run_recount(
    home: &Path,
    store_root: PathBuf,
    mut options: RecountOptions,
    all_scopes: bool,
) -> Result<RecountReport, String> {
    let (e2e, keypair) = load_e2e_keys(home)?;
    let db = fold_db::fold_db_core::factory::create_fold_db(
        &DatabaseConfig {
            path: store_root,
            engine: StorageEngine::Laststore,
            cloud_sync: None,
        },
        &e2e,
        keypair,
    )
    .await
    .map_err(|e| format!("create_fold_db: {e}"))?;
    if all_scopes {
        match db.atom_counter_scopes().await {
            Ok(named) => {
                for scope in named {
                    if !options.scopes.contains(&Some(scope.clone())) {
                        options.scopes.push(Some(scope));
                    }
                }
            }
            Err(error) => {
                let _ = db.shutdown_local_only().await;
                return Err(format!("list the scopes: {error}"));
            }
        }
    }
    let outcome = db
        .db_ops()
        .atoms()
        .recount_counters(&options)
        .await
        .map_err(|e| format!("recount: {e}"));
    let shutdown = db
        .shutdown_local_only()
        .await
        .map_err(|e| format!("local shutdown after the recount: {e}"));
    match (outcome, shutdown) {
        (Ok(report), Ok(())) => Ok(report),
        (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
        (Err(error), Err(shutdown_error)) => Err(format!("{error}; {shutdown_error}")),
    }
}

fn print_report(
    args: &RecountArgs<'_>,
    preflight: &Preflight,
    elapsed_secs: u64,
    report: &RecountReport,
) -> Result<(), String> {
    if args.json {
        let output = Output {
            ok: true,
            mode: "recount-offline",
            home: args.home.display().to_string(),
            preflight,
            elapsed_secs,
            recount: report,
        };
        let text = serde_json::to_string_pretty(&output).map_err(|e| format!("serialize: {e}"))?;
        println!("{text}");
        return Ok(());
    }
    println!(
        "recount-offline ({})",
        if report.executed {
            "executed"
        } else {
            "dry run"
        }
    );
    println!("  home: {}", args.home.display());
    let scopes: Vec<&str> = report
        .scopes
        .iter()
        .map(|scope| scope.as_deref().unwrap_or("personal"))
        .collect();
    println!("  scopes: {}", scopes.join(", "));
    println!(
        "  read: {} tips, {} conflict records, {} counters, {} bodies; {} atoms are referenced",
        report.tips_read,
        report.records_read,
        report.counters_read,
        report.bodies_read,
        report.referenced_atoms
    );
    println!(
        "  counters: {} unchanged, {} created, {} raised, {} lowered, {} suspect cleared, {} rewritten",
        report.unchanged, report.created, report.raised, report.lowered, report.suspect_cleared, report.rewritten
    );
    println!(
        "  deleted: {} counters without a body, {} bodies without a holder",
        report.counters_deleted, report.bodies_deleted
    );
    println!(
        "  lost bodies (named by tips or records, no body): {}",
        report.lost_bodies
    );
    println!(
        "  retention queue: {} entries read; {} records without a queue entry, {} entries in a bucket without a directory row; repaired: {} records, {} buckets",
        report.queue_entries_read,
        report.records_without_queue_entry,
        report.queue_entries_without_directory,
        report.retention_records_repaired,
        report.retention_buckets_repaired
    );
    println!(
        "  {} operations in {} batches, {} ms; range reads: {} calls, {} retried with fewer rows, {} narrowed",
        report.operations,
        report.batches,
        report.elapsed_ms,
        report.range_reads.calls,
        report.range_reads.shrinks,
        report.range_reads.splits
    );
    for change in &report.changes {
        println!(
            "  {} {} {:?} -> {:?}",
            change.action, change.atom, change.before, change.after
        );
    }
    for note in &report.notes {
        println!("  note: {note}");
    }
    if !report.executed && report.changed() > 0 {
        println!(
            "  Dry run: nothing was written. Pass --execute to write {} change(s).",
            report.changed()
        );
    }
    Ok(())
}
