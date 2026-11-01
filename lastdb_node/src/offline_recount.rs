//! `lastdb db recount`: repair the blob counters and the retention queue of a
//! STOPPED home.
//!
//! The recount reads the records and the field history, counts the file blob
//! references of each blob, and repairs the blob counters and the blob queue.
//! It gives a bucket of the retention queue that holds entries its directory
//! row back, and it can raise the replay cutoff. See
//! `fold_db::db_operations::RecordStore::recount_counters`. No gate and no write
//! path uses it.
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
    /// Set the blob counters to the exact count. Needs `all_scopes`.
    pub blobs_exact: bool,
    /// Raise the replay cutoff to this unix second. The cutoff is one record
    /// for the whole home, so it does not depend on the scopes.
    pub delete_cutoff_unix_secs: Option<u64>,
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
    /// The blob step of the recount, the same object as `recount.blobs`. A reader
    /// that looks for `blobs.rows_missing` finds it here.
    blobs: &'a fold_db::db_operations::BlobRecountReport,
    recount: &'a RecountReport,
}

/// Run the recount on a stopped home and print the report.
pub fn recount_offline(args: &RecountArgs<'_>) -> Result<(), String> {
    if let Some(cutoff) = args.delete_cutoff_unix_secs {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| e.to_string())?
            .as_secs();
        if cutoff == 0 || cutoff > now {
            return Err("delete cutoff requires a positive unix second at or before now".into());
        }
    }
    if !args.i_know_this_is_primary {
        refuse_primary(args.home).map_err(|e| format!("{e}; or pass --i-know-this-is-primary"))?;
    }
    let store_root = args.home.join("data");
    if !store_root.join("laststore-pack-node-v1").is_file() {
        return Err(
            "recount requires a native LastDB node home (data/laststore-pack-node-v1)".into(),
        );
    }
    for scope in &args.scopes {
        fold_db::db_operations::RecordStore::validate_scope(Some(scope))
            .map_err(|e| e.to_string())?;
    }
    let (preflight, _lock) = preflight_stopped_home(args.home, &store_root)?;
    let mut scopes = vec![None];
    scopes.extend(args.scopes.iter().cloned().map(Some));
    let options = RecountOptions {
        execute: args.execute,
        scopes,
        blobs_exact: args.blobs_exact && args.all_scopes,
        delete_cutoff_nanos: args
            .delete_cutoff_unix_secs
            .map(|secs| {
                secs.checked_mul(1_000_000_000)
                    .ok_or_else(|| "delete cutoff overflows nanoseconds".to_string())
            })
            .transpose()?,
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
    let db = fold_db::fold_db_core::factory::create_fold_db_with_options(
        &DatabaseConfig {
            path: store_root,
            engine: StorageEngine::Laststore,
        },
        &e2e,
        keypair,
        fold_db::fold_db_core::factory::FoldDbOptions {
            offline_maintenance: true,
            ..Default::default()
        },
    )
    .await
    .map_err(|e| format!("create_fold_db: {e}"))?;
    if all_scopes {
        match db.verify_ref_scopes().await {
            Ok(named) => {
                for scope in named {
                    if !options.scopes.contains(&Some(scope.clone())) {
                        options.scopes.push(Some(scope));
                    }
                }
            }
            Err(error) => {
                let _ = db.shutdown().await;
                return Err(format!("list the scopes: {error}"));
            }
        }
    }
    let outcome = db
        .db_ops()
        .records()
        .recount_counters(&options)
        .await
        .map_err(|e| format!("recount: {e}"));
    let shutdown = db
        .shutdown()
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
            blobs: &report.blobs,
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
        "  retention queue: {} entries read; {} entries in a bucket without a directory row; {} buckets repaired",
        report.queue_entries_read,
        report.queue_entries_without_directory,
        report.retention_buckets_repaired
    );
    println!(
        "  {} operations in {} batches, {} ms; range reads: {} calls, {} over runs of deleted keys, {} retried with fewer rows, {} narrowed",
        report.operations,
        report.batches,
        report.elapsed_ms,
        report.range_reads.calls,
        report.range_reads.skips,
        report.range_reads.shrinks,
        report.range_reads.splits
    );
    print_blob_step(report);
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

/// The blob step of the report.
fn print_blob_step(report: &RecountReport) {
    print_replay_cutoff(report);
    let blobs = &report.blobs;
    if !blobs.ran {
        return;
    }
    println!(
        "  blobs ({}): {} records and field history entries name {} blobs; {} counters read",
        if blobs.exact { "exact" } else { "raise only" },
        blobs.records_read,
        blobs.referenced_blobs,
        blobs.counters_read
    );
    println!(
        "  blob counters: {} unchanged, {} created, {} raised, {} lowered, {} suspect cleared, {} dwell cancelled, {} dwell started; {} queue entries read, {} deleted, {} restored, {} directory rows restored",
        blobs.unchanged,
        blobs.created,
        blobs.raised,
        blobs.lowered,
        blobs.suspect_cleared,
        blobs.dwell_cancelled,
        blobs.dwell_started,
        blobs.queue_read,
        blobs.queue_deleted,
        blobs.queue_restored,
        blobs.directory_rows_restored
    );
    if blobs.rows_missing > 0 {
        println!(
            "  blob rows missing: {} referenced blob(s), for example {}",
            blobs.rows_missing,
            blobs.missing_row_samples.join(", ")
        );
    }
    for note in &blobs.notes {
        println!("  note: {note}");
    }
}

fn print_replay_cutoff(report: &RecountReport) {
    if let Some(raised) = &report.replay_cutoff {
        println!(
            "  global replay cutoff {} -> {} ns ({})",
            raised.cutoff_before_nanos,
            raised.cutoff_after_nanos,
            if raised.executed {
                "executed"
            } else {
                "report only"
            }
        );
    }
}
