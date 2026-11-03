//! `lastdb db recount`: raise the replay cutoff of a STOPPED home.
//!
//! See `fold_db::db_operations::RecordStore::raise_replay_cutoff`. No gate and
//! no write path uses it.
//!
//! The home is protected before anything is opened: no daemon is alive for it,
//! its socket accepts no connection, and the last session ended cleanly with a
//! shutdown flush receipt. The store open takes the run home owner lock.
//! A home that a daemon serves is refused. The primary home is refused unless
//! the caller passes `--i-know-this-is-primary`.

use std::path::{Path, PathBuf};
use std::time::Instant;

use fold_db::db_operations::record_store::ReplayCutoffReport;
use fold_db::storage::config::{DatabaseConfig, StorageEngine};
use serde::Serialize;

use crate::offline_home::{load_e2e_keys, preflight_stopped_home, refuse_primary, Preflight};

/// What the caller asked for.
pub struct RecountArgs<'a> {
    pub home: &'a Path,
    /// Write the change. Without it the command only reports it.
    pub execute: bool,
    /// Raise the replay cutoff to this unix second. The cutoff is one record
    /// for the whole home.
    pub delete_cutoff_unix_secs: u64,
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
    replay_cutoff: &'a ReplayCutoffReport,
}

/// Raise the replay cutoff of a stopped home and print the report.
pub fn recount_offline(args: &RecountArgs<'_>) -> Result<(), String> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| e.to_string())?
        .as_secs();
    let cutoff = args.delete_cutoff_unix_secs;
    if cutoff == 0 || cutoff > now {
        return Err("delete cutoff requires a positive unix second at or before now".into());
    }
    let cutoff_nanos = cutoff
        .checked_mul(1_000_000_000)
        .ok_or_else(|| "delete cutoff overflows nanoseconds".to_string())?;
    if !args.i_know_this_is_primary {
        refuse_primary(args.home).map_err(|e| format!("{e}; or pass --i-know-this-is-primary"))?;
    }
    let store_root = args.home.join("data");
    if !store_root
        .join(fold_db::storage::disk_pack::NODE_MARKER)
        .is_file()
    {
        return Err(format!(
            "recount requires a native LastDB node home (data/{})",
            fold_db::storage::disk_pack::NODE_MARKER
        ));
    }
    let preflight = preflight_stopped_home(args.home, &store_root)?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        // Unoptimized builds keep the factory's futures on the stack.
        .thread_stack_size(16 * 1024 * 1024)
        .build()
        .map_err(|e| format!("tokio: {e}"))?;
    let started = Instant::now();
    let report = runtime.block_on(run_recount(
        args.home,
        store_root,
        cutoff_nanos,
        args.execute,
    ))?;
    print_report(args, &preflight, started.elapsed().as_secs(), &report)
}

/// Open the home the way the sibling offline tools do (no cloud sync), raise
/// the cutoff, then finish every local flush barrier even when the raise fails.
async fn run_recount(
    home: &Path,
    store_root: PathBuf,
    cutoff_nanos: u64,
    execute: bool,
) -> Result<ReplayCutoffReport, String> {
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
    let outcome = db
        .db_ops()
        .records()
        .raise_replay_cutoff(cutoff_nanos, execute)
        .await
        .map_err(|e| format!("raise the replay cutoff: {e}"));
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
    report: &ReplayCutoffReport,
) -> Result<(), String> {
    if args.json {
        let output = Output {
            ok: true,
            mode: "recount-offline",
            home: args.home.display().to_string(),
            preflight,
            elapsed_secs,
            replay_cutoff: report,
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
    println!(
        "  global replay cutoff {} -> {} ns",
        report.cutoff_before_nanos, report.cutoff_after_nanos
    );
    println!(
        "  {} operations in {} batches, {elapsed_secs} s",
        report.operations, report.batches
    );
    if !report.executed && report.operations > 0 {
        println!("  Dry run: nothing was written. Pass --execute to write the change.");
    }
    Ok(())
}
