//! Personal cloud log history indexing, reclamation, and snapshot wait helpers.

use super::{BackupIndexLogHistoryArgs, BackupReclaimArgs};
use cloud_backup_protocol::Digest;
use lastdb_node::log_sync::{index_history, reclaim_snapshot_log, IndexHistoryReport};
use serde_json::Value;
use std::path::Path;
use std::thread;
use std::time::{Duration, Instant};

const WAIT: Duration = Duration::from_secs(70);

enum Poll {
    Pending,
    Ready(u64, u64),
    Failed(String),
}

pub(super) fn run_index_history(
    home: &Path,
    args: &BackupIndexLogHistoryArgs,
) -> Result<(), String> {
    if args.status == args.batches.is_some() {
        return Err("choose --status or --batches".into());
    }
    let report = crate::block_on(
        "backup index-log-history",
        index_history(home, &args.database, args.batches, progress),
    )?;
    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).map_err(|_| "cannot encode index status")?
        );
        return Ok(());
    }
    println!(
        "Database {} ({})",
        report.database_id.as_str(),
        report.environment
    );
    println!("Cloud history indexing enabled: {}", report.index_enabled);
    println!("Database history eligible: {}", report.eligible);
    println!("First indexed delivery: {}", first(&report));
    if args.batches.is_some() {
        println!(
            "Confirmed {} batches and {} indexed entries",
            report.batches,
            report.indexed_entries.get()
        );
    }
    Ok(())
}

fn progress(report: &IndexHistoryReport) {
    eprintln!(
        "Confirmed {} batches and {} indexed entries. First indexed delivery is {}",
        report.batches,
        report.indexed_entries.get(),
        first(report),
    );
}

fn first(report: &IndexHistoryReport) -> String {
    report
        .first_indexed_delivery
        .map_or_else(|| "none".into(), |value| value.get().to_string())
}

pub(super) fn run_reclaim(home: &Path, args: &BackupReclaimArgs) -> Result<(), String> {
    if args.status == args.allow_required_client_upgrade {
        return Err("choose --status or --allow-required-client-upgrade".into());
    }
    let batches = args
        .allow_required_client_upgrade
        .then(|| args.batches.unwrap_or(16));
    let expected = args
        .expect_snapshot
        .clone()
        .map(Digest::try_from)
        .transpose()
        .map_err(|_| "--expect-snapshot requires a lowercase SHA-256 digest")?;
    let report = crate::block_on(
        "backup reclaim",
        reclaim_snapshot_log(
            home,
            &args.database,
            batches,
            expected.as_ref(),
            args.steps_per_request,
        ),
    )?;
    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).map_err(|_| "cannot encode reclaim status")?
        );
        return Ok(());
    }
    let snapshot = report.snapshot_id.as_ref().map_or("none", |id| id.as_str());
    println!(
        "Database {} ({})",
        report.database_id.as_str(),
        report.environment
    );
    println!("Cloud reclamation enabled: {}", report.reclaim_enabled);
    println!("Database history eligible: {}", report.eligible);
    println!("Recovery snapshot: {snapshot}");
    println!("Snapshot covers through {}", report.snapshot_through.get());
    println!(
        "Cloud log reclaimed through {}",
        report.reclaimed_through.get()
    );
    if args.allow_required_client_upgrade {
        println!("Reclaim requests confirmed: {}", report.batches);
        if let Some(state) = report.provider_cleanup {
            println!("Retired snapshot cleanup: {state}");
        }
    }
    Ok(())
}

pub(super) fn print_backup(value: &Value, json: bool) -> Result<(), String> {
    if !json {
        if let (Some(through), Some(replayed)) = (
            value["snapshot_through"].as_u64(),
            value["replayed"].as_u64(),
        ) {
            println!("snapshot_through={through} replayed={replayed}");
        }
        println!("Signed backup");
    }
    println!(
        "{}",
        serde_json::to_string_pretty(value).map_err(|_| "cannot encode backup result")?
    );
    Ok(())
}

pub(super) fn wait_until_head(socket: &Path, database: &str, nonce: &str) -> Result<(), String> {
    let path = format!("/api/backup/snapshot?database={database}&nonce={nonce}");
    let deadline = Instant::now() + WAIT;
    loop {
        match poll_status(socket, &path) {
            Poll::Ready(through, entries) => {
                println!("snapshot_through={through}");
                println!("log_head={through}");
                println!("log_entries={entries}");
                return Ok(());
            }
            Poll::Failed(err) => return Err(err),
            Poll::Pending if Instant::now() >= deadline => {
                return Err("signed snapshot did not reach the log head".into());
            }
            Poll::Pending => thread::sleep(Duration::from_millis(200)),
        }
    }
}

fn poll_status(socket: &Path, path: &str) -> Poll {
    let Ok(raw) = crate::get_path_retry(socket, path, Duration::from_secs(10)) else {
        return Poll::Pending;
    };
    let Ok(reply) = crate::parse_json_response(&raw, "backup snapshot status") else {
        return Poll::Pending;
    };
    if reply["ready"].as_bool() == Some(true) {
        if let (Some(through), Some(entries)) = (
            reply["snapshot_through"].as_u64(),
            reply["log_entries"].as_u64(),
        ) {
            return Poll::Ready(through, entries);
        }
    }
    if let Some(err) = reply["error"].as_str().filter(|s| !s.is_empty()) {
        return Poll::Failed(err.to_owned());
    }
    Poll::Pending
}
