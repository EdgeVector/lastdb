//! Inspect or advance the reclaimed cloud log position from an existing owner home.

use super::{block_on_cloud, BackupReclaimArgs};
use std::path::Path;

pub(super) fn run(home: &Path, args: BackupReclaimArgs) -> Result<(), String> {
    if args.status == args.allow_required_client_upgrade {
        return Err("choose --status or --allow-required-client-upgrade".into());
    }
    let batches = args
        .allow_required_client_upgrade
        .then(|| args.batches.unwrap_or(16));
    let report = block_on_cloud(
        "backup reclaim",
        lastdb_node::log_sync::reclaim_snapshot_log(home, &args.database, batches),
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
    println!("Account generation {}", report.account_generation.get());
    println!("Cloud reclamation enabled: {}", report.reclaim_enabled);
    println!("Database history eligible: {}", report.eligible);
    println!("Recovery snapshot: {snapshot}");
    println!("Snapshot covers through {}", report.snapshot_through.get());
    println!(
        "Cloud log reclaimed through {}",
        report.reclaimed_through.get()
    );
    if args.allow_required_client_upgrade {
        println!("Batches reclaimed: {}", report.batches);
    }
    Ok(())
}
