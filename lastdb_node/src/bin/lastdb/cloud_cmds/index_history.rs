//! Report and advance the acceptance index for an existing owner's personal log.

use std::path::Path;

use lastdb_node::log_sync::{index_history, IndexHistoryReport};

use super::{block_on_cloud, BackupIndexLogHistoryArgs};

pub(super) fn run(home: &Path, args: &BackupIndexLogHistoryArgs) -> Result<(), String> {
    if args.status == args.batches.is_some() {
        return Err("choose --status or --batches".into());
    }
    let report = block_on_cloud(
        "backup index-log-history",
        index_history(home, &args.database, args.batches, progress),
    )?;
    if args.json {
        let json =
            serde_json::to_string_pretty(&report).map_err(|_| "cannot encode index status")?;
        println!("{json}");
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
