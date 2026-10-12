use super::*;

pub(super) fn command(data_dir: Option<PathBuf>, args: &BackupGcCompactArgs) -> Result<(), String> {
    let home = lastdb_node::host::resolve_home(data_dir)?;
    let report = fold_db::sync::engine::gc_jobs::compact_terminal_receipts(
        &home.join("backup_gc_jobs"),
        &args.job_id,
        args.remove_sources,
    )
    .map_err(|error| format!("local backup receipt compaction failed: {error}"))?;
    if args.json {
        println!(
            "{}",
            serde_json::to_string(&report)
                .map_err(|error| format!("receipt report JSON failed: {error}"))?
        );
    } else {
        println!(
            "Backup job {}: {} receipts, {} packed bytes, {} source files removed.",
            report.job_id, report.records, report.pack_bytes, report.source_files_removed
        );
        println!("Summary SHA256: {}", report.summary_sha256);
        println!("Pack SHA256: {}", report.pack_sha256);
        println!("Job summary and backup index stay unchanged by this command.");
    }
    Ok(())
}
