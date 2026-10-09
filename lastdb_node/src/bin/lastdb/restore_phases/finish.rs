//! Completion markers and the restore report.

use super::*;

use fold_db::sync::engine::BackupRestoreMode;

/// Mark the destination complete and, for a flush-and-resume restore, re-arm
/// cloud sync. Returns the remote-ready record for a remote-only restore.
pub(crate) fn finalize_destination(
    homes: &Homes,
    flavor: &Flavor<'_>,
    applied: &Applied,
    remote_descriptor: Option<&RemoteRecovery>,
    source_db_hash: &str,
) -> Phase<Option<serde_json::Value>> {
    let target = &homes.target;
    let bootstrap_done = target.join(lastdb_node::cloud::BOOTSTRAP_DONE_FILE);
    write_owner_only_local(&bootstrap_done, b"ok\n").map_err(|detail| {
        io_failure(
            Stage::CompletionMarker,
            format!(
                "write {} after LastStore restore: {detail}",
                bootstrap_done.display()
            ),
        )
    })?;
    if applied.mode == BackupRestoreMode::ReplayTail && !flavor.remote_latest {
        lastdb_node::cloud::clear_cloud_resume_required(target)
            .map_err(|detail| io_failure(Stage::CompletionMarker, detail))?;
        lastdb_node::cloud::resume_cloud_sync_file(target)
            .map_err(|detail| io_failure(Stage::CompletionMarker, detail))?;
    }
    if !flavor.remote_only() {
        return Ok(None);
    }
    let report = &applied.report;
    let expected_mode = if flavor.remote_s0_only {
        BackupRestoreMode::S0Only
    } else {
        BackupRestoreMode::ReplayTail
    };
    if applied.mode != expected_mode
        || !report.remote_read_only
        || !report.source_scope_verified
        || !homes.target_paused_cloud.is_file()
        || !lastdb_node::cloud::cloud_resume_required_path(target).is_file()
        || homes.target_active_cloud.exists()
        || !bootstrap_done.is_file()
    {
        return Err(RestoreFailure::new(
            Stage::CompletionMarker,
            Code::OperationFailed,
            "remote restore did not leave a complete Cloud Off target",
        ));
    }
    let recovery = remote_descriptor.expect("remote recovery was checked");
    let (ready_file, mode_name) = if flavor.remote_s0_only {
        (RESCUE_S0_RESTORE_READY_FILE, "s0_only")
    } else {
        (NORMAL_LATEST_RESTORE_READY_FILE, "replay_tail")
    };
    let ready = serde_json::json!({
        "version": 1,
        "ok": true,
        "db_hash": source_db_hash,
        "store_uuid": recovery.descriptor.store_uuid,
        "manifest_sha256": report.manifest_sha256,
        "counter": report.counter,
        "restore_mode": mode_name,
        "cloud_sync_off": true,
    });
    let bytes = serde_json::to_vec_pretty(&ready).map_err(|error| {
        RestoreFailure::new(
            Stage::CompletionMarker,
            Code::SerializationError,
            format!("encode remote restore record: {error}"),
        )
    })?;
    write_owner_only_local(&target.join(ready_file), &bytes)
        .map_err(|detail| io_failure(Stage::CompletionMarker, detail))?;
    std::fs::File::open(target)
        .and_then(|dir| dir.sync_all())
        .map_err(|error| {
            io_failure(
                Stage::CompletionMarker,
                format!("sync remote restore home: {error}"),
            )
        })?;
    Ok(Some(ready))
}

fn render_failure(error: impl std::fmt::Display) -> RestoreFailure {
    RestoreFailure::new(
        Stage::RenderReport,
        Code::SerializationError,
        format!("encode report: {error}"),
    )
}

/// Print the restore report as JSON, or as the human-readable summary.
pub(crate) fn render_report(
    homes: &Homes,
    flavor: &Flavor<'_>,
    applied: &Applied,
    remote_ready: Option<serde_json::Value>,
    json_only: bool,
) -> Phase<()> {
    let report = &applied.report;
    if json_only {
        let mut report_json = serde_json::to_value(report).map_err(render_failure)?;
        if let Some(serde_json::Value::Object(ready)) = remote_ready {
            report_json
                .as_object_mut()
                .expect("restore report is an object")
                .extend(ready);
        }
        println!(
            "{}",
            serde_json::to_string_pretty(&report_json).map_err(render_failure)?
        );
        return Ok(());
    }
    println!("Restored LastStore backup into {}", homes.target.display());
    println!("  manifest: {}", report.manifest_sha256);
    println!("  counter:  {}", report.counter);
    println!("  cut_csn:  {}", report.cut_csn);
    println!("  chunks:   {}", report.chunks_installed);
    println!("  bytes:    {}", format_bytes(report.bytes_installed));
    println!("  epoch:    {}", report.restored_epoch);
    println!("  source scope verified: {}", report.source_scope_verified);
    println!("  remote read-only:      {}", report.remote_read_only);
    if applied.mode == BackupRestoreMode::S0Only {
        println!("  cloud mutation tail:  skipped");
        println!("  Cloud Sync:           Off");
    } else if flavor.remote_latest {
        println!("  Cloud Sync:           Off");
    }
    if let Some(ml) = &report.mutation_log_replay {
        println!(
            "  mutation-log: considered={} applied={} records={}",
            ml.segments_considered, ml.segments_applied, ml.records_applied
        );
    }
    Ok(())
}
