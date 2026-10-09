use super::*;

/// The owner calls this only after a fresh source-free restore proves the
/// normal backup. The exact manifest SHA binds that external proof to this
/// durable cut. The job rechecks cloud and local state before it turns sync on.
pub(crate) fn finish(
    host: &Host,
    restore_manifest_sha256: &str,
    restore_home: Option<&str>,
) -> Result<serde_json::Value, String> {
    if restore_manifest_sha256.len() != 64
        || !restore_manifest_sha256
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err("finish requires the restored normal manifest SHA-256".into());
    }
    if cloud::cloud_sync_file_state(&host.home) != "on"
        || !cloud::cloud_resume_required_path(&host.home).exists()
    {
        return Err("finish requires an active config and durable resume marker".into());
    }
    let engine = host
        .db
        .sync_engine()
        .ok_or("finish requires the backup-only sync engine")?;
    if !engine.is_backup_only_mode() {
        return Err("finish requires the backup-only sync engine".into());
    }
    if host
        .primary_resume_job_running
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return status(host);
    }
    let mut receipt = match read_receipt(&host.home) {
        Ok(Some(receipt)) => receipt,
        Ok(None) => {
            host.primary_resume_job_running
                .store(false, Ordering::Release);
            return Err("finish requires a verified backup receipt".into());
        }
        Err(error) => {
            host.primary_resume_job_running
                .store(false, Ordering::Release);
            return Err(error);
        }
    };
    if !receipt.accepts_restored_manifest(restore_manifest_sha256) {
        host.primary_resume_job_running
            .store(false, Ordering::Release);
        return Err("finish requires proof of the exact verified normal backup".into());
    }
    if receipt.fresh_from_local {
        let result = restore_home
            .ok_or_else(|| "fresh backup finish requires --restore-home".to_string())
            .and_then(|path| restore_receipt::require_fresh_restore(&host.home, path, &receipt));
        if let Err(error) = result {
            host.primary_resume_job_running
                .store(false, Ordering::Release);
            return Err(error);
        }
    }
    receipt.process_start_ts = host.process_start_ts;
    receipt.phase = "finish_accepted".into();
    receipt.error = None;
    if let Err(error) = write_receipt(&host.home, &receipt) {
        host.primary_resume_job_running
            .store(false, Ordering::Release);
        return Err(error);
    }
    let view = receipt.view(&host.home, true);
    let home = host.home.clone();
    let db = Arc::clone(&host.db);
    let running = Arc::clone(&host.primary_resume_job_running);
    if let Err(error) = std::thread::Builder::new()
        .name("lastdb-primary-resume-finish".into())
        .spawn(move || {
            let result = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| format!("start primary resume finish runtime: {error}"))
                .and_then(|runtime| runtime.block_on(run_finish(&home, &db, &mut receipt)));
            if let Err(error) = result {
                receipt.phase = "failed".into();
                receipt.error = Some(error);
                let _ = write_receipt(&home, &receipt);
            }
            running.store(false, Ordering::Release);
        })
    {
        host.primary_resume_job_running
            .store(false, Ordering::Release);
        return Err(format!("start primary resume finish job: {error}"));
    }
    Ok(view)
}
