//! Graceful shutdown: drain budgets, background drain, and the flush proof.

use std::sync::Arc;

use lastdb_node::host::Host;

pub(crate) fn shutdown_handler_drain_timeout() -> std::time::Duration {
    std::time::Duration::from_secs(180)
}

pub(crate) fn complete_shutdown_proof(
    home: &std::path::Path,
    pid: u32,
    start_ts: u64,
    errors: &[String],
) -> Result<(), String> {
    if !errors.is_empty() {
        return Err(format!(
            "shutdown did not prove a complete flush: {}",
            errors.join("; ")
        ));
    }
    lastdb_node::shutdown_receipt::write_shutdown_flush_receipt(home, pid, start_ts)
        .map_err(|error| format!("could not write shutdown flush proof: {error}"))?;
    Ok(())
}

/// Settle every owned task, including after a sibling failure, before the flush.
pub(crate) fn drain_background(
    runtime: &tokio::runtime::Runtime,
    host: &Arc<Host>,
    ttl_sweep_task: tokio::task::JoinHandle<()>,
    automatic_backup_task: tokio::task::JoinHandle<()>,
    shared_sync_task: tokio::task::JoinHandle<()>,
    received_shares_task: tokio::task::JoinHandle<()>,
) -> Result<(), String> {
    runtime.block_on(async {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(180);
        let drain = async {
            let (ttl_sweep, automatic, shared_sync, received_shares, byte_backup) = tokio::join!(
                ttl_sweep_task,
                automatic_backup_task,
                shared_sync_task,
                received_shares_task,
                host.byte_backup.drain(deadline),
            );
            let mut errors = Vec::new();
            for (name, result) in [
                ("ttl sweep", ttl_sweep),
                ("automatic backup", automatic),
                ("shared sync", shared_sync),
                ("received shares", received_shares),
            ] {
                if let Err(error) = result {
                    errors.push(format!("{name} task failed: {error}"));
                }
            }
            if let Err(error) = byte_backup {
                errors.push(format!("byte backup did not drain: {error}"));
            }
            if errors.is_empty() {
                Ok(())
            } else {
                Err(errors.join("; "))
            }
        };
        tokio::time::timeout_at(deadline, drain)
            .await
            .map_err(|_| "background writers did not drain within 180 seconds".to_string())?
    })
}

/// Complete every local flush barrier after the writers drain.
pub(crate) fn flush_database(runtime: &tokio::runtime::Runtime, host: &Host) -> Result<(), String> {
    runtime
        .block_on(host.db.shutdown())
        .map_err(|error| format!("FoldDB shutdown flush failed: {error}"))
}
