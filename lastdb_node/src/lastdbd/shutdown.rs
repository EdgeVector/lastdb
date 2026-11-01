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
    lastdb_node::session_ledger::write_shutdown_flush_receipt(home, pid, start_ts)
        .map_err(|error| format!("could not write shutdown flush proof: {error}"))?;
    if let Err(error) = lastdb_node::session_ledger::mark_clean_shutdown_with_reason(
        home,
        pid,
        Some("signal (graceful shutdown)"),
    ) {
        lastdb_node::session_ledger::clear_shutdown_flush_receipt(home)
            .map_err(|clear| format!("could not clear failed shutdown proof: {clear}"))?;
        return Err(format!("could not record clean shutdown: {error}"));
    }
    Ok(())
}

/// Settle every owned task, including after a sibling failure, before the flush.
pub(crate) fn drain_background(
    runtime: &tokio::runtime::Runtime,
    host: &Arc<Host>,
    sampler_task: tokio::task::JoinHandle<()>,
    automatic_backup_task: tokio::task::JoinHandle<()>,
) -> Result<(), String> {
    runtime.block_on(async {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(180);
        let drain = async {
            let (sampler, automatic, byte_backup) = tokio::join!(
                sampler_task,
                automatic_backup_task,
                host.byte_backup.drain(deadline),
            );
            let mut errors = Vec::new();
            for (name, result) in [("self-metrics", sampler), ("automatic backup", automatic)] {
                if let Err(error) = result {
                    errors.push(format!("{name} task failed: {error}"));
                }
            }
            if let Err(error) = byte_backup {
                errors.push(format!("byte backup did not drain: {error}"));
            }
            while host.background_writes_in_flight() {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
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

/// Persist the graceful-shutdown intent BEFORE the bounded async drain.
/// launchd's kill window can expire while shutdown is still flushing; without
/// this intermediate disposition the next boot misclassifies every such
/// restart as a native crash and emits a false `unclean_exit` Sentry event.
pub(crate) fn record_shutdown_intent(
    ledger_present: bool,
    home: &std::path::Path,
    pid: u32,
    errors: &mut Vec<String>,
) {
    if !ledger_present {
        errors.push("session ledger is absent".to_string());
        return;
    }
    if let Err(e) = lastdb_node::session_ledger::mark_shutdown_started_with_reason(
        home,
        pid,
        Some("signal (graceful shutdown started)"),
    ) {
        tracing::warn!(
            target: "lastdbd::session_ledger",
            error = %e,
            "couldn't record graceful shutdown intent in the session ledger"
        );
        errors.push(format!("could not record shutdown intent: {e}"));
    }
}
