//! Graceful shutdown: drain budgets, background drain, and the flush proof.

use std::sync::Arc;

use lastdb_node::host::Host;

pub(crate) fn shutdown_handler_drain_timeout() -> std::time::Duration {
    #[cfg(debug_assertions)]
    if std::env::var("LASTDB_ISOLATED_COPY").ok().as_deref() == Some("1") {
        if let Ok(raw) = std::env::var("LASTDB_TEST_SHUTDOWN_DRAIN_TIMEOUT_MS") {
            if let Ok(ms) = raw.parse::<u64>() {
                if (1..=180_000).contains(&ms) {
                    return std::time::Duration::from_millis(ms);
                }
            }
        }
    }
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

/// Wait for the sampler and conflict-fold tasks, then for background writers.
pub(crate) fn drain_background(
    runtime: &tokio::runtime::Runtime,
    host: &Arc<Host>,
    sampler_task: tokio::task::JoinHandle<()>,
    home_fold_task: tokio::task::JoinHandle<()>,
) -> Result<(), String> {
    runtime.block_on(async {
        let drain = async {
            sampler_task
                .await
                .map_err(|error| format!("self-metrics task failed: {error}"))?;
            home_fold_task
                .await
                .map_err(|error| format!("home conflict task failed: {error}"))?;
            while host.background_writes_in_flight() {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
            Ok::<(), String>(())
        };
        tokio::time::timeout(std::time::Duration::from_secs(180), drain)
            .await
            .map_err(|_| "background writers did not drain within 180 seconds".to_string())?
    })
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
