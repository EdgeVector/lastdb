//! Bounded background reaper for orphan `protein:` rows.
//!
//! ## Why
//!
//! `gc_orphan_proteins` already exists (`lastdb db gc-proteins`), but it is a
//! manual verb — nothing runs it unless an operator remembers to. `protein:`
//! is otherwise **unbounded**: no code path deletes a row once created, so a
//! future client-probe leak (like the historical one that left 30,311 orphan
//! rows with no `molprot:` back-ref) grows forever instead of getting reaped.
//!
//! This gives the class a reaper the same way [`super::mutation_flush`] gives
//! mutation durability a background flusher: an opt-in periodic task,
//! env-configurable, disabled when unset or set to `0`, stopped explicitly on
//! shutdown and on Drop so tests do not leak tasks.
//!
//! ## Env
//!
//! | Variable | Meaning |
//! |----------|---------|
//! | `LASTDB_PROTEIN_REAPER_MS` | Interval for the background reaper. Unset or `0` disables it. |

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::db_operations::DbOperations;

/// Env: background protein-reaper interval in milliseconds (`0` = off).
pub const PROTEIN_REAPER_MS_ENV: &str = "LASTDB_PROTEIN_REAPER_MS";

/// Suggested reaper period — a full-scan class, so hours not milliseconds.
pub const DEFAULT_PROTEIN_REAPER_MS: u64 = 21_600_000;

/// Resolved protein-reaper policy for this process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProteinReaperPolicy {
    /// Background reaper period. `None` means no periodic reaper.
    pub interval: Option<Duration>,
}

impl ProteinReaperPolicy {
    /// Parse from environment.
    pub fn from_env() -> Self {
        Self {
            interval: parse_protein_reaper_interval(std::env::var(PROTEIN_REAPER_MS_ENV).ok()),
        }
    }
}

/// `LASTDB_PROTEIN_REAPER_MS` → interval; unset / empty / `0` → off.
#[must_use]
pub fn parse_protein_reaper_interval(raw: Option<String>) -> Option<Duration> {
    let raw = raw?;
    let s = raw.trim();
    if s.is_empty() {
        return None;
    }
    match s.parse::<u64>() {
        Ok(0) => None,
        Ok(ms) => Some(Duration::from_millis(ms)),
        Err(_) => {
            tracing::warn!(
                raw = %s,
                env = PROTEIN_REAPER_MS_ENV,
                default_ms = DEFAULT_PROTEIN_REAPER_MS,
                "invalid LASTDB_PROTEIN_REAPER_MS; disabling background protein reaper"
            );
            None
        }
    }
}

/// Handle for the optional periodic protein-reaper task.
///
/// Aborted on [`super::fold_db::FoldDB::shutdown`] before the final
/// durability flush, and on Drop so tests do not leak tasks.
pub struct BackgroundProteinReaperTask {
    join: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Set when we intentionally stop; the loop checks this so abort races
    /// do not log spurious reaper errors mid-shutdown.
    stop: Arc<AtomicBool>,
}

impl BackgroundProteinReaperTask {
    /// No-op handle when the background reaper is disabled.
    pub fn disabled() -> Self {
        Self {
            join: Mutex::new(None),
            stop: Arc::new(AtomicBool::new(true)),
        }
    }

    /// Spawn a periodic `db_ops.atoms().gc_orphan_proteins(false, None)` on
    /// the current Tokio runtime.
    pub fn spawn(db_ops: Arc<DbOperations>, interval: Duration) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_flag = Arc::clone(&stop);
        // lint:spawn-bare-ok process-lifetime reaper loop — not request-scoped.
        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            // Skip the immediate first tick so boot is not followed by a
            // pointless full protein scan before anything has leaked.
            ticker.tick().await;
            loop {
                ticker.tick().await;
                if stop_flag.load(Ordering::Relaxed) {
                    break;
                }
                match db_ops.atoms().gc_orphan_proteins(false, None).await {
                    Ok(report) if report.proteins_deleted > 0 => {
                        tracing::info!(
                            target: "fold_node::database",
                            proteins_deleted = report.proteins_deleted,
                            bytes_freed_approx = report.bytes_freed_approx,
                            "background protein reaper reclaimed orphan rows"
                        );
                    }
                    Ok(_) => {}
                    Err(e) => {
                        if stop_flag.load(Ordering::Relaxed) {
                            break;
                        }
                        tracing::warn!(
                            target: "fold_node::database",
                            error = %e,
                            "background protein reaper failed"
                        );
                    }
                }
            }
        });
        Self {
            join: Mutex::new(Some(handle)),
            stop,
        }
    }

    /// Signal the loop to exit and abort the task (best-effort).
    pub fn stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self
            .join
            .lock()
            .expect("background protein reaper join lock")
            .take()
        {
            handle.abort();
        }
    }

    pub fn is_running(&self) -> bool {
        self.join
            .lock()
            .expect("background protein reaper join lock")
            .as_ref()
            .is_some_and(|h| !h.is_finished())
    }
}

impl Drop for BackgroundProteinReaperTask {
    fn drop(&mut self) {
        self.stop();
    }
}
