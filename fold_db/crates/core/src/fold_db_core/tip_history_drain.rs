//! Bounded background drain for legacy tip-version (`tv:`) chains.
//!
//! ## Why
//!
//! Tip history is opt-in for new writes, but existing live chains remain until
//! something reclaims them. The only prior reclaim path is the full-store
//! manual verb `gc-atoms --prune-live-history`, which is unbounded and pairs
//! badly with cloud-backup re-staging when operators fall back to collection
//! compaction.
//!
//! This scheduler runs the same CAS/revalidation-safe drain as
//! [`crate::db_operations::AtomStore::drain_tip_history_chains_from_checkpoint`]
//! on a cadence: each tick is a **bounded page**, resumes from a durable
//! cursor, never compact a collection, and never mutates the live primary
//! outside ordinary tip rewrites / `tv:` deletes.
//!
//! ## Env
//!
//! | Variable | Meaning |
//! |----------|---------|
//! | `LASTDB_TIP_HISTORY_DRAIN_MS` | Interval for the background drain. Unset or `0` disables it. |
//! | `LASTDB_TIP_HISTORY_DRAIN_MAX_KEYS` | Optional max `mk:` tips examined per tick (default 256). |

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::db_operations::{DbOperations, TipHistoryDrainOptions};

/// Env: background tip-history drain interval in milliseconds (`0` = off).
pub const TIP_HISTORY_DRAIN_MS_ENV: &str = "LASTDB_TIP_HISTORY_DRAIN_MS";

/// Env: max `mk:` keys examined per background tick.
pub const TIP_HISTORY_DRAIN_MAX_KEYS_ENV: &str = "LASTDB_TIP_HISTORY_DRAIN_MAX_KEYS";

/// Suggested drain period — history reclaim, not a hot path.
pub const DEFAULT_TIP_HISTORY_DRAIN_MS: u64 = 3_600_000;

/// Default scan budget per tick when the env override is unset.
pub const DEFAULT_TIP_HISTORY_DRAIN_MAX_KEYS: usize = 256;

/// Resolved tip-history drain policy for this process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TipHistoryDrainPolicy {
    /// Background drain period. `None` means no periodic drain.
    pub interval: Option<Duration>,
    /// Max `mk:` tips examined per tick.
    pub max_keys: usize,
}

impl TipHistoryDrainPolicy {
    /// Parse from environment.
    pub fn from_env() -> Self {
        Self {
            interval: parse_tip_history_drain_interval(
                std::env::var(TIP_HISTORY_DRAIN_MS_ENV).ok(),
            ),
            max_keys: parse_tip_history_drain_max_keys(
                std::env::var(TIP_HISTORY_DRAIN_MAX_KEYS_ENV).ok(),
            ),
        }
    }
}

/// `LASTDB_TIP_HISTORY_DRAIN_MS` → interval; unset / empty / `0` → off.
#[must_use]
pub fn parse_tip_history_drain_interval(raw: Option<String>) -> Option<Duration> {
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
                env = TIP_HISTORY_DRAIN_MS_ENV,
                default_ms = DEFAULT_TIP_HISTORY_DRAIN_MS,
                "invalid LASTDB_TIP_HISTORY_DRAIN_MS; disabling background tip-history drain"
            );
            None
        }
    }
}

/// `LASTDB_TIP_HISTORY_DRAIN_MAX_KEYS` → page size; unset / invalid → default.
#[must_use]
pub fn parse_tip_history_drain_max_keys(raw: Option<String>) -> usize {
    let Some(raw) = raw else {
        return DEFAULT_TIP_HISTORY_DRAIN_MAX_KEYS;
    };
    let s = raw.trim();
    if s.is_empty() {
        return DEFAULT_TIP_HISTORY_DRAIN_MAX_KEYS;
    }
    match s.parse::<usize>() {
        Ok(0) => DEFAULT_TIP_HISTORY_DRAIN_MAX_KEYS,
        Ok(n) => n,
        Err(_) => {
            tracing::warn!(
                raw = %s,
                env = TIP_HISTORY_DRAIN_MAX_KEYS_ENV,
                default = DEFAULT_TIP_HISTORY_DRAIN_MAX_KEYS,
                "invalid LASTDB_TIP_HISTORY_DRAIN_MAX_KEYS; using default"
            );
            DEFAULT_TIP_HISTORY_DRAIN_MAX_KEYS
        }
    }
}

/// Handle for the optional periodic tip-history drain task.
pub struct BackgroundTipHistoryDrainTask {
    join: Mutex<Option<tokio::task::JoinHandle<()>>>,
    stop: Arc<AtomicBool>,
}

impl BackgroundTipHistoryDrainTask {
    /// No-op handle when the background drain is disabled.
    pub fn disabled() -> Self {
        Self {
            join: Mutex::new(None),
            stop: Arc::new(AtomicBool::new(true)),
        }
    }

    /// Spawn a periodic checkpointed tip-history drain on the current Tokio
    /// runtime.
    pub fn spawn(db_ops: Arc<DbOperations>, interval: Duration, max_keys: usize) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_flag = Arc::clone(&stop);
        // lint:spawn-bare-ok process-lifetime reclaim loop — not request-scoped.
        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            // Skip the immediate first tick so boot is not followed by a
            // reclaim scan before ordinary traffic settles.
            ticker.tick().await;
            loop {
                ticker.tick().await;
                if stop_flag.load(Ordering::Relaxed) {
                    break;
                }
                let options = TipHistoryDrainOptions {
                    dry_run: false,
                    max_keys,
                    max_prunes: Some(max_keys),
                    after_key: None,
                    storage_prefix: None,
                };
                match db_ops
                    .atoms()
                    .drain_tip_history_chains_from_checkpoint(options)
                    .await
                {
                    Ok((report, checkpoint))
                        if report.tips_chain_cleared > 0 || report.tips_skipped_changed > 0 =>
                    {
                        tracing::info!(
                            target: "fold_node::database",
                            keys_scanned = report.keys_scanned,
                            tips_with_chain = report.tips_with_chain,
                            tips_chain_cleared = report.tips_chain_cleared,
                            tip_versions_pruned = report.tip_versions_pruned,
                            tips_skipped_changed = report.tips_skipped_changed,
                            more_remaining = report.more_remaining,
                            sweep_complete = checkpoint.sweep_complete,
                            tips_chain_cleared_total = checkpoint.tips_chain_cleared_total,
                            "background tip-history drain pass"
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
                            "background tip-history drain failed"
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
            .expect("background tip-history drain join lock")
            .take()
        {
            handle.abort();
        }
    }

    pub fn is_running(&self) -> bool {
        self.join
            .lock()
            .expect("background tip-history drain join lock")
            .as_ref()
            .is_some_and(|h| !h.is_finished())
    }
}

impl Drop for BackgroundTipHistoryDrainTask {
    fn drop(&mut self) {
        self.stop();
    }
}
