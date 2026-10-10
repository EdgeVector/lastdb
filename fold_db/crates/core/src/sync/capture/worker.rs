//! Store-level capture surface after store-diff cold capture retirement.
//!
//! Write-path CDC watermark / store-diff cold capture is gone. Continuous
//! export uses [`crate::sync::engine::CaptureMode::MutationLog`] (pin-log).
//! Sealed-chunk backup / snapshot / outbox overflow heal remain.
//!
//! These methods stay as thin no-ops or backup helpers so callers (backup,
//! compact, outbox, replay absorb) do not need a parallel delete pass.

use crate::sync::engine::{SyncEngine, SyncState};
use crate::sync::snapshot::Snapshot;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// A crash-safe dirty-key intent. It deliberately stores keys rather than the
/// attempted value: after a crash the drain reads the serving store's current
/// truth, so an intent persisted just before a local write is safe whether the
/// process died before or after that write committed.
#[derive(Debug, Serialize, Deserialize)]
struct CaptureReexportMarker {
    #[serde(default)]
    namespace: String,
    #[serde(default)]
    keys: Vec<String>,
    /// Serialized MutationIntent envelopes. When present, drain appends
    /// that op instead of re-reading physical KV keys.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    mutation_intent: Option<Vec<crate::sync::log::MutationEnvelope>>,
}

#[derive(Debug, Default, Clone)]
pub struct CaptureTickStats {
    /// Number of marker rows examined in this bounded physical page.
    pub markers_examined: usize,
    /// A physical cursor remains after this page.
    pub scan_more: bool,
    pub namespaces_scanned: usize,
    pub puts_staged: usize,
    pub deletes_staged: usize,
    pub skipped_pending: usize,
    /// Undecodable markers dropped from the plane by this tick.
    ///
    /// Counted separately from `skipped_pending`: a skipped marker is still
    /// queued and will be retried, a dropped one is gone for good.
    pub poison_dropped: usize,
    pub staging_budget_exhausted: bool,
    pub outbox_overflow_paused: bool,
    /// Capture skipped because intentional Cloud Sync off exceeded grace.
    pub sync_off_stop_staging: bool,
}

impl SyncEngine {
    /// Drain only the bounded durable dirty-key queue. The retired cold
    /// store-diff scan stays retired: recovery performs point reads for keys
    /// whose write-path capture did not reach the pin log.
    pub async fn run_capture_tick(&self) -> Result<CaptureTickStats, String> {
        self.run_capture_tick_inner(true).await
    }

    /// Step only the marker plane during a long sync cycle or local drain.
    /// The outer cycle already runs all automatic compaction probes.
    pub(crate) async fn run_capture_marker_tick(&self) -> Result<CaptureTickStats, String> {
        self.run_capture_tick_inner(false).await
    }

    async fn flush_capture_intent_markers(
        &self,
        pending: &mut Vec<(Vec<u8>, Vec<crate::sync::log::MutationEnvelope>)>,
        cleared: &mut Vec<Vec<u8>>,
        stats: &mut CaptureTickStats,
        namespaces: &mut std::collections::HashSet<String>,
    ) {
        if pending.is_empty() {
            return;
        }
        let batch = std::mem::take(pending);
        if self.config.legacy_personal_cloud_sync {
            // The legacy personal outbox needs its existing per-entry durable
            // append. The marker batch only writes the continuous pin log.
            for (marker_key, envelopes) in batch {
                match self
                    .record_op_with_publication(crate::sync::mutation_intent::mutation_intent_op(
                        envelopes,
                    ))
                    .await
                {
                    Ok(receipt) if receipt.durable_capture_written => {
                        stats.puts_staged += 1;
                        namespaces.insert("mutation_intent".to_string());
                        cleared.push(marker_key);
                    }
                    Ok(_) => {
                        self.record_capture_reexport_failure(
                            "legacy capture marker reached no durable target",
                        )
                        .await;
                        stats.skipped_pending += 1;
                    }
                    Err(error) => {
                        self.record_capture_reexport_failure(&error).await;
                        stats.skipped_pending += 1;
                    }
                }
            }
            return;
        }
        // A malformed marker cannot hold the valid neighbors in its batch.
        // Limit split attempts so a plane-wide I/O error does not multiply
        // work without a bound. The scan cursor still advances past failures.
        let mut work = vec![batch];
        let mut attempts = 0usize;
        let mut last_error = None;
        let skipped_before = stats.skipped_pending;
        const MAX_BATCH_ATTEMPTS: usize = 16;
        while let Some(part) = work.pop() {
            if attempts >= MAX_BATCH_ATTEMPTS {
                stats.skipped_pending += part.len();
                continue;
            }
            attempts += 1;
            match self.record_mutation_intent_marker_batch(&part).await {
                Ok(recorded_keys) => {
                    stats.puts_staged += recorded_keys.len();
                    stats.skipped_pending += part.len().saturating_sub(recorded_keys.len());
                    if recorded_keys.len() != part.len() {
                        last_error = Some(
                            "capture marker batch returned fewer durable receipts than markers"
                                .to_string(),
                        );
                    }
                    if !recorded_keys.is_empty() {
                        namespaces.insert("mutation_intent".to_string());
                        cleared.extend(recorded_keys);
                    }
                }
                Err(error) if part.len() > 1 && attempts < MAX_BATCH_ATTEMPTS => {
                    last_error = Some(error);
                    let middle = part.len() / 2;
                    let mut left = part;
                    let right = left.split_off(middle);
                    work.push(right);
                    work.push(left);
                }
                Err(error) => {
                    last_error = Some(error);
                    stats.skipped_pending += part.len();
                }
            }
        }
        if stats.skipped_pending > skipped_before {
            self.record_capture_reexport_failure(
                last_error
                    .as_deref()
                    .unwrap_or("capture marker batch left durable markers pending"),
            )
            .await;
        }
    }

    /// Post-upload watermark ack — no-op after Watermark retirement.
    #[allow(clippy::unused_async)] // no-op stub; callers keep `.await`
    pub async fn ack_uploaded_capture(
        &self,
        _entries: &[crate::sync::log::LogEntry],
    ) -> Result<(), String> {
        Ok(())
    }

    /// Remote apply absorb into export-baseline watermark — no-op.
    #[allow(clippy::unused_async)] // no-op stub; callers keep `.await`
    pub async fn capture_absorb_put(
        &self,
        _namespace: &str,
        _key: &[u8],
        _value: &[u8],
        _applied: bool,
    ) -> Result<(), String> {
        Ok(())
    }

    /// Remote delete absorb into export-baseline watermark — no-op.
    #[allow(clippy::unused_async)] // no-op stub; callers keep `.await`
    pub async fn capture_absorb_delete(
        &self,
        _namespace: &str,
        _key: &[u8],
        _applied: bool,
    ) -> Result<(), String> {
        Ok(())
    }

    /// Best-effort capture absorb for a put: warn and continue on failure so
    /// replay never aborts solely because watermark bookkeeping failed.
    pub(crate) async fn absorb_put_or_warn(
        &self,
        namespace: &str,
        key: &[u8],
        value: &[u8],
        applied: bool,
        ctx: &str,
    ) {
        if let Err(e) = self
            .capture_absorb_put(namespace, key, value, applied)
            .await
        {
            // Plain `{}` format (not tracing field `{ctx}`) so Mini clippy -D
            // warnings stays clean and the message matches prior call sites.
            tracing::warn!(error = %e, "capture absorb after {} failed", ctx);
        }
    }

    /// Best-effort capture absorb for a delete: warn and continue on failure.
    pub(crate) async fn absorb_delete_or_warn(
        &self,
        namespace: &str,
        key: &[u8],
        applied: bool,
        ctx: &str,
    ) {
        if let Err(e) = self.capture_absorb_delete(namespace, key, applied).await {
            tracing::warn!(error = %e, "capture absorb after {} failed", ctx);
        }
    }

    /// Clear capture pending entries whose staging seq was dropped.
    pub async fn capture_clear_pending_for_seqs(&self, seqs: &[u64]) {
        if seqs.is_empty() {
            return;
        }
        let set: std::collections::HashSet<u64> = seqs.iter().copied().collect();
        let mut pending = self.capture_pending.lock().await;
        pending.retain(|_, p| !set.contains(&p.staging_seq));
    }

    /// Clear residual in-memory capture-pending after a successful snapshot.
    ///
    /// Name retained for call sites in backup/compact; the durable export-
    /// baseline watermark is gone, so this only empties the (always-empty)
    /// pending map for a safe transition.
    pub(crate) async fn capture_reset_watermark_from_snapshot(
        &self,
        _snapshot: &Snapshot,
    ) -> Result<(), String> {
        self.capture_pending.lock().await.clear();
        Ok(())
    }

    /// Minimum spacing between snapshot ATTEMPTS while the outbox stays
    /// escalated (e.g. `backup_snapshot` keeps failing). Prevents a
    /// persistent cloud outage from turning every sync cycle into a full-DB
    /// snapshot attempt, while still retrying — unlike a one-shot-per-process
    /// gate, this is the standing overflow valve and must keep trying as long
    /// as the outbox stays over threshold.
    const OUTBOX_OVERFLOW_RETRY_COOLDOWN_SECS: u64 = 300;

    /// After a sync cycle's upload drain attempt: if durable staging is still
    /// deeper than [`crate::sync::SyncConfig::max_outbox_entries`] or older
    /// than [`crate::sync::SyncConfig::outbox_overflow_max_age_secs`], take a
    /// successful snapshot then clear staging. **Never** clears if the
    /// snapshot fails.
    ///
    /// Local R/W is never gated by this. This is the standing product valve
    /// (2026-07-18: cap/age → snapshot, never drop-oldest or reject) — it can
    /// fire repeatedly over an engine process's lifetime, at most once per
    /// [`Self::OUTBOX_OVERFLOW_RETRY_COOLDOWN_SECS`] so a persistent cloud
    /// failure cannot turn every cycle into a snapshot attempt.
    pub async fn maybe_force_snapshot_for_outbox_overflow(&self) -> Result<(), String> {
        if !self.config.legacy_personal_cloud_sync {
            return Ok(());
        }

        let Some(reason) = self.outbox_overflow_reason().await? else {
            return Ok(());
        };
        let count = self.outbox_count().await.unwrap_or(0);
        if count == 0 {
            return Ok(());
        }

        let now_secs = crate::clock::unix_secs();
        {
            let mut last_attempt = self.outbox_overflow_last_attempt_at.lock().await;
            if let Some(prev) = *last_attempt {
                if now_secs.saturating_sub(prev) < Self::OUTBOX_OVERFLOW_RETRY_COOLDOWN_SECS {
                    let msg = format!(
                        "durable outbox overflow ({reason}, count={count}); last snapshot attempt {}s ago, waiting out retry cooldown",
                        now_secs.saturating_sub(prev)
                    );
                    self.set_state(SyncState::Dirty, Some(&msg)).await;
                    return Ok(());
                }
            }
            *last_attempt = Some(now_secs);
        }

        tracing::warn!(
            reason,
            count,
            "durable sync outbox overflow; forcing snapshot then clearing staging"
        );
        *self.last_outbox_overflow_reason.lock().await = Some(reason.to_string());

        match self.backup_snapshot().await {
            Ok(seq) => {
                // Clear all durable staging — never do this on snapshot failure.
                let dropped = self.drop_oldest_outbox_entries(count).await?;
                {
                    let mut pending = self.pending.lock().await;
                    pending.clear();
                }
                self.capture_pending.lock().await.clear();
                let msg = format!(
                    "outbox overflow ({reason}): snapshot seq={seq} then cleared {dropped} staging entries"
                );
                tracing::info!("{msg}");
                self.set_state(SyncState::Dirty, Some(&msg)).await;
                Ok(())
            }
            Err(e) => {
                // Design: NEVER clear outbox if snapshot failed.
                let remaining = self.outbox_count().await.unwrap_or(count);
                let msg = format!(
                    "outbox overflow ({reason}): backup_snapshot failed; keeping {remaining} staging entries: {e}"
                );
                tracing::error!("{msg}");
                self.set_state(SyncState::Dirty, Some(&msg)).await;
                Err(msg)
            }
        }
    }

    /// **Live** cloud snapshot + staging heal — never requires stopping Mini.
    ///
    /// Product rule: cloud backup must not force an exclusive offline open of
    /// the primary store. This uses the already-open encrypting store handle
    /// (same as compact / do_sync) so local R/W continues while the snapshot
    /// scan runs.
    ///
    /// Steps:
    /// 1. `backup_snapshot` → upload `latest.enc`
    /// 2. Only if that succeeds: clear durable upload staging + in-memory queues
    ///
    /// **Never** clears staging if snapshot fails.
    pub async fn heal_staging_via_snapshot(&self) -> Result<HealStagingReport, String> {
        if !self.config.legacy_personal_cloud_sync {
            return Err(
                "legacy personal snapshot heal is disabled for LastStore backup homes".to_string(),
            );
        }
        let staging_before = self.outbox_count().await.unwrap_or(0);
        tracing::info!(
            staging_before,
            "live heal_staging_via_snapshot: snapshot then clear (writes stay live)"
        );

        // Allow the automatic overflow valve to attempt again immediately
        // instead of waiting out its retry cooldown.
        *self.outbox_overflow_last_attempt_at.lock().await = None;

        let seq = self
            .backup_snapshot()
            .await
            .map_err(|e| format!("backup_snapshot failed (staging NOT cleared): {e}"))?;

        // Re-read count after snapshot (capture may have staged more during scan).
        let to_clear = self.outbox_count().await.unwrap_or(staging_before);
        let dropped = if to_clear > 0 {
            self.drop_oldest_outbox_entries(to_clear).await?
        } else {
            0
        };
        {
            let mut pending = self.pending.lock().await;
            pending.clear();
        }
        self.capture_pending.lock().await.clear();

        let staging_after = self.outbox_count().await.unwrap_or(0);
        let msg = format!(
            "live heal: snapshot seq={seq} cleared {dropped} staging entries; remaining={staging_after}"
        );
        tracing::info!("{msg}");
        self.set_state(SyncState::Idle, Some(&msg)).await;

        Ok(HealStagingReport {
            snapshot_seq: seq,
            staging_before,
            staging_cleared: dropped,
            staging_after,
        })
    }
}

/// Result of [`SyncEngine::heal_staging_via_snapshot`].
#[derive(Debug, Clone)]
pub struct HealStagingReport {
    pub snapshot_seq: u64,
    pub staging_before: usize,
    pub staging_cleared: usize,
    pub staging_after: usize,
}

mod compact_policy;
mod compact_triggers;
mod reexport;
mod tick;
pub(crate) use compact_policy::*;
pub(crate) use compact_triggers::*;
