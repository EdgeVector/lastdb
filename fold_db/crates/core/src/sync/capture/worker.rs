//! Store-level capture surface after store-diff cold capture retirement.
//!
//! Write-path CDC watermark / store-diff cold capture is gone. Continuous
//! export uses [`crate::sync::engine::CaptureMode::MutationLog`] (pin-log).
//! Sealed-chunk backup / snapshot / outbox overflow heal remain.
//!
//! These methods stay as thin no-ops or backup helpers so callers (backup,
//! compact, outbox, replay absorb) do not need a parallel delete pass.

use crate::sync::engine::{SyncEngine, SyncState, CAPTURE_MARKER_BATCH_MAX_ENVELOPE_BYTES};
use crate::sync::log::LogOp;
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

    async fn run_capture_tick_inner(
        &self,
        probe_compaction: bool,
    ) -> Result<CaptureTickStats, String> {
        let _drain = self.capture_reexport_drain_lock.lock().await;
        let started = Instant::now();
        let marker_store = self
            .store
            .open_namespace(CAPTURE_REEXPORT_NAMESPACE)
            .await
            .map_err(|e| format!("open capture re-export queue: {e}"))?;
        // Probe before the early return. The marker plane can hold dead
        // records after the live backlog reaches zero. A sync cycle probes
        // outside this method, so its marker-only call skips these probes.
        if probe_compaction {
            self.maybe_compact_capture_reexport_plane().await;
            self.maybe_compact_locator_plane().await;
            self.maybe_compact_tips_plane().await;
            self.maybe_compact_large_captured_planes().await;
            self.maybe_compact_residual_capture_free_planes().await;
        }

        // One physical handle per page makes every row share `row_handle`.
        // That lets a time-limited pass resume after the last examined key,
        // even when a failed marker remains in the same handle.
        let cursor = self.capture_reexport_scan_cursor.lock().await.clone();
        if cursor.is_none() {
            *self.capture_reexport_scan_lap_version.lock().await =
                self.capture_reexport_presence.load(Ordering::Acquire);
            *self.capture_reexport_scan_lap_saw_rows.lock().await = false;
        }
        let page = marker_store
            .scan_range_physical_paged(
                b"",
                &[0xff, 0xff, 0xff, 0xff],
                cursor.as_ref(),
                CAPTURE_REEXPORT_PAGE_SIZE,
                1,
            )
            .await
            .map_err(|e| format!("read capture re-export queue: {e}"))?;
        let rows = page.rows;
        if !rows.is_empty() {
            // A process restart resets the counter to zero. A physical page
            // proves only a lower bound. A continuation proves at least one
            // more row or handle. Do not inflate an exact single-row count.
            self.capture_reexport_pending_count.fetch_max(
                rows.len() as u64 + u64::from(page.next_cursor.is_some()),
                Ordering::Relaxed,
            );
        }
        if rows.is_empty() {
            let scan_more = page.next_cursor.is_some();
            *self.capture_reexport_scan_cursor.lock().await = page.next_cursor;
            self.note_capture_reexport_scan_page(false, !scan_more)
                .await;
            return Ok(CaptureTickStats {
                scan_more,
                ..CaptureTickStats::default()
            });
        }

        let mut stats = CaptureTickStats::default();
        let mut cleared = Vec::new();
        // Undecodable markers, dropped with the drained ones in the same batch
        // delete. Kept separate from `cleared` so the tick can tell real drain
        // progress from a poison eviction when it decides whether to clear the
        // unrecovered-error latch.
        let mut poison = Vec::new();
        let mut poison_error: Option<String> = None;
        let mut namespaces = std::collections::HashSet::new();
        let mut intent_markers = Vec::new();
        let mut intent_batch_bytes = 0usize;
        let mut last_examined = None;
        let mut examined_count = 0usize;
        let mut budget_exhausted = false;
        for (marker_key, marker_bytes) in rows {
            if started.elapsed() >= CAPTURE_REEXPORT_TICK_BUDGET && last_examined.is_some() {
                budget_exhausted = true;
                break;
            }
            last_examined = Some(marker_key.clone());
            examined_count += 1;
            let marker: CaptureReexportMarker = match serde_json::from_slice(&marker_bytes) {
                Ok(marker) => marker,
                Err(error) => {
                    let error = format!("decode capture re-export marker: {error}");
                    self.record_capture_reexport_poison(&marker_key, &marker_bytes, &error)
                        .await;
                    stats.poison_dropped += 1;
                    poison.push(marker_key);
                    poison_error = Some(error);
                    continue;
                }
            };
            if let Some(envelopes) = marker.mutation_intent {
                let envelope_bytes = match serde_json::to_vec(&envelopes) {
                    Ok(bytes) => bytes.len(),
                    Err(error) => {
                        self.record_capture_reexport_failure(&format!(
                            "encode capture marker envelope: {error}"
                        ))
                        .await;
                        stats.skipped_pending += 1;
                        continue;
                    }
                };
                if envelope_bytes > CAPTURE_MARKER_BATCH_MAX_ENVELOPE_BYTES {
                    self.record_capture_reexport_failure(
                        "capture marker envelope exceeds the bounded batch byte cap",
                    )
                    .await;
                    stats.skipped_pending += 1;
                    continue;
                }
                if intent_markers.len() >= CAPTURE_REEXPORT_BATCH_SIZE
                    || intent_batch_bytes.saturating_add(envelope_bytes)
                        > CAPTURE_MARKER_BATCH_MAX_ENVELOPE_BYTES
                {
                    self.flush_capture_intent_markers(
                        &mut intent_markers,
                        &mut cleared,
                        &mut stats,
                        &mut namespaces,
                    )
                    .await;
                    intent_batch_bytes = 0;
                }
                intent_batch_bytes += envelope_bytes;
                intent_markers.push((marker_key, envelopes));
                if intent_markers.len() >= CAPTURE_REEXPORT_BATCH_SIZE {
                    self.flush_capture_intent_markers(
                        &mut intent_markers,
                        &mut cleared,
                        &mut stats,
                        &mut namespaces,
                    )
                    .await;
                    intent_batch_bytes = 0;
                }
                continue;
            }
            self.flush_capture_intent_markers(
                &mut intent_markers,
                &mut cleared,
                &mut stats,
                &mut namespaces,
            )
            .await;
            intent_batch_bytes = 0;
            let keys: Vec<Vec<u8>> = match marker
                .keys
                .iter()
                .map(|key| LogOp::decode_bytes(key).map_err(|e| e.to_string()))
                .collect()
            {
                Ok(keys) => keys,
                Err(error) => {
                    // The marker parsed, but its encoded key list did not. That
                    // is the same deterministic poison as an undecodable body:
                    // no retry can turn these bytes into keys.
                    let error = format!("decode capture re-export marker keys: {error}");
                    self.record_capture_reexport_poison(&marker_key, &marker_bytes, &error)
                        .await;
                    stats.poison_dropped += 1;
                    poison.push(marker_key);
                    poison_error = Some(error);
                    continue;
                }
            };
            let source = match self.store.open_namespace(&marker.namespace).await {
                Ok(source) => source,
                Err(error) => {
                    let error = format!("open dirty namespace '{}': {error}", marker.namespace);
                    self.record_capture_reexport_failure(&error).await;
                    stats.skipped_pending += 1;
                    continue;
                }
            };
            let values = match source.get_many(keys.clone()).await {
                Ok(values) => values,
                Err(error) => {
                    let error = format!("read dirty keys from '{}': {error}", marker.namespace);
                    self.record_capture_reexport_failure(&error).await;
                    stats.skipped_pending += 1;
                    continue;
                }
            };
            let mut puts = Vec::new();
            let mut deletes = Vec::new();
            for (key, value) in keys.into_iter().zip(values) {
                match value {
                    Some(value) => puts.push((key, value)),
                    None => deletes.push(key),
                }
            }
            let recovered = async {
                if !puts.is_empty() {
                    let digests: Vec<(Vec<u8>, [u8; 32])> = puts
                        .iter()
                        .map(|(key, value)| {
                            (key.clone(), {
                                use sha2::{Digest, Sha256};
                                Sha256::digest(value).into()
                            })
                        })
                        .collect();
                    self.record_physical_digest(&marker.namespace, &digests)
                        .await?;
                }
                if !deletes.is_empty() {
                    self.record_batch_delete(&marker.namespace, &deletes)
                        .await?;
                }
                Ok::<(), String>(())
            }
            .await;
            match recovered {
                Ok(()) => {
                    stats.puts_staged += puts.len();
                    stats.deletes_staged += deletes.len();
                    namespaces.insert(marker.namespace);
                    cleared.push(marker_key);
                }
                Err(error) => {
                    self.record_capture_reexport_failure(&error).await;
                    stats.skipped_pending += 1;
                }
            }
        }
        self.flush_capture_intent_markers(
            &mut intent_markers,
            &mut cleared,
            &mut stats,
            &mut namespaces,
        )
        .await;
        // Drained and poison markers leave the plane together: both are done
        // with, and evicting the poison in the same batch is what stops it
        // from occupying the head of every future page.
        let drained = cleared.len() as u64;
        let evicted = drained + poison.len() as u64;
        if evicted > 0 {
            let mut removed = cleared;
            removed.append(&mut poison);
            marker_store
                .batch_delete(removed.clone())
                .await
                .map_err(|e| format!("delete drained capture re-export markers: {e}"))?;
            marker_store
                .flush()
                .await
                .map_err(|e| format!("flush drained capture re-export markers: {e}"))?;
            if let Err(error) = self.pin_log.retire_capture_marker_receipts(&removed).await {
                tracing::warn!(%error, "capture marker receipt retirement deferred");
            }
            let _ = self.capture_reexport_pending_count.fetch_update(
                Ordering::Relaxed,
                Ordering::Relaxed,
                |count| Some(count.saturating_sub(evicted)),
            );
        }
        if drained > 0 && stats.skipped_pending == 0 {
            // Steady drain progress clears the unrecovered-error latch so a
            // slow residual reexport under interactive throttle does not keep
            // sync_degraded latched for hours after real recovery starts. A
            // failed marker in the same page still needs its error on status.
            *self.last_capture_reexport_error.lock().await = None;
        }
        if let Some(error) = poison_error {
            // Dropping a marker is data loss, so it outranks the progress latch
            // clear above: a tick that drained 200 good markers and threw away
            // one unreadable marker must still report the unreadable one.
            *self.last_capture_reexport_error.lock().await = Some(error);
        }
        let next_cursor = if budget_exhausted {
            let mut position = page.row_handle.ok_or_else(|| {
                "capture re-export physical page returned rows without a row handle".to_string()
            })?;
            position.after_key = last_examined;
            Some(position)
        } else {
            page.next_cursor
        };
        let scan_more = next_cursor.is_some();
        *self.capture_reexport_scan_cursor.lock().await = next_cursor;
        self.note_capture_reexport_scan_page(true, !scan_more).await;
        stats.markers_examined = examined_count;
        stats.scan_more = scan_more;
        stats.staging_budget_exhausted = budget_exhausted;
        stats.namespaces_scanned = namespaces.len();
        Ok(stats)
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
pub(crate) use compact_policy::*;
pub(crate) use compact_triggers::*;
