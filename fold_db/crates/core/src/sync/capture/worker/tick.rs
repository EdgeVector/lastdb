//! One bounded drain tick over the capture re-export marker plane.

use super::compact_policy::{
    CAPTURE_REEXPORT_BATCH_SIZE, CAPTURE_REEXPORT_NAMESPACE, CAPTURE_REEXPORT_PAGE_SIZE,
    CAPTURE_REEXPORT_TICK_BUDGET,
};
use super::{CaptureReexportMarker, CaptureTickStats};
use crate::storage::traits::KvStore;
use crate::sync::engine::{SyncEngine, CAPTURE_MARKER_BATCH_MAX_ENVELOPE_BYTES};
use crate::sync::log::LogOp;
use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

/// Per-tick accumulators shared by the marker handlers.
#[derive(Default)]
struct TickAccum {
    stats: CaptureTickStats,
    cleared: Vec<Vec<u8>>,
    /// Undecodable markers, dropped with the drained ones in the same batch
    /// delete. Kept separate from `cleared` so the tick can tell real drain
    /// progress from a poison eviction when it decides whether to clear the
    /// unrecovered-error latch.
    poison: Vec<Vec<u8>>,
    poison_error: Option<String>,
    namespaces: HashSet<String>,
    intent_markers: Vec<(Vec<u8>, Vec<crate::sync::log::MutationEnvelope>)>,
    intent_batch_bytes: usize,
}

impl SyncEngine {
    pub(super) async fn run_capture_tick_inner(
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

        let mut acc = TickAccum::default();
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
            self.drain_one_marker(marker_key, marker_bytes, &mut acc)
                .await;
        }
        self.flush_intents(&mut acc).await;
        self.retire_drained_markers(&marker_store, &mut acc).await?;
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
        let mut stats = acc.stats;
        stats.markers_examined = examined_count;
        stats.scan_more = scan_more;
        stats.staging_budget_exhausted = budget_exhausted;
        stats.namespaces_scanned = acc.namespaces.len();
        Ok(stats)
    }

    async fn flush_intents(&self, acc: &mut TickAccum) {
        self.flush_capture_intent_markers(
            &mut acc.intent_markers,
            &mut acc.cleared,
            &mut acc.stats,
            &mut acc.namespaces,
        )
        .await;
        acc.intent_batch_bytes = 0;
    }

    /// Record an undecodable marker and queue it for eviction with the drained
    /// ones.
    async fn poison_marker(
        &self,
        marker_key: Vec<u8>,
        marker_bytes: &[u8],
        error: String,
        acc: &mut TickAccum,
    ) {
        self.record_capture_reexport_poison(&marker_key, marker_bytes, &error)
            .await;
        acc.stats.poison_dropped += 1;
        acc.poison.push(marker_key);
        acc.poison_error = Some(error);
    }

    async fn drain_one_marker(
        &self,
        marker_key: Vec<u8>,
        marker_bytes: Vec<u8>,
        acc: &mut TickAccum,
    ) {
        let marker: CaptureReexportMarker = match serde_json::from_slice(&marker_bytes) {
            Ok(marker) => marker,
            Err(error) => {
                let error = format!("decode capture re-export marker: {error}");
                self.poison_marker(marker_key, &marker_bytes, error, acc)
                    .await;
                return;
            }
        };
        let CaptureReexportMarker {
            namespace,
            keys,
            mutation_intent,
        } = marker;
        if let Some(envelopes) = mutation_intent {
            self.queue_intent_marker(marker_key, envelopes, acc).await;
            return;
        }
        self.flush_intents(acc).await;
        self.recover_plain_marker(marker_key, &marker_bytes, namespace, keys, acc)
            .await;
    }

    async fn queue_intent_marker(
        &self,
        marker_key: Vec<u8>,
        envelopes: Vec<crate::sync::log::MutationEnvelope>,
        acc: &mut TickAccum,
    ) {
        let envelope_bytes = match serde_json::to_vec(&envelopes) {
            Ok(bytes) => bytes.len(),
            Err(error) => {
                self.record_capture_reexport_failure(&format!(
                    "encode capture marker envelope: {error}"
                ))
                .await;
                acc.stats.skipped_pending += 1;
                return;
            }
        };
        if envelope_bytes > CAPTURE_MARKER_BATCH_MAX_ENVELOPE_BYTES {
            self.record_capture_reexport_failure(
                "capture marker envelope exceeds the bounded batch byte cap",
            )
            .await;
            acc.stats.skipped_pending += 1;
            return;
        }
        if acc.intent_markers.len() >= CAPTURE_REEXPORT_BATCH_SIZE
            || acc.intent_batch_bytes.saturating_add(envelope_bytes)
                > CAPTURE_MARKER_BATCH_MAX_ENVELOPE_BYTES
        {
            self.flush_intents(acc).await;
        }
        acc.intent_batch_bytes += envelope_bytes;
        acc.intent_markers.push((marker_key, envelopes));
        if acc.intent_markers.len() >= CAPTURE_REEXPORT_BATCH_SIZE {
            self.flush_intents(acc).await;
        }
    }

    async fn recover_plain_marker(
        &self,
        marker_key: Vec<u8>,
        marker_bytes: &[u8],
        namespace: String,
        encoded_keys: Vec<String>,
        acc: &mut TickAccum,
    ) {
        let keys: Vec<Vec<u8>> = match encoded_keys
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
                self.poison_marker(marker_key, marker_bytes, error, acc)
                    .await;
                return;
            }
        };
        let source = match self.store.open_namespace(&namespace).await {
            Ok(source) => source,
            Err(error) => {
                let error = format!("open dirty namespace '{namespace}': {error}");
                self.record_capture_reexport_failure(&error).await;
                acc.stats.skipped_pending += 1;
                return;
            }
        };
        let values = match source.get_many(keys.clone()).await {
            Ok(values) => values,
            Err(error) => {
                let error = format!("read dirty keys from '{namespace}': {error}");
                self.record_capture_reexport_failure(&error).await;
                acc.stats.skipped_pending += 1;
                return;
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
                self.record_physical_digest(&namespace, &digests).await?;
            }
            if !deletes.is_empty() {
                self.record_batch_delete(&namespace, &deletes).await?;
            }
            Ok::<(), String>(())
        }
        .await;
        match recovered {
            Ok(()) => {
                acc.stats.puts_staged += puts.len();
                acc.stats.deletes_staged += deletes.len();
                acc.namespaces.insert(namespace);
                acc.cleared.push(marker_key);
            }
            Err(error) => {
                self.record_capture_reexport_failure(&error).await;
                acc.stats.skipped_pending += 1;
            }
        }
    }

    /// Remove drained and poison markers from the plane, then settle the
    /// unrecovered-error latch.
    async fn retire_drained_markers(
        &self,
        marker_store: &Arc<dyn KvStore>,
        acc: &mut TickAccum,
    ) -> Result<(), String> {
        // Drained and poison markers leave the plane together: both are done
        // with, and evicting the poison in the same batch is what stops it
        // from occupying the head of every future page.
        let drained = acc.cleared.len() as u64;
        let evicted = drained + acc.poison.len() as u64;
        if evicted > 0 {
            let mut removed = std::mem::take(&mut acc.cleared);
            removed.append(&mut acc.poison);
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
        if drained > 0 && acc.stats.skipped_pending == 0 {
            // Steady drain progress clears the unrecovered-error latch so a
            // slow residual reexport under interactive throttle does not keep
            // sync_degraded latched for hours after real recovery starts. A
            // failed marker in the same page still needs its error on status.
            *self.last_capture_reexport_error.lock().await = None;
        }
        if let Some(error) = acc.poison_error.take() {
            // Dropping a marker is data loss, so it outranks the progress latch
            // clear above: a tick that drained 200 good markers and threw away
            // one unreadable marker must still report the unreadable one.
            *self.last_capture_reexport_error.lock().await = Some(error);
        }
        Ok(())
    }
}
