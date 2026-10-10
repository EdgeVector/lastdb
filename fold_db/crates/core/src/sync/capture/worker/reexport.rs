//! Capture re-export marker plane: staging, clearing, poison handling and
//! the compaction probes that follow them.

use super::compact_policy::{
    CAPTURE_PRESENCE_EMPTY, CAPTURE_PRESENCE_MASK, CAPTURE_PRESENCE_NONEMPTY,
    CAPTURE_REEXPORT_NAMESPACE, CAPTURE_REEXPORT_NONCE,
};
use super::CaptureReexportMarker;
use crate::sync::engine::SyncEngine;
use crate::sync::log::LogOp;
use std::sync::atomic::Ordering;

impl SyncEngine {
    fn mark_capture_reexport_staged(&self) {
        let _ = self.capture_reexport_presence.fetch_update(
            Ordering::AcqRel,
            Ordering::Acquire,
            |old| Some((old.wrapping_add(4) & !CAPTURE_PRESENCE_MASK) | CAPTURE_PRESENCE_NONEMPTY),
        );
    }

    fn mark_capture_reexport_observed(&self) {
        let _ = self.capture_reexport_presence.fetch_update(
            Ordering::AcqRel,
            Ordering::Acquire,
            |old| Some((old & !CAPTURE_PRESENCE_MASK) | CAPTURE_PRESENCE_NONEMPTY),
        );
    }

    pub(crate) fn capture_reexport_pending_known_nonempty(&self) -> Option<bool> {
        match self.capture_reexport_presence.load(Ordering::Acquire) & CAPTURE_PRESENCE_MASK {
            CAPTURE_PRESENCE_NONEMPTY => Some(true),
            CAPTURE_PRESENCE_EMPTY => Some(false),
            _ => None,
        }
    }

    pub(super) async fn note_capture_reexport_scan_page(&self, rows_seen: bool, finished: bool) {
        if rows_seen {
            *self.capture_reexport_scan_lap_saw_rows.lock().await = true;
            self.mark_capture_reexport_observed();
        }
        if finished && !*self.capture_reexport_scan_lap_saw_rows.lock().await {
            let expected = *self.capture_reexport_scan_lap_version.lock().await;
            let empty = (expected & !CAPTURE_PRESENCE_MASK) | CAPTURE_PRESENCE_EMPTY;
            if self
                .capture_reexport_presence
                .compare_exchange(expected, empty, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                self.capture_reexport_pending_count
                    .store(0, Ordering::Relaxed);
            }
        }
    }

    pub(crate) async fn stage_mutation_intent_reexport(
        &self,
        envelopes: &[crate::sync::log::MutationEnvelope],
    ) -> Result<Vec<u8>, String> {
        if envelopes.is_empty() {
            return Ok(Vec::new());
        }
        let now = crate::clock::unix_nanos_wide();
        let nonce = CAPTURE_REEXPORT_NONCE.fetch_add(1, Ordering::Relaxed);
        let marker_key = format!("{:010}-{now:039}-{nonce:020}", std::process::id()).into_bytes();
        let mut mutation_intent = envelopes.to_vec();
        for envelope in &mut mutation_intent {
            envelope.strip_sot_field_values();
        }
        let marker = CaptureReexportMarker {
            namespace: "mutation_intent".to_string(),
            keys: Vec::new(),
            mutation_intent: Some(mutation_intent),
        };
        let marker_bytes = serde_json::to_vec(&marker)
            .map_err(|e| format!("encode mutation-intent re-export marker: {e}"))?;
        let store = self
            .store
            .open_namespace(CAPTURE_REEXPORT_NAMESPACE)
            .await
            .map_err(|e| format!("open capture re-export queue: {e}"))?;
        // Invalidate a concurrent empty scan before the durable put begins.
        // A failed put may leave a conservative true until the next empty lap.
        self.mark_capture_reexport_staged();
        store
            .put(&marker_key, marker_bytes)
            .await
            .map_err(|e| format!("persist mutation-intent re-export marker: {e}"))?;
        store
            .flush()
            .await
            .map_err(|e| format!("flush mutation-intent re-export marker: {e}"))?;
        self.capture_reexport_pending_count
            .fetch_add(1, Ordering::Relaxed);
        Ok(marker_key)
    }

    pub(crate) async fn stage_capture_reexport(
        &self,
        namespace: &str,
        keys: &[Vec<u8>],
    ) -> Result<Vec<u8>, String> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let now = crate::clock::unix_nanos_wide();
        let nonce = CAPTURE_REEXPORT_NONCE.fetch_add(1, Ordering::Relaxed);
        let marker_key = format!("{:010}-{now:039}-{nonce:020}", std::process::id()).into_bytes();
        let marker = CaptureReexportMarker {
            namespace: namespace.to_string(),
            keys: keys.iter().map(|key| LogOp::encode_bytes(key)).collect(),
            mutation_intent: None,
        };
        let marker_bytes = serde_json::to_vec(&marker)
            .map_err(|e| format!("encode capture re-export marker: {e}"))?;
        let store = self
            .store
            .open_namespace(CAPTURE_REEXPORT_NAMESPACE)
            .await
            .map_err(|e| format!("open capture re-export queue: {e}"))?;
        self.mark_capture_reexport_staged();
        let result = async {
            store
                .put(&marker_key, marker_bytes)
                .await
                .map_err(|e| format!("persist capture re-export marker: {e}"))?;
            store
                .flush()
                .await
                .map_err(|e| format!("flush capture re-export marker: {e}"))
        }
        .await;
        match result {
            Ok(()) => {
                self.capture_reexport_pending_count
                    .fetch_add(1, Ordering::Relaxed);
                Ok(marker_key)
            }
            Err(error) => {
                self.record_capture_reexport_failure(&error).await;
                Err(error)
            }
        }
    }

    pub(crate) async fn clear_capture_reexport(&self, marker_key: &[u8]) -> Result<(), String> {
        if marker_key.is_empty() {
            return Ok(());
        }
        let store = self
            .store
            .open_namespace(CAPTURE_REEXPORT_NAMESPACE)
            .await
            .map_err(|e| format!("open capture re-export queue: {e}"))?;
        store
            .delete(marker_key)
            .await
            .map_err(|e| format!("delete capture re-export marker: {e}"))?;
        store
            .flush()
            .await
            .map_err(|e| format!("flush capture re-export marker delete: {e}"))?;
        if let Err(error) = self
            .pin_log
            .retire_capture_marker_receipts(&[marker_key.to_vec()])
            .await
        {
            tracing::warn!(%error, "capture marker receipt retirement deferred");
        }
        let _ = self.capture_reexport_pending_count.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |count| Some(count.saturating_sub(1)),
        );
        Ok(())
    }

    /// Every automatic plane sweep runs on [`super::PlaneCompactor`], which
    /// shares this engine's packing lock and cloud-pause slot. A node with no
    /// engine steps the identical code from
    /// [`crate::fold_db_core::sync_coordinator::SyncCoordinator`].
    pub(crate) async fn maybe_compact_capture_reexport_plane(&self) {
        self.compaction.maybe_compact_capture_reexport_plane().await;
    }

    pub(crate) async fn maybe_compact_locator_plane(&self) {
        self.compaction.maybe_compact_locator_plane().await;
    }

    pub(crate) async fn maybe_compact_residual_capture_free_planes(&self) {
        self.compaction
            .maybe_compact_residual_capture_free_planes()
            .await;
    }

    pub(crate) async fn maybe_photograph_aligned_compact_if_dirty(&self) {
        self.compaction
            .maybe_photograph_aligned_compact_if_dirty()
            .await;
    }

    pub(crate) async fn maybe_compact_large_captured_planes(&self) {
        self.compaction.maybe_compact_large_captured_planes().await;
    }

    pub(crate) async fn maybe_compact_tips_plane(&self) {
        self.compaction.maybe_compact_tips_plane().await;
    }

    pub(crate) async fn record_capture_reexport_failure(&self, error: &str) {
        self.capture_reexport_failure_count
            .fetch_add(1, Ordering::Relaxed);
        *self.last_capture_reexport_error.lock().await = Some(error.to_string());
    }

    /// Record one deterministically undecodable marker before dropping it.
    ///
    /// Retrying is pointless — the same bytes decode the same way every tick —
    /// and keeping the marker pins a slot in the fixed-size drain page, which
    /// is how a handful of poison rows wedged the whole queue. Dropping loses
    /// the dirty keys the marker named, so the loss is logged with the marker
    /// key and a bounded preview of the raw bytes, counted on status, and left
    /// in `last_capture_reexport_error` for the operator to find.
    pub(super) async fn record_capture_reexport_poison(
        &self,
        marker_key: &[u8],
        bytes: &[u8],
        error: &str,
    ) {
        const PREVIEW_BYTES: usize = 160;
        self.capture_reexport_poison_dropped_count
            .fetch_add(1, Ordering::Relaxed);
        let preview: String = String::from_utf8_lossy(&bytes[..bytes.len().min(PREVIEW_BYTES)])
            .chars()
            .map(|c| if c.is_control() { '.' } else { c })
            .collect();
        tracing::error!(
            "dropping undecodable capture re-export marker key={} len={} preview={preview:?}: {error}",
            String::from_utf8_lossy(marker_key),
            bytes.len(),
        );
        self.record_capture_reexport_failure(error).await;
    }
}
