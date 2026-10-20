//! Recording durable mutation-intent markers into the mutation log.

use super::*;

impl SyncEngine {
    /// Move a bounded page of durable Async markers into the mutation log.
    /// The pin-log writer flushes an allocation floor, marker receipts, and
    /// target rows in that order. A retry after marker-delete failure reuses
    /// the receipt's positions instead of minting duplicate cloud operations.
    pub(crate) async fn record_mutation_intent_marker_batch(
        &self,
        markers: &[(Vec<u8>, Vec<crate::sync::log::MutationEnvelope>)],
    ) -> Result<Vec<Vec<u8>>, String> {
        if markers.is_empty() {
            return Ok(Vec::new());
        }
        Self::validate_marker_batch(markers)?;
        if !matches!(self.config.capture_mode, CaptureMode::MutationLog)
            || !self.should_stage_cloud_mutations().await
        {
            return Err("continuous cloud capture is unavailable; durable marker retained".into());
        }

        // The destination set cannot change between frontier allocation and
        // the durable rows/receipts. This is the same lock order as record_op.
        let _target_config = self.target_config_lock.lock().await;
        let targets = self.targets.lock().await.clone();
        let partitioner = self.partitioner.lock().await.clone();
        let generation = self
            .target_config_generation
            .load(std::sync::atomic::Ordering::Acquire);
        let marker_keys = markers
            .iter()
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        let existing_receipts = self
            .pin_log
            .capture_marker_receipt_presence(&marker_keys)
            .await?;
        let mut entries = Vec::with_capacity(markers.len());
        for ((marker_key, envelopes), already_recorded) in markers.iter().zip(existing_receipts) {
            let op = crate::sync::mutation_intent::mutation_intent_op(envelopes.clone());
            let mut entry = if already_recorded {
                // The pin-log method validates the full receipt and repairs
                // its original rows. These unused coordinates do not advance
                // the process sequence or a durable allocation floor.
                LogEntry {
                    seq: 0,
                    timestamp_ms: 0,
                    device_id: self.device_id.clone(),
                    op,
                }
            } else {
                self.make_entry_for_target_snapshot(op, generation, &targets)
                    .await?
            };
            entry.op.strip_sot_field_values();
            entries.push((marker_key.clone(), entry));
        }
        let cleared = self
            .pin_log
            .append_capture_marker_batch(&entries, &targets, &partitioner)
            .await?;
        self.set_state(SyncState::Dirty, None).await;
        // Product capture is async: the marker drain is the append the
        // coalesce hold must see. Stamp before the wake so that wake does
        // not read "no append in this process" and seal at once.
        self.note_mutation_log_local_append();
        self.wake.notify_one();
        Ok(cleared)
    }

    fn validate_marker_batch(
        markers: &[(Vec<u8>, Vec<crate::sync::log::MutationEnvelope>)],
    ) -> Result<(), String> {
        const MAX_MARKERS: usize = 256;
        if markers.len() > MAX_MARKERS {
            return Err(format!(
                "capture marker batch exceeds {MAX_MARKERS} markers"
            ));
        }
        let mut total_bytes = 0usize;
        for (marker_key, envelopes) in markers {
            if marker_key.is_empty() || envelopes.is_empty() {
                return Err("capture marker batch has an empty key or mutation intent".into());
            }
            let bytes = serde_json::to_vec(envelopes)
                .map_err(|error| format!("encode capture marker envelope: {error}"))?;
            total_bytes = total_bytes.saturating_add(bytes.len());
            if total_bytes > CAPTURE_MARKER_BATCH_MAX_ENVELOPE_BYTES {
                return Err(format!(
                    "capture marker batch exceeds {CAPTURE_MARKER_BATCH_MAX_ENVELOPE_BYTES} envelope bytes"
                ));
            }
        }
        Ok(())
    }

    /// The direct publication path uses the same marker receipt as the
    /// background drain. The caller holds the drain lock through marker
    /// deletion, so the receipt remains available for this exact read.
    pub(crate) async fn record_mutation_intent_marker_with_publication(
        &self,
        marker_key: Vec<u8>,
        envelopes: Vec<crate::sync::log::MutationEnvelope>,
    ) -> Result<super::super::pin_log::MutationLogAppendReceipt, String> {
        self.record_mutation_intent_marker_batch(&[(marker_key.clone(), envelopes)])
            .await?;
        self.pin_log
            .capture_marker_append_receipt(&marker_key)
            .await
    }
}
