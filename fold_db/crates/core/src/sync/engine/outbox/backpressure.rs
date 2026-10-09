//! Oversize-entry drops and adaptive head deferral for the outbox.

use super::*;

impl SyncEngine {
    pub(super) async fn note_oversize_outbox_drop(
        &self,
        seq: u64,
        size_bytes: usize,
        max_bytes: usize,
        source: &'static str,
    ) {
        self.oversize_outbox_drop_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dropped_at = crate::clock::unix_secs();
        *self.last_oversize_outbox_drop.lock().await = Some(OversizeOutboxDropStatus {
            seq,
            size_bytes,
            max_bytes,
            source: source.to_string(),
            dropped_at,
        });
    }

    /// Fixed absolute byte ceiling for *durable* outbox forget decisions.
    ///
    /// Equals `SyncConfig::max_upload_bytes_per_cycle`, overridable by
    /// `LASTDB_SYNC_MAX_UPLOAD_BYTES` (same fixed knob Fixed-mode policy uses).
    /// Adaptive cycle budget (`UploadPolicySnapshot::max_upload_bytes`) may
    /// shrink under RSS pressure (often to 1 MiB) and is **selection-only** —
    /// it must never permanently `forget_recorded_op` a row that was admitted
    /// under this absolute max (cloud history hole under memory pressure).
    pub(crate) fn absolute_outbox_max_bytes(&self) -> usize {
        env_flag::var_or(
            "LASTDB_SYNC_MAX_UPLOAD_BYTES",
            self.config.max_upload_bytes_per_cycle,
        )
    }

    /// Drop one oversize outbox entry (durable + in-memory) and record the drop.
    ///
    /// Shared by `schedule_outbox_entries` (in-memory queue, raw durable, decoded
    /// durable) and `record_op` so the counter / forget / status path cannot
    /// drift across call sites.
    ///
    /// Callers must only invoke this when `size` exceeds the **absolute**
    /// (`absolute_outbox_max_bytes`) ceiling — never the adaptive cycle budget.
    pub(super) async fn drop_oversize_outbox_entry(
        &self,
        seq: u64,
        size: usize,
        max_bytes: usize,
        source: &'static str,
        log_msg: &str,
    ) -> Result<(), String> {
        tracing::warn!(
            target: "fold_db::sync::memory",
            seq,
            size,
            max_bytes,
            "{log_msg}"
        );
        self.forget_recorded_op(seq).await?;
        self.note_oversize_outbox_drop(seq, size, max_bytes, source)
            .await;
        Ok(())
    }

    /// Defer `head_seq` **and every later seq** out of the in-memory upload
    /// queue (durable rows stay).
    ///
    /// Used when the adaptive cycle budget shrank below an entry's size but the
    /// entry still fits under the absolute forget ceiling — defer, do not
    /// destroy. Dropping the whole tail (not just `head_seq`) is what keeps
    /// FIFO: personal upload keys are the client `entry.seq`, so uploading a
    /// later row while `head_seq` is still durable-only lets a peer's
    /// `seq > cursor` listing skip past the deferred head forever.
    pub(super) async fn defer_from_seq_for_cycle(&self, head_seq: u64) {
        let mut pending = self.pending.lock().await;
        self.defer_from_seq_for_cycle_locked(&mut pending, head_seq);
    }

    /// `defer_from_seq_for_cycle` for a caller that already holds `pending`.
    pub(super) fn defer_from_seq_for_cycle_locked(
        &self,
        pending: &mut Vec<LogEntry>,
        head_seq: u64,
    ) {
        self.adaptive_deferred_head_seq
            .fetch_update(
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
                |current| {
                    Some(if current == 0 {
                        head_seq
                    } else {
                        current.min(head_seq)
                    })
                },
            )
            .ok();
        pending.retain(|entry| entry.seq < head_seq);
    }

    /// Lowest seq currently deferred by the adaptive budget, if any.
    pub(super) fn adaptive_deferred_head(&self) -> Option<u64> {
        match self
            .adaptive_deferred_head_seq
            .load(std::sync::atomic::Ordering::Acquire)
        {
            0 => None,
            seq => Some(seq),
        }
    }

    /// Forget the adaptive defer barrier (start of a cycle, or when the
    /// deferred head itself leaves the durable outbox).
    pub(super) fn clear_adaptive_deferred_head(&self) {
        self.adaptive_deferred_head_seq
            .store(0, std::sync::atomic::Ordering::Release);
    }

    /// Clear the barrier when the row holding it is gone from the durable
    /// outbox (uploaded or absolutely forgotten).
    pub(super) fn clear_adaptive_deferred_head_if_removed(
        &self,
        removed_seqs: impl IntoIterator<Item = u64>,
    ) {
        let Some(head) = self.adaptive_deferred_head() else {
            return;
        };
        if removed_seqs.into_iter().any(|seq| seq == head) {
            self.clear_adaptive_deferred_head();
        }
    }
}
