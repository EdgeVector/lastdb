//! Durable outbox key layout, counts, windows, and removal.

use super::*;

impl SyncEngine {
    pub(crate) fn outbox_key(seq: u64) -> Vec<u8> {
        format!("{SYNC_OUTBOX_ENTRY_PREFIX}{seq:020}").into_bytes()
    }

    pub(crate) async fn outbox_store(
        &self,
    ) -> Result<Arc<dyn crate::storage::traits::KvStore>, String> {
        self.store
            .open_namespace(SYNC_OUTBOX_NAMESPACE)
            .await
            .map_err(|e| format!("failed to open durable sync outbox: {e}"))
    }

    /// Parse the entry seq out of an outbox key (`entry:<seq:020>`).
    pub(crate) fn seq_from_outbox_key(key: &[u8]) -> Option<u64> {
        std::str::from_utf8(key)
            .ok()?
            .strip_prefix(SYNC_OUTBOX_ENTRY_PREFIX)?
            .parse()
            .ok()
    }

    /// Return the outbox metadata guard, seeding it from storage on first use.
    ///
    /// Seeding performs a single key scan (the only `O(backlog)` outbox read on
    /// the hot paths, at most once per process) to establish the entry count and
    /// the highest persisted seq, and advances `self.seq` past that seq so a
    /// post-restart write can never reuse an existing key.
    pub(crate) async fn outbox_meta(
        &self,
    ) -> Result<tokio::sync::MutexGuard<'_, OutboxMeta>, String> {
        let mut meta = self.outbox_meta.lock().await;
        if meta.count.is_none() {
            let store = self.outbox_store().await?;
            // Keys only — never materialise multi-MB LogEntry values for a
            // counter seed (re-enable thrash: full scan_prefix held the whole
            // durable outbox in RAM before any upload cap ran).
            let keys = store
                .scan_prefix_keys(SYNC_OUTBOX_ENTRY_PREFIX.as_bytes())
                .await
                .map_err(|e| format!("failed to seed durable sync outbox counter: {e}"))?;
            let mut max_seq = 0u64;
            for key in &keys {
                if let Some(seq) = Self::seq_from_outbox_key(key) {
                    max_seq = max_seq.max(seq);
                }
            }
            // Keep the seq generator at or above every persisted entry before
            // a snapshot can use it as the cut that authorizes staging clear.
            // Lock order is always outbox_meta -> seq (nothing takes seq then
            // outbox_meta), so this cannot deadlock.
            if max_seq > 0 {
                let mut seq = self.seq.lock().await;
                *seq = (*seq).max(max_seq);
            }
            meta.count = Some(keys.len());
            meta.max_seq = max_seq;
        }
        Ok(meta)
    }

    /// Current durable outbox entry count, read from the in-memory counter
    /// (seeding it once if necessary). `O(1)` in outbox depth after seeding.
    pub(crate) async fn outbox_count(&self) -> Result<usize, String> {
        Ok(self.outbox_meta().await?.count.unwrap_or(0))
    }

    /// Adjust the cached outbox counter by `delta`, saturating at zero, and
    /// raise the tracked `max_seq` to `seq` (0 == "no new seq"). Called after a
    /// successful outbox mutation so the counter tracks the store.
    pub(crate) async fn adjust_outbox_count(&self, delta: i64, seq: u64) -> Result<(), String> {
        let mut meta = self.outbox_meta().await?;
        let current = meta.count.unwrap_or(0) as i64;
        meta.count = Some((current + delta).max(0) as usize);
        meta.max_seq = meta.max_seq.max(seq);
        Ok(())
    }

    /// The oldest outbox entry (smallest seq == first key), or `None` if empty.
    /// Reads a single row so callers can get head metadata (e.g. oldest-entry
    /// age) without materializing the whole outbox.
    pub(crate) async fn outbox_head_entry(&self) -> Result<Option<LogEntry>, String> {
        let store = self.outbox_store().await?;
        let rows = store
            .scan_prefix_paged(SYNC_OUTBOX_ENTRY_PREFIX.as_bytes(), 1)
            .await
            .map_err(|e| format!("failed to read durable sync outbox head: {e}"))?;
        match rows.into_iter().next() {
            Some((key, value)) => {
                let entry: LogEntry = serde_json::from_slice(&value).map_err(|e| {
                    format!(
                        "failed to decode durable sync outbox head {}: {e}",
                        String::from_utf8_lossy(&key)
                    )
                })?;
                Ok(Some(entry))
            }
            None => Ok(None),
        }
    }

    /// Drop the oldest `limit` durable outbox entries (lowest seq). Called
    /// only from the overflow valve (`maybe_force_snapshot_for_outbox_overflow`
    /// / `heal_staging_via_snapshot`), and only after a successful snapshot —
    /// never from the write path. Returns how many entries were removed.
    pub(crate) async fn drop_oldest_outbox_entries(&self, limit: usize) -> Result<usize, String> {
        if limit == 0 {
            return Ok(0);
        }
        let store = self.outbox_store().await?;
        let mut total_removed = 0usize;

        while total_removed < limit {
            let page_limit = (limit - total_removed).min(OUTBOX_DROP_BATCH_SIZE);
            let rows = store
                .scan_prefix_paged(SYNC_OUTBOX_ENTRY_PREFIX.as_bytes(), page_limit)
                .await
                .map_err(|e| format!("failed to scan oldest durable sync outbox entries: {e}"))?;
            if rows.is_empty() {
                break;
            }

            let mut seqs: Vec<u64> = Vec::with_capacity(rows.len());
            let mut keys: Vec<Vec<u8>> = Vec::with_capacity(rows.len());
            for (key, _) in rows {
                // Keep keys/seqs 1:1 so durable counter, pending, and
                // capture-pending markers stay aligned even if a key is
                // unparseable (skip those; leave them for repair rather than
                // shrinking the counter without clearing in-memory state).
                match Self::seq_from_outbox_key(&key) {
                    Some(seq) => {
                        seqs.push(seq);
                        keys.push(key);
                    }
                    None => {
                        tracing::warn!(
                            target: "fold_db::sync::memory",
                            key = %String::from_utf8_lossy(&key),
                            "skipping unparseable durable outbox key in drop_oldest_outbox_entries"
                        );
                    }
                }
            }
            if keys.is_empty() {
                // Avoid infinite loop if a page is all unparseable keys.
                break;
            }
            let removed = keys.len();
            debug_assert_eq!(removed, seqs.len());
            store
                .batch_delete(keys)
                .await
                .map_err(|e| format!("failed to drop oldest durable sync outbox entries: {e}"))?;
            store
                .flush()
                .await
                .map_err(|e| format!("failed to flush oldest durable sync outbox drops: {e}"))?;
            self.adjust_outbox_count(-(removed as i64), 0).await?;
            {
                let mut pending = self.pending.lock().await;
                pending.retain(|entry| !seqs.contains(&entry.seq));
            }
            self.capture_clear_pending_for_seqs(&seqs).await;
            total_removed += removed;
        }

        Ok(total_removed)
    }

    pub(crate) async fn persist_outbox_entry(&self, entry: &LogEntry) -> Result<(), String> {
        // Local-first: outbox depth/age overflow is never handled inline here.
        // The sync cycle's `maybe_force_snapshot_for_outbox_overflow` (called
        // from `do_sync`, off the write path) is the only place that ever
        // reduces durable outbox depth — and only after a successful snapshot.
        let store = self.outbox_store().await?;
        let bytes = serde_json::to_vec(entry)
            .map_err(|e| format!("failed to encode durable sync outbox entry: {e}"))?;
        store
            .put(&Self::outbox_key(entry.seq), bytes)
            .await
            .map_err(|e| format!("failed to persist durable sync outbox entry: {e}"))?;
        store
            .flush()
            .await
            .map_err(|e| format!("failed to flush durable sync outbox entry: {e}"))?;
        self.adjust_outbox_count(1, entry.seq).await?;
        Ok(())
    }

    pub async fn forget_recorded_op(&self, seq: u64) -> Result<(), String> {
        let store = self.outbox_store().await?;
        let existed = store
            .delete(&Self::outbox_key(seq))
            .await
            .map_err(|e| format!("failed to remove durable sync outbox entry: {e}"))?;
        store
            .flush()
            .await
            .map_err(|e| format!("failed to flush durable sync outbox removal: {e}"))?;
        if existed {
            self.adjust_outbox_count(-1, 0).await?;
        }
        {
            let mut pending = self.pending.lock().await;
            pending.retain(|entry| entry.seq != seq);
        }
        self.clear_adaptive_deferred_head_if_removed([seq]);
        // C3/C11: staging drop without upload must not leave export-pending forever
        // (otherwise capture ticks skip the key and never re-emit).
        self.capture_clear_pending_for_seqs(&[seq]).await;
        Ok(())
    }

    pub(crate) async fn remove_outbox_entries(&self, entries: &[LogEntry]) -> Result<(), String> {
        if entries.is_empty() {
            return Ok(());
        }
        let store = self.outbox_store().await?;
        let keys: Vec<Vec<u8>> = entries
            .iter()
            .map(|entry| Self::outbox_key(entry.seq))
            .collect();
        let removed = keys.len();
        store
            .batch_delete(keys)
            .await
            .map_err(|e| format!("failed to remove uploaded durable sync outbox entries: {e}"))?;
        store
            .flush()
            .await
            .map_err(|e| format!("failed to flush uploaded durable sync outbox removals: {e}"))?;
        // These keys were just read out of the outbox and uploaded, so each
        // existed exactly once; decrement the counter by the batch size.
        self.adjust_outbox_count(-(removed as i64), 0).await?;
        self.clear_adaptive_deferred_head_if_removed(entries.iter().map(|entry| entry.seq));
        // C3: former watermark ack (no-op after Watermark retirement); caller removes
        // entries only after upload succeeds).
        if let Err(e) = self.ack_uploaded_capture(entries).await {
            tracing::warn!(error = %e, "capture ack after upload failed");
        }
        Ok(())
    }

    /// Oldest durable outbox row with seq strictly greater than `after_seq`
    /// (or the absolute oldest when `after_seq` is `None`).
    ///
    /// Returns `(seq, raw_value_len, Some(entry))` when the row is under
    /// `max_bytes` (or `max_bytes == 0`), or `(seq, raw_value_len, None)` when
    /// the raw stored value exceeds the cap — **without** JSON-deserializing
    /// the poison payload (re-enable thrash: decode of multi-MB BatchPuts was
    /// the allocation spike).
    pub(crate) async fn outbox_row_after(
        &self,
        after_seq: Option<u64>,
        max_bytes: usize,
    ) -> Result<Option<(u64, usize, Option<LogEntry>)>, String> {
        let store = self.outbox_store().await?;
        let rows = match after_seq {
            None => store
                .scan_prefix_paged(SYNC_OUTBOX_ENTRY_PREFIX.as_bytes(), 1)
                .await
                .map_err(|e| format!("failed to scan durable sync outbox head: {e}"))?,
            Some(seq) => {
                // Keys are `entry:{seq:020}` — lexicographic order matches numeric.
                let start = Self::outbox_key(seq.saturating_add(1));
                // End bound: first key after the `entry:` prefix family.
                let end = b"entry;";
                store
                    .scan_range_paged(&start, end, 1)
                    .await
                    .map_err(|e| format!("failed to scan durable sync outbox after {seq}: {e}"))?
            }
        };
        match rows.into_iter().next() {
            Some((key, value)) => {
                let seq = Self::seq_from_outbox_key(&key).ok_or_else(|| {
                    format!(
                        "durable sync outbox key is not entry:seq: {}",
                        String::from_utf8_lossy(&key)
                    )
                })?;
                let raw_len = value.len();
                if max_bytes > 0 && raw_len > max_bytes {
                    return Ok(Some((seq, raw_len, None)));
                }
                let entry: LogEntry = serde_json::from_slice(&value).map_err(|e| {
                    format!(
                        "failed to decode durable sync outbox entry {}: {e}",
                        String::from_utf8_lossy(&key)
                    )
                })?;
                Ok(Some((seq, raw_len, Some(entry))))
            }
            None => Ok(None),
        }
    }

    // =========================================================================
}
