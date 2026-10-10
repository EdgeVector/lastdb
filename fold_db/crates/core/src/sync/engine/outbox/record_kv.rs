//! Typed put/delete recorders, sequence seeding, and entry construction.

use super::*;

impl SyncEngine {
    /// Leftover physical write: key + sha256 of the body, not the body.
    pub(crate) async fn record_physical_digest(
        &self,
        namespace: &str,
        items: &[(Vec<u8>, [u8; 32])],
    ) -> Result<u64, String> {
        let encoded_items: Vec<(String, String)> = items
            .iter()
            .map(|(key, digest)| (LogOp::encode_bytes(key), LogOp::encode_bytes(digest)))
            .collect();
        let record_count = items.len() as u64;
        let result = self
            .record_op(LogOp::PhysicalDigest {
                namespace: namespace.to_string(),
                items: encoded_items,
            })
            .await;
        if result.is_ok() {
            let counter = if crate::sync::policy::is_catalog_namespace(namespace) {
                &self.capture_physical_catalog_records
            } else {
                &self.capture_physical_fallback_records
            };
            counter.fetch_add(record_count, std::sync::atomic::Ordering::Relaxed);
        }
        result
    }

    /// Applyable leftover put. `db_catalog` membership uses this so org-log
    /// replay can restore the row. Other leftover catalog planes stay on
    /// [`Self::record_physical_digest`].
    pub(crate) async fn record_put(
        &self,
        namespace: &str,
        key: &[u8],
        value: &[u8],
    ) -> Result<u64, String> {
        self.record_op(LogOp::Put {
            namespace: namespace.to_string(),
            key: LogOp::encode_bytes(key),
            value: LogOp::encode_bytes(value),
        })
        .await
    }

    /// Record a committed delete operation for durable mutation-log capture.
    pub(crate) async fn record_delete(&self, namespace: &str, key: &[u8]) -> Result<u64, String> {
        self.record_op(LogOp::Delete {
            namespace: namespace.to_string(),
            key: LogOp::encode_bytes(key),
        })
        .await
    }

    /// Record a committed batch put for durable mutation-log capture.
    pub(crate) async fn record_batch_put(
        &self,
        namespace: &str,
        items: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<u64, String> {
        let encoded_items: Vec<(String, String)> = items
            .iter()
            .map(|(k, v)| (LogOp::encode_bytes(k), LogOp::encode_bytes(v)))
            .collect();

        self.record_op(LogOp::BatchPut {
            namespace: namespace.to_string(),
            items: encoded_items,
        })
        .await
    }

    /// Record a committed batch delete for durable mutation-log capture.
    pub(crate) async fn record_batch_delete(
        &self,
        namespace: &str,
        keys: &[Vec<u8>],
    ) -> Result<u64, String> {
        let encoded_keys: Vec<String> = keys.iter().map(|k| LogOp::encode_bytes(k)).collect();

        self.record_op(LogOp::BatchDelete {
            namespace: namespace.to_string(),
            keys: encoded_keys,
        })
        .await
    }

    /// Raise `self.seq` to every durable high-water mark a new sequence number
    /// must sort above, and return the seeded value.
    ///
    /// Two independent floors, and the snapshot path needs BOTH:
    ///
    /// - `outbox_meta().max_seq` — the highest seq still staged for upload.
    ///   Seeding from it is what stops a post-restart write from overwriting an
    ///   unsynced entry when the wall clock has regressed.
    /// - `durable_frontier_floor_for_writer` — the flushed mutation-log
    ///   allocation floor plus each target's published-F high-water mark. This
    ///   is the ONLY floor that survives a fully drained outbox, which is the
    ///   steady state of a healthy node: uploaded entries are deleted on ack,
    ///   so `max_seq` falls back to 0 while the published frontier keeps
    ///   climbing.
    ///
    /// Fails rather than seeding low. A snapshot cut below a published
    /// frontier is not a slow path, it is a wrong one: `latest.enc` stamped at
    /// a stale sequence lets a restore replay older cloud logs *after* the
    /// snapshot and reverse a delete that only the snapshot captured.
    ///
    /// Lock order is `outbox_meta -> seq`; nothing takes `seq` then
    /// `outbox_meta`, so this cannot deadlock.
    pub(crate) async fn seed_seq_from_durable_floor(
        &self,
        target_generation: u64,
        targets: &[SyncTarget],
    ) -> Result<u64, String> {
        let mut meta = self.outbox_meta().await?;
        if meta.seeded_target_generation != Some(target_generation) {
            let validate_all_pin_rows = !meta.validated_pin_log_allocation_floor;
            let durable_floor = self
                .pin_log
                .durable_frontier_floor_for_writer(&self.device_id, targets, validate_all_pin_rows)
                .await?;
            let seed = meta.max_seq.max(durable_floor);
            let mut seq = self.seq.lock().await;
            *seq = (*seq).max(seed);
            meta.seeded_target_generation = Some(target_generation);
            meta.validated_pin_log_allocation_floor = true;
        }
        drop(meta);
        Ok(*self.seq.lock().await)
    }

    pub(super) async fn make_entry_for_target_snapshot(
        &self,
        op: LogOp,
        target_generation: u64,
        targets: &[SyncTarget],
    ) -> Result<LogEntry, String> {
        // Seed `self.seq` past every durable floor BEFORE minting. Seeding is
        // lazy (the outbox counter fires once, on the first `outbox_meta()`
        // call), so without this the very first write after a restart would
        // mint from an unseeded counter: if the wall clock has regressed,
        // `nanos` could land <= the highest persisted seq and the new entry
        // would overwrite or sort below an existing unsynced entry.
        self.seed_seq_from_durable_floor(target_generation, targets)
            .await?;

        let nanos = crate::clock::unix_nanos();

        // Ensure monotonically increasing: if clock gives same nanos as last
        // entry, bump by 1 to guarantee uniqueness within this process.
        let mut last = self.seq.lock().await;
        let seq = if nanos <= *last { *last + 1 } else { nanos };
        *last = seq;

        Ok(LogEntry {
            seq,
            timestamp_ms: crate::clock::unix_millis(),
            device_id: self.device_id.clone(),
            op,
        })
    }

    // =========================================================================
    // Sync cycle
}
