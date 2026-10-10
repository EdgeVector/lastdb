//! Compaction policy + snapshot/delete-old-logs.

use super::super::error::{SyncError, SyncResult};
use super::super::org_sync::SyncTarget;
use super::*;
use crate::hex::hex_lower;
use crate::sync::snapshot_log::Frontier;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::io::Read;

impl SyncEngine {
    pub(crate) async fn create_photograph_at_cut(
        &self,
        target_prefix: &str,
        last_seq: u64,
    ) -> SyncResult<Snapshot> {
        // Refuse before the barrier and before any row is read: the personal
        // photograph holds the whole store in memory, so a store over the cap
        // cannot complete it and each attempt only costs footprint and swap.
        if target_prefix.is_empty() {
            let disk_bytes = photograph_disk_bytes(self.store.as_ref()).await?;
            if let Some(refusal) =
                personal_photograph_size_refusal(disk_bytes, personal_photograph_max_disk_bytes())
            {
                return Err(SyncError::Storage(refusal));
            }
        }

        // The resident-primary path can acknowledge a mutation before its
        // LastStore put completes. F already names those captured mutations.
        // Drain the work below the barrier before S enumerates the store, or a
        // published cut can omit a row and then prune the only log that has it.
        self.invoke_photograph_cut_barrier()
            .await
            .map_err(SyncError::Storage)?;

        if target_prefix.is_empty() {
            Snapshot::create(self.store.as_ref(), &self.device_id, last_seq).await
        } else {
            let restore_scopes = self
                .target_restore_scopes
                .lock()
                .await
                .get(target_prefix)
                .cloned()
                .unwrap_or_else(|| vec![target_prefix.to_string()]);
            Snapshot::create_scoped_to_prefixes(
                self.store.as_ref(),
                &self.device_id,
                last_seq,
                &restore_scopes,
            )
            .await
        }
    }

    /// High-water that can name a personal photograph cut.
    ///
    /// The legacy outbox advances `self.seq`. The continuous MutationLog plane
    /// leaves that scalar at zero and advances its incorporated vector F
    /// instead. Use the active plane's high-water so an entry-count or size
    /// trigger can publish S after modern writer-segment uploads.
    pub(crate) async fn personal_compaction_high_water(&self) -> u64 {
        if self.config.capture_mode == CaptureMode::MutationLog {
            self.pin_log.incorporated_frontier().await.max_through()
        } else {
            *self.seq.lock().await
        }
    }

    /// Evaluate the size/time-based compaction policy against current engine
    /// state. Thin async wrapper that gathers the [`CompactionInputs`] under the
    /// state locks and delegates to the pure [`should_compact`] decision.
    pub(crate) async fn should_compact_now(&self) -> bool {
        let log_bytes_since = *self.bytes_since_snapshot.lock().await;
        let snapshot_bytes = *self.last_snapshot_bytes.lock().await;
        let last_at = *self.last_snapshot_at.lock().await;
        let elapsed_secs = last_at.map(|at| crate::clock::unix_secs().saturating_sub(at));
        let entries_since = *self.entries_since_snapshot.lock().await;

        should_compact(
            CompactionInputs {
                log_bytes_since,
                snapshot_bytes,
                elapsed_secs,
                entries_since,
            },
            self.config.compaction_log_ratio,
            self.config.compaction_max_interval_secs,
            self.config.compaction_min_interval_secs,
            self.config.compaction_threshold,
        )
    }

    // Compaction (snapshot + delete old logs)
    // =========================================================================

    /// `proven_prefixes` carries the set of prefixes this cycle already proved
    /// decryptable (see [`SyncEngine::compact_target`]'s poison gate). Callers
    /// outside a sync cycle pass an empty set and pay for a fresh proof.
    pub(crate) async fn compact(
        &self,
        last_seq: u64,
        proven_prefixes: &HashSet<String>,
    ) -> SyncResult<()> {
        if !self.config.legacy_personal_cloud_sync
            && self.config.capture_mode != CaptureMode::MutationLog
        {
            return Ok(());
        }
        let personal = self.targets.lock().await[0].clone();
        let snapshot_bytes = self
            .compact_target(&personal, last_seq, proven_prefixes)
            .await?;
        // Hollow / no-op compact (e.g. last_seq collapsed to 0 with unseeded
        // download cursor) returns Ok(0) without uploading a snapshot. Do not
        // treat that as success: recording would zero growth counters and set
        // last_snapshot_at, starving real compaction behind min-interval.
        if snapshot_bytes == 0 {
            tracing::info!(
                last_seq,
                "compaction skipped: no snapshot uploaded (hollow/no-op path)"
            );
            return Ok(());
        }
        self.record_personal_compaction_complete(snapshot_bytes)
            .await;

        tracing::info!(
            snapshot_bytes,
            "compaction complete: snapshot at seq {last_seq}"
        );
        Ok(())
    }

    pub(crate) async fn record_personal_compaction_complete(&self, snapshot_bytes: u64) {
        // Photograph upload + latest CAS succeeded (the `?` paths above would have
        // returned otherwise). Record the new snapshot's size/time and reset the
        // per-snapshot accumulators so the size/time trigger measures growth from
        // here forward. (The log/snapshot prune blocks are best-effort and
        // intentionally non-fatal, so reaching here means the snapshot is durable.)
        // Callers must not invoke this on Ok(0) hollow/no-op compact; refuse
        // anyway so a future caller cannot zero counters without a real stamp.
        if snapshot_bytes == 0 {
            return;
        }
        *self.last_snapshot_bytes.lock().await = snapshot_bytes;
        *self.last_snapshot_at.lock().await = Some(crate::clock::unix_secs());
        *self.bytes_since_snapshot.lock().await = 0;
        *self.entries_since_snapshot.lock().await = 0;
        // C3 capture-pending clear is tied to the exact durable snapshot payload in
        // compact_target, before concurrent post-snapshot writes can be
        // mistaken for already-exported data.
    }

    pub(crate) async fn maybe_compact_personal_log(
        &self,
        proven_prefixes: &HashSet<String>,
    ) -> SyncResult<()> {
        if !self.config.legacy_personal_cloud_sync
            && self.config.capture_mode != CaptureMode::MutationLog
        {
            return Ok(());
        }
        let personal = self.targets.lock().await[0].clone();
        let objects = self.auth.list_log_objects(&personal).await?;
        let seqs: Vec<u64> = objects
            .iter()
            .filter_map(|obj| parse_mutation_log_object_key(&obj.key).map(|(_, seq)| seq))
            .collect();
        let Some(remote_last_seq) =
            target_log_compaction_seq(&seqs, self.config.compaction_threshold)
        else {
            return Ok(());
        };
        let local_last_seq = *self.seq.lock().await;
        let last_seq = remote_last_seq.max(local_last_seq);
        let snapshot_bytes = self
            .compact_target(&personal, last_seq, proven_prefixes)
            .await?;
        // Same hollow/no-op contract as `compact`: Ok(0) means no snapshot was
        // uploaded — do not record completion or log "complete".
        if snapshot_bytes == 0 {
            tracing::info!(
                remote_log_entries = seqs.len(),
                remote_last_seq,
                last_seq,
                "remote-log-count compaction skipped: no snapshot uploaded (hollow/no-op path)"
            );
            return Ok(());
        }
        self.record_personal_compaction_complete(snapshot_bytes)
            .await;
        tracing::info!(
            remote_log_entries = seqs.len(),
            remote_last_seq,
            last_seq,
            snapshot_bytes,
            "remote-log-count compaction complete"
        );
        Ok(())
    }

    pub(crate) async fn maybe_compact_target_log(
        &self,
        target: &SyncTarget,
        proven_prefixes: &HashSet<String>,
    ) -> SyncResult<()> {
        if target.prefix.is_empty() {
            return Ok(());
        }
        let objects = self.auth.list_log_objects(target).await?;
        let seqs: Vec<u64> = objects
            .iter()
            .filter_map(|obj| parse_mutation_log_object_key(&obj.key).map(|(_, seq)| seq))
            .collect();
        let Some(last_seq) = target_log_compaction_seq(&seqs, self.config.compaction_threshold)
        else {
            return Ok(());
        };
        self.compact_target(target, last_seq, proven_prefixes)
            .await?;
        Ok(())
    }

    /// Build a named photograph of `target`, CAS `latest` to `(S, F, counter)`,
    /// then prune the logs and photographs the published cut supersedes.
    ///
    /// `proven_prefixes` is the set of prefixes the current sync cycle has
    /// already proved decryptable; a prefix in that set skips the poison gate
    /// below rather than paying for a second proof round-trip in one cycle.
    pub(crate) async fn compact_target(
        &self,
        target: &SyncTarget,
        requested_last_seq: u64,
        proven_prefixes: &HashSet<String>,
    ) -> SyncResult<u64> {
        let Some((last_seq, photograph_frontier)) = self
            .choose_compaction_cut(target, requested_last_seq, proven_prefixes)
            .await?
        else {
            return Ok(0);
        };
        let published = self
            .publish_compaction_photograph(target, last_seq, photograph_frontier)
            .await?;

        match self.auth.list_log_objects(target).await {
            Ok(objects) => {
                self.delete_compacted_log_objects(
                    target,
                    &objects,
                    &published.latest_payload.frontier,
                )
                .await;
                if Self::supports_personal_log_index(target) {
                    self.rebuild_personal_log_index_after_compact(target, &objects, last_seq)
                        .await;
                }
            }
            Err(e) => {
                tracing::warn!(
                    target = %target.label,
                    error = %e,
                    "failed to list logs for compaction cleanup (non-fatal)"
                );
            }
        }

        // Keep one committed photograph. The old object remains valid until
        // the CAS above succeeds; only then may cleanup remove it.
        self.delete_prior_photograph(
            target,
            published.prior_snapshot_name,
            &published.snapshot_name,
        )
        .await;
        self.prune_superseded_snapshots(target).await;
        if target.prefix.is_empty() {
            self.prune_superseded_thumb_packs(target).await;
        }

        tracing::info!(
            target = %target.label,
            snapshot_bytes = published.snapshot_bytes,
            last_seq,
            "target compaction complete"
        );
        Ok(published.snapshot_bytes)
    }

    // =========================================================================
    // Bootstrap (download snapshot + replay logs)
}

/// On-disk bytes of the planes `Snapshot::create` would photograph (every
/// namespace the snapshot does not skip). A stat walk per plane; no shard
/// loads and no row reads. `None` when any plane cannot be measured, so the
/// caller keeps today's behaviour instead of guessing.
pub(super) async fn photograph_disk_bytes(
    store: &dyn crate::storage::traits::NamespacedStore,
) -> SyncResult<Option<u64>> {
    let mut total: u64 = 0;
    for name in store.list_namespaces().await? {
        if crate::sync::policy::snapshot_should_skip_namespace(&name) {
            continue;
        }
        let Some(bytes) = store.collection_disk_bytes(&name) else {
            return Ok(None);
        };
        total = total.saturating_add(bytes);
    }
    Ok(Some(total))
}

pub(super) fn sha256_file_hex(path: &std::path::Path) -> SyncResult<String> {
    let mut file = std::fs::File::open(path)
        .map_err(|e| SyncError::Storage(format!("open sealed photograph for hash: {e}")))?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 1024 * 1024];
    loop {
        let read = file
            .read(&mut buf)
            .map_err(|e| SyncError::Storage(format!("read sealed photograph for hash: {e}")))?;
        if read == 0 {
            break;
        }
        hasher.update(&buf[..read]);
    }
    Ok(hex_lower(hasher.finalize()))
}

pub(super) fn photograph_frontier_from_log_keys<'a>(
    last_seq: u64,
    keys: impl IntoIterator<Item = &'a str>,
    incorporated: Option<&Frontier>,
) -> Frontier {
    let mut writer_hwm = BTreeMap::new();
    let mut saw_flat = false;
    for key in keys {
        let Some((writer, seq)) = parse_mutation_log_object_key(key) else {
            continue;
        };
        if seq > last_seq {
            continue;
        }
        match writer {
            Some(writer) => {
                let seq = match incorporated {
                    Some(Frontier::Vector { through }) => {
                        let Some(local_through) = through.get(&writer) else {
                            continue;
                        };
                        seq.min(*local_through)
                    }
                    Some(Frontier::Scalar { through }) => seq.min(*through),
                    None => seq,
                };
                writer_hwm
                    .entry(writer)
                    .and_modify(|through: &mut u64| *through = (*through).max(seq))
                    .or_insert(seq);
            }
            None if incorporated.is_none() => saw_flat = true,
            None => {}
        }
    }
    if incorporated.is_some() || (!saw_flat && !writer_hwm.is_empty()) {
        Frontier::vector(writer_hwm)
    } else {
        Frontier::scalar(last_seq)
    }
}
