//! Compaction policy + snapshot/delete-old-logs.

use super::super::error::{SyncError, SyncResult};
use super::super::org_sync::SyncTarget;
use super::*;
use crate::hex::hex_lower;
use crate::sync::snapshot_log::{Frontier, LatestCasPayload};
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
        // Poison gate (P1): compaction publishes the photograph a new device
        // bootstraps from, so a wrong-key publish here is the
        // highest-blast-radius poison there is, exactly as in
        // `backup_snapshot_once`. The per-cycle upload path proves the prefix
        // before appending (cycle.rs), but compaction is reached on paths that
        // uploaded nothing this cycle: the size/time trigger fires whenever
        // `should_compact_now()` is true, and `maybe_compact_personal_log` runs
        // specifically when the pending queue is EMPTY. An idle-but-drifted-key
        // device crossing `compaction_max_interval_secs` would otherwise reseal
        // and publish `latest` with zero verification that the local key
        // still opens the existing cloud prefix. Fail closed instead: an
        // unproven prefix defers compaction to a later cycle, which costs
        // cloud storage, not data.
        if !proven_prefixes.contains(&target.prefix) {
            self.prove_prefix_decryptable(target).await?;
        }
        // Data-loss guard (P1): never snapshot-stamp or delete past the highest
        // seq this device has actually REPLAYED into its local store for this
        // target. Callers derive `requested_last_seq` from a fresh remote listing
        // (`maybe_compact_personal_log` / `maybe_compact_target_log`) or from this
        // device's own upload counter `self.seq` (`compact`) — neither is bounded
        // by what this device has DOWNLOADED. A concurrent peer of the same user
        // can upload a log entry in the window between this device's download and
        // its compaction re-list; the snapshot we upload is built from the LOCAL
        // store (which lacks that entry), yet an unbounded `last_seq` would stamp
        // and delete right up to it — leaving the cloud object deleted but absent
        // from the published photograph: permanent, silent loss. Bounding by the download
        // cursor guarantees every deleted cloud entry is provably present in the
        // uploaded snapshot; the un-replayed tail is simply deferred to a later
        // cycle (a single-device engine re-downloads its own entries next cycle,
        // so the cursor tracks the global replayed head and compaction is not
        // stalled — only the newest not-yet-downloaded-back entries defer).
        let incorporated_frontier =
            if target.prefix.is_empty() && self.config.capture_mode == CaptureMode::MutationLog {
                Some(self.pin_log.incorporated_frontier().await)
            } else {
                None
            };
        let cursor = if let Some(frontier) = incorporated_frontier.as_ref() {
            frontier.max_through()
        } else {
            let cursors = self.download_cursors.lock().await;
            cursors.get(&target.prefix).copied().unwrap_or(0)
        };
        let candidate_last_seq = requested_last_seq.min(cursor);
        let cut_objects = self.auth.list_log_objects(target).await?;
        let photograph_frontier = photograph_frontier_from_log_keys(
            candidate_last_seq,
            cut_objects.iter().map(|object| object.key.as_str()),
            incorporated_frontier.as_ref(),
        );
        let last_seq = photograph_frontier.max_through();
        // Refuse to stamp latest at last_seq=0 when the caller requested a
        // non-empty compact high-water. That combination means the download
        // cursor never tracked the replayed head (e.g. snapshot-only bootstrap
        // forgot to seed it); uploading a seq-0 checkpoint would replace a
        // good photograph with a hollow one. Empty prefix (requested == 0) is
        // the only legitimate last_seq=0 compact path.
        if last_seq == 0 && requested_last_seq > 0 {
            tracing::warn!(
                target = %target.label,
                prefix = %target.prefix,
                requested_last_seq,
                download_cursor = cursor,
            "skipping compaction: computed last_seq=0 while requested > 0 \
                 (download cursor not seeded; refuse hollow photograph stamp)"
            );
            return Ok(0);
        }
        tracing::info!(
            target = %target.label,
            prefix = %target.prefix,
            requested_last_seq,
            download_cursor = cursor,
            last_seq,
            "compacting sync target (delete/stamp bounded by download cursor)"
        );

        let snapshot = self
            .create_photograph_at_cut(&target.prefix, last_seq)
            .await?;

        // Seal to temp file; dual-upload without sealed.clone() of full buffer.
        let sealed_dir = std::env::temp_dir().join("lastdb-snapshot-seal");
        let sealed_path = sealed_dir.join(format!(
            "compact-{}-{}-{}.enc",
            target.label.replace(['/', ':'], "_"),
            last_seq,
            std::process::id()
        ));
        let snapshot_bytes = snapshot
            .seal_to_path(&target.crypto, &sealed_path)
            .await
            .inspect_err(|_| {
                let _ = std::fs::remove_file(&sealed_path);
            })?;

        let snapshot_id = sha256_file_hex(&sealed_path)?;
        // Share prefixes keep their established `latest.enc` contract. The
        // storage service does not expose photograph CAS for shared scopes;
        // personal and org/database targets use the named photograph model.
        let legacy_share = target.prefix.starts_with("share:");
        let snapshot_name = if legacy_share {
            format!("{last_seq}.enc")
        } else {
            format!("{snapshot_id}.enc")
        };
        let prior_latest = if legacy_share {
            None
        } else {
            Some(self.auth.photograph_latest_get_for_target(target).await?)
        };
        let publish_counter = prior_latest
            .as_ref()
            .and_then(|response| response.latest.as_ref())
            .map_or(1, |latest| latest.counter.saturating_add(1));
        let prior_snapshot_name = prior_latest
            .as_ref()
            .and_then(|response| response.latest.as_ref())
            .map(|latest| format!("{}.enc", latest.snapshot_id));
        let latest_payload = LatestCasPayload {
            model_version: crate::sync::snapshot_log::SNAPSHOT_LOG_MODEL_VERSION,
            snapshot_id: snapshot_id.clone(),
            frontier: photograph_frontier,
            counter: publish_counter,
            store_uuid: None,
            epoch: None,
        };

        if target.prefix.is_empty() {
            let pack_id = snapshot_name.clone();
            match self
                .upload_thumb_pack_for_snapshot(&snapshot, &pack_id)
                .await
            {
                Ok(Some(count)) => tracing::info!(
                    target: "fold_db::sync",
                    pack_id = %pack_id,
                    thumbnails = count,
                    "compaction uploaded thumbnail pack"
                ),
                Ok(None) => {}
                Err(e) => tracing::warn!(
                    target: "fold_db::sync",
                    pack_id = %pack_id,
                    error = %e,
                    "compaction thumbnail pack upload failed (non-fatal)"
                ),
            }
        }
        let upload_result: SyncResult<()> = async {
            if legacy_share {
                if self.config.snapshot_retention > 0 {
                    self.upload_snapshot_file_for_target_with_retry(
                        &format!("upload snapshot '{}' {snapshot_name}", target.label),
                        target,
                        &snapshot_name,
                        &sealed_path,
                    )
                    .await?;
                }
                self.upload_snapshot_file_for_target_with_retry(
                    &format!("upload snapshot '{}' latest.enc", target.label),
                    target,
                    "latest.enc",
                    &sealed_path,
                )
                .await?;
                return Ok(());
            }
            self.upload_snapshot_file_for_target_with_retry(
                &format!("upload photograph '{}' {snapshot_name}", target.label),
                target,
                &snapshot_name,
                &sealed_path,
            )
            .await?;
            if let Err(e) = self
                .auth
                .confirm_snapshot_upload_for_target(target, &snapshot_name)
                .await
            {
                tracing::warn!(
                    target: "fold_db::sync",
                    error = %e,
                    snapshot = %snapshot_name,
                    "confirm_snapshot_upload metering failed for photograph (non-fatal)"
                );
            }
            let published = self
                .auth
                .photograph_latest_cas_for_target(target, &latest_payload)
                .await?;
            tracing::info!(
                target: "fold_db::sync::photograph",
                latest_key = %published.key,
                snapshot_id = %snapshot_id,
                frontier = ?latest_payload.frontier,
                counter = publish_counter,
                "published compact photograph latest tuple"
            );
            Ok(())
        }
        .await;
        let _ = tokio::fs::remove_file(&sealed_path).await;
        upload_result?;
        if target.prefix.is_empty() {
            if let Err(e) = self.capture_reset_watermark_from_snapshot(&snapshot).await {
                tracing::warn!(
                    error = %e,
                    "capture pending clear after compaction failed (non-fatal)"
                );
            }
        }
        drop(snapshot);

        match self.auth.list_log_objects(target).await {
            Ok(objects) => {
                let old_keys: Vec<(u64, String)> = objects
                    .iter()
                    .filter_map(|obj| {
                        let (writer, seq) = parse_mutation_log_object_key(&obj.key)?;
                        latest_payload
                            .frontier
                            .covers_log(writer.as_deref(), seq)
                            .then(|| {
                                (
                                    seq,
                                    relative_mutation_log_key(&obj.key)
                                        .unwrap_or(obj.key.as_str())
                                        .to_string(),
                                )
                            })
                    })
                    .collect();
                if !old_keys.is_empty() {
                    let mut deleted = 0usize;
                    let mut presign_err: Option<SyncError> = None;
                    for chunk in old_keys.chunks(MAX_PRESIGN_BATCH) {
                        let seqs: Vec<u64> = chunk.iter().map(|(seq, _)| *seq).collect();
                        let keys: Vec<String> = chunk.iter().map(|(_, key)| key.clone()).collect();
                        match self
                            .auth
                            .presign_log_delete_object_keys(target, &seqs, &keys)
                            .await
                        {
                            Ok(delete_urls) => {
                                // URLs come back in request order; debit
                                // only the objects whose DELETE succeeded.
                                let mut gone_seqs = Vec::with_capacity(chunk.len());
                                let mut gone_keys = Vec::with_capacity(chunk.len());
                                for (url, (seq, key)) in delete_urls.iter().zip(chunk) {
                                    if let Err(e) = self.s3.delete(url).await {
                                        tracing::warn!(
                                            target = %target.label,
                                            error = %e,
                                            "failed to delete compacted log (non-fatal)"
                                        );
                                    } else {
                                        gone_seqs.push(*seq);
                                        gone_keys.push(key.clone());
                                    }
                                }
                                deleted += delete_urls.len();
                                if !gone_seqs.is_empty() {
                                    if let Err(e) = self
                                        .auth
                                        .confirm_log_delete_object_keys(
                                            target, &gone_seqs, &gone_keys,
                                        )
                                        .await
                                    {
                                        tracing::warn!(
                                            target = %target.label,
                                            error = %e,
                                            "failed to confirm compacted log delete for \
                                             metering (non-fatal; the daily storage audit heals it)"
                                        );
                                    }
                                }
                            }
                            Err(e) => {
                                presign_err = Some(e);
                                break;
                            }
                        }
                    }
                    if let Some(e) = presign_err {
                        tracing::warn!(
                            target = %target.label,
                            error = %e,
                            "failed to get delete URLs for compacted logs (non-fatal)"
                        );
                    }
                    if deleted > 0 {
                        tracing::info!(target = %target.label, deleted, "deleted compacted log entries");
                    }
                }
                if Self::supports_personal_log_index(target) {
                    // Stale-listing race (P1): a concurrent peer can upload seq X
                    // and merge X into `log_index.enc` while this compact is
                    // mid-flight. Building the post-compact index from the
                    // pre-delete listing alone would drop X, and peers that
                    // trust the index would never see that seq until a full
                    // list rebuild. After deletes complete:
                    //   1) re-list the cloud log (fresh objects), and
                    //   2) union with the existing index's seqs > last_seq
                    // so neither source of truth can silently lose concurrent
                    // peer uploads. Pure decision lives in
                    // `personal_log_index_seqs_after_compact`.
                    let listed_seqs: Vec<u64> = match self.auth.list_log_objects(target).await {
                        Ok(fresh) => fresh
                            .iter()
                            .filter_map(|obj| {
                                parse_mutation_log_object_key(&obj.key).map(|(_, seq)| seq)
                            })
                            .collect(),
                        Err(e) => {
                            tracing::warn!(
                                target = %target.label,
                                error = %e,
                                "post-delete re-list for personal log index failed; \
                                 falling back to pre-delete listing + existing index union"
                            );
                            objects
                                .iter()
                                .filter_map(|obj| {
                                    parse_mutation_log_object_key(&obj.key).map(|(_, seq)| seq)
                                })
                                .collect()
                        }
                    };
                    let existing_seqs: Vec<u64> = match self.read_personal_log_index(target).await {
                        Ok(Some(idx)) => idx.seqs,
                        Ok(None) => Vec::new(),
                        Err(e) => {
                            tracing::warn!(
                                target = %target.label,
                                error = %e,
                                "read existing personal log index during compact failed; \
                                 continuing with listing only"
                            );
                            Vec::new()
                        }
                    };
                    let retained =
                        personal_log_index_seqs_after_compact(last_seq, listed_seqs, existing_seqs);
                    let index = PersonalLogIndex::from_seqs(retained);
                    if let Err(e) = self
                        .prune_personal_log_index_after_compact(target, &index, last_seq)
                        .await
                    {
                        tracing::warn!(
                            target = %target.label,
                            error = %e,
                            "failed to prune personal log index after compaction (non-fatal)"
                        );
                    }
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
        if let Some(previous) = prior_snapshot_name.filter(|previous| previous != &snapshot_name) {
            match self
                .auth
                .presign_snapshot_delete_for_target(target, &previous)
                .await
            {
                Ok(url) => {
                    if let Err(e) = self.s3.delete(&url).await {
                        tracing::warn!(
                            target = %target.label,
                            snapshot = %previous,
                            error = %e,
                            "failed to delete prior photograph after CAS (non-fatal)"
                        );
                    } else if let Err(e) = self
                        .auth
                        .confirm_snapshot_delete_for_target(target, &previous)
                        .await
                    {
                        tracing::warn!(
                            target = %target.label,
                            snapshot = %previous,
                            error = %e,
                            "failed to confirm prior photograph delete for metering (non-fatal)"
                        );
                    }
                }
                Err(e) => tracing::warn!(
                    target = %target.label,
                    snapshot = %previous,
                    error = %e,
                    "failed to presign prior photograph delete (non-fatal)"
                ),
            }
        }

        let retain = self.config.snapshot_retention;
        match self.auth.list_snapshot_objects(target).await {
            Ok(objects) => {
                let keys: Vec<String> = objects.into_iter().map(|obj| obj.key).collect();
                let to_prune = snapshots_to_prune(&keys, retain);
                if !to_prune.is_empty() {
                    let mut deleted = 0usize;
                    for name in &to_prune {
                        match self
                            .auth
                            .presign_snapshot_delete_for_target(target, name)
                            .await
                        {
                            Ok(url) => {
                                if let Err(e) = self.s3.delete(&url).await {
                                    tracing::warn!(
                                        target = %target.label,
                                        snapshot = %name,
                                        error = %e,
                                        "failed to delete superseded snapshot (non-fatal)"
                                    );
                                } else {
                                    deleted += 1;
                                    if let Err(e) = self
                                        .auth
                                        .confirm_snapshot_delete_for_target(target, name)
                                        .await
                                    {
                                        tracing::warn!(
                                            target = %target.label,
                                            snapshot = %name,
                                            error = %e,
                                            "failed to confirm snapshot delete for metering \
                                             (non-fatal)"
                                        );
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::warn!(
                                    target = %target.label,
                                    snapshot = %name,
                                    error = %e,
                                    "failed to presign snapshot delete (non-fatal)"
                                );
                            }
                        }
                    }
                    if deleted > 0 {
                        tracing::info!(
                            target = %target.label,
                            deleted,
                            retained = retain,
                            "pruned superseded snapshots"
                        );
                    }
                }
            }
            Err(e) => {
                tracing::warn!(
                    target = %target.label,
                    error = %e,
                    "failed to list snapshots for retention prune (non-fatal)"
                );
            }
        }

        // Thumbnail packs use the photograph object name and are only written
        // for the personal target. Apply the same retention here so compaction
        // cannot grow `thumbs/packs/` without bound. When snapshot_retention
        // is 0 (keep only the current photograph), keep the single newest
        // pack — that object *is* the restore source after loose thumbs are
        // deleted post-pack-upload.
        if target.prefix.is_empty() {
            let pack_retain = retain.max(1);
            match self.auth.list_objects("thumbs/packs/").await {
                Ok(objects) => {
                    let keys: Vec<String> = objects.into_iter().map(|obj| obj.key).collect();
                    let to_prune = thumb_packs_to_prune(&keys, pack_retain);
                    if !to_prune.is_empty() {
                        let mut deleted = 0usize;
                        for pack_id in &to_prune {
                            match self.auth.presign_thumb_pack_delete(pack_id).await {
                                Ok(url) => {
                                    if let Err(e) = self.s3.delete(&url).await {
                                        tracing::warn!(
                                            target = %target.label,
                                            pack_id = %pack_id,
                                            error = %e,
                                            "failed to delete superseded thumb pack (non-fatal)"
                                        );
                                    } else {
                                        deleted += 1;
                                        if let Err(e) =
                                            self.auth.confirm_thumb_pack_delete(pack_id).await
                                        {
                                            tracing::warn!(
                                                target = %target.label,
                                                pack_id = %pack_id,
                                                error = %e,
                                                "failed to confirm thumb pack delete for \
                                                 metering (non-fatal)"
                                            );
                                        }
                                    }
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        target = %target.label,
                                        pack_id = %pack_id,
                                        error = %e,
                                        "failed to presign thumb pack delete (non-fatal)"
                                    );
                                }
                            }
                        }
                        if deleted > 0 {
                            tracing::info!(
                                target = %target.label,
                                deleted,
                                retained = pack_retain,
                                "pruned superseded thumbnail packs"
                            );
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        target = %target.label,
                        error = %e,
                        "failed to list thumbnail packs for retention prune (non-fatal)"
                    );
                }
            }
        }

        tracing::info!(
            target = %target.label,
            snapshot_bytes,
            last_seq,
            "target compaction complete"
        );
        Ok(snapshot_bytes)
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

fn sha256_file_hex(path: &std::path::Path) -> SyncResult<String> {
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
