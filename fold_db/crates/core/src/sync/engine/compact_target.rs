//! Phases of `SyncEngine::compact_target`: choose the cut, publish the
//! photograph, then prune what the published cut supersedes.

use super::super::error::{SyncError, SyncResult};
use super::super::org_sync::SyncTarget;
use super::compact::{photograph_frontier_from_log_keys, sha256_file_hex};
use super::*;
use crate::sync::snapshot_log::{Frontier, LatestCasPayload};

/// What `publish_compaction_photograph` committed, for the cleanup phases.
pub(super) struct PublishedPhotograph {
    pub(super) snapshot_bytes: u64,
    pub(super) snapshot_name: String,
    pub(super) prior_snapshot_name: Option<String>,
    pub(super) latest_payload: LatestCasPayload,
}

impl SyncEngine {
    /// Poison gate plus the data-loss bound. Returns the `(last_seq, frontier)`
    /// to stamp, or `None` when compaction must be skipped.
    pub(super) async fn choose_compaction_cut(
        &self,
        target: &SyncTarget,
        requested_last_seq: u64,
        proven_prefixes: &HashSet<String>,
    ) -> SyncResult<Option<(u64, Frontier)>> {
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
            return Ok(None);
        }
        tracing::info!(
            target = %target.label,
            prefix = %target.prefix,
            requested_last_seq,
            download_cursor = cursor,
            last_seq,
            "compacting sync target (delete/stamp bounded by download cursor)"
        );
        Ok(Some((last_seq, photograph_frontier)))
    }

    /// Seal a photograph at the cut and publish it (CAS `latest`).
    pub(super) async fn publish_compaction_photograph(
        &self,
        target: &SyncTarget,
        last_seq: u64,
        photograph_frontier: Frontier,
    ) -> SyncResult<PublishedPhotograph> {
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
        Ok(PublishedPhotograph {
            snapshot_bytes,
            snapshot_name,
            prior_snapshot_name,
            latest_payload,
        })
    }

    /// Delete the cloud log entries the published frontier covers.
    pub(super) async fn delete_compacted_log_objects(
        &self,
        target: &SyncTarget,
        objects: &[crate::sync::auth::S3ObjectInfo],
        frontier: &Frontier,
    ) {
        let old_keys: Vec<(u64, String)> = objects
            .iter()
            .filter_map(|obj| {
                let (writer, seq) = parse_mutation_log_object_key(&obj.key)?;
                frontier.covers_log(writer.as_deref(), seq).then(|| {
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
                                .confirm_log_delete_object_keys(target, &gone_seqs, &gone_keys)
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
    }

    /// Rebuild the personal log index after the deletes.
    pub(super) async fn rebuild_personal_log_index_after_compact(
        &self,
        target: &SyncTarget,
        objects: &[crate::sync::auth::S3ObjectInfo],
        last_seq: u64,
    ) {
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
                    .filter_map(|obj| parse_mutation_log_object_key(&obj.key).map(|(_, seq)| seq))
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

    /// Remove the photograph this publish replaced (after the CAS).
    pub(super) async fn delete_prior_photograph(
        &self,
        target: &SyncTarget,
        prior_snapshot_name: Option<String>,
        snapshot_name: &str,
    ) {
        // Keep one committed photograph. The old object remains valid until
        // the CAS above succeeds; only then may cleanup remove it.
        if let Some(previous) = prior_snapshot_name.filter(|previous| previous != snapshot_name) {
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
    }

    /// Apply snapshot retention to the target's snapshot objects.
    pub(super) async fn prune_superseded_snapshots(&self, target: &SyncTarget) {
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
    }

    /// Apply the same retention to personal thumbnail packs.
    pub(super) async fn prune_superseded_thumb_packs(&self, target: &SyncTarget) {
        let retain = self.config.snapshot_retention;
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
    }
}
