//! Phases of `SyncEngine::compact_target`: choose the cut, publish the
//! photograph, then prune what the published cut supersedes.

use super::super::error::SyncResult;
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
            self.upload_compaction_thumb_pack(&snapshot, &snapshot_name)
                .await;
        }
        let upload_result = self
            .upload_and_publish_photograph(
                target,
                &sealed_path,
                legacy_share,
                &snapshot_name,
                &snapshot_id,
                &latest_payload,
            )
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

    /// Upload the thumbnail pack that rides with a personal photograph.
    async fn upload_compaction_thumb_pack(&self, snapshot: &Snapshot, snapshot_name: &str) {
        let pack_id = snapshot_name.to_string();
        match self
            .upload_thumb_pack_for_snapshot(snapshot, &pack_id)
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

    /// Upload the sealed photograph, then CAS `latest` to it.
    async fn upload_and_publish_photograph(
        &self,
        target: &SyncTarget,
        sealed_path: &std::path::Path,
        legacy_share: bool,
        snapshot_name: &str,
        snapshot_id: &str,
        latest_payload: &LatestCasPayload,
    ) -> SyncResult<()> {
        let publish_counter = latest_payload.counter;
        if legacy_share {
            if self.config.snapshot_retention > 0 {
                self.upload_snapshot_file_for_target_with_retry(
                    &format!("upload snapshot '{}' {snapshot_name}", target.label),
                    target,
                    snapshot_name,
                    sealed_path,
                )
                .await?;
            }
            self.upload_snapshot_file_for_target_with_retry(
                &format!("upload snapshot '{}' latest.enc", target.label),
                target,
                "latest.enc",
                sealed_path,
            )
            .await?;
            return Ok(());
        }
        self.upload_snapshot_file_for_target_with_retry(
            &format!("upload photograph '{}' {snapshot_name}", target.label),
            target,
            snapshot_name,
            sealed_path,
        )
        .await?;
        if let Err(e) = self
            .auth
            .confirm_snapshot_upload_for_target(target, snapshot_name)
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
            .photograph_latest_cas_for_target(target, latest_payload)
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
}
