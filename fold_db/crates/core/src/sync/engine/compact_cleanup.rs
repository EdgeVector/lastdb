//! Cleanup phases of `SyncEngine::compact_target`: prune the logs, photographs
//! and thumbnail packs the published cut supersedes. All best-effort.

use super::super::org_sync::SyncTarget;
use super::*;
use crate::sync::snapshot_log::Frontier;

impl SyncEngine {
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
