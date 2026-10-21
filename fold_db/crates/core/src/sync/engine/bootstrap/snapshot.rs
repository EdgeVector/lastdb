//! Snapshot leg of a target bootstrap: download, unseal, restore.

use super::*;
use crate::sync::auth::S3ObjectInfo;
use crate::sync::snapshot_log::Frontier;

/// What `fetch_bootstrap_snapshot` found in the cloud for one target.
pub(super) struct BootstrapSnapshotFetch {
    pub(super) snapshot_data: Option<Vec<u8>>,
    pub(super) snapshot_download_ms: u64,
    pub(super) snapshot_present: bool,
    pub(super) log_objects: Option<Vec<S3ObjectInfo>>,
    pub(super) legacy_personal_root: bool,
}

impl SyncEngine {
    /// Download the target's snapshot (named photograph, else `latest.enc`,
    /// else the legacy personal root) and the log listing that goes with it.
    pub(super) async fn fetch_bootstrap_snapshot(
        &self,
        idx: usize,
        target: &SyncTarget,
        photograph_snapshot_id: Option<&str>,
    ) -> SyncResult<BootstrapSnapshotFetch> {
        let mut snapshot_download_ms = 0u64;
        let mut snapshot_data = if let Some(snapshot_id) = photograph_snapshot_id {
            let snapshot_name = format!("{snapshot_id}.enc");
            let snapshot_url = self
                .auth
                .presign_snapshot_download_for_target(target, &snapshot_name)
                .await?;
            let download_started = Instant::now();
            let data = self.s3.download(&snapshot_url).await?;
            snapshot_download_ms =
                snapshot_download_ms.saturating_add(elapsed_ms(download_started));
            let Some(data) = data else {
                return Err(SyncError::Storage(format!(
                    "latest names photograph {snapshot_id} but snapshots/{snapshot_name} is missing"
                )));
            };
            let actual = sha256_hex(&data);
            if actual != snapshot_id {
                return Err(SyncError::Crypto(format!(
                    "photograph id mismatch: latest={snapshot_id} downloaded={actual}"
                )));
            }
            Some(data)
        } else {
            let snapshot_url = self
                .auth
                .presign_snapshot_download_for_target(target, "latest.enc")
                .await?;
            let download_started = Instant::now();
            let data = self.s3.download(&snapshot_url).await?;
            snapshot_download_ms =
                snapshot_download_ms.saturating_add(elapsed_ms(download_started));
            data
        };
        let mut snapshot_present = snapshot_data.is_some();
        let mut log_objects = None;
        let mut legacy_personal_root = false;

        if !snapshot_present {
            let db_hash_log_objects = self.auth.list_log_objects(target).await?;
            if idx == 0 && target.prefix.is_empty() && db_hash_log_objects.is_empty() {
                let legacy_snapshot_url = self
                    .auth
                    .presign_snapshot_download_legacy_personal("latest.enc")
                    .await?;
                let download_started = Instant::now();
                let legacy_snapshot_data = self.s3.download(&legacy_snapshot_url).await?;
                snapshot_download_ms =
                    snapshot_download_ms.saturating_add(elapsed_ms(download_started));
                let legacy_log_objects = self.auth.list_objects_legacy_personal("log/").await?;
                if legacy_snapshot_data.is_some() || !legacy_log_objects.is_empty() {
                    tracing::info!(
                        target: "fold_db::sync::bootstrap",
                        "db_hash root is empty; bootstrapping personal target from legacy principal root"
                    );
                    snapshot_data = legacy_snapshot_data;
                    snapshot_present = snapshot_data.is_some();
                    log_objects = Some(legacy_log_objects);
                    legacy_personal_root = true;
                } else {
                    log_objects = Some(db_hash_log_objects);
                }
            } else {
                log_objects = Some(db_hash_log_objects);
            }
        }
        Ok(BootstrapSnapshotFetch {
            snapshot_data,
            snapshot_download_ms,
            snapshot_present,
            log_objects,
            legacy_personal_root,
        })
    }

    /// Run the restore barrier BEFORE a personal restore installs rows, not only
    /// after, so the serving store does not hold state of the home that the
    /// restore replaces.
    async fn prepare_serving_store_before_restore(&self, target: &SyncTarget) -> SyncResult<()> {
        if !target.prefix.is_empty() {
            return Ok(());
        }
        self.invoke_photograph_restore_barrier()
            .await
            .map_err(|error| {
                SyncError::Storage(format!(
                    "prepare serving store before photograph restore: {error}"
                ))
            })
    }

    /// Unseal one snapshot and restore it into the right store. Returns the
    /// snapshot's `last_seq`.
    pub(super) async fn restore_bootstrap_snapshot(
        &self,
        target: &SyncTarget,
        data: &[u8],
        photograph_frontier: Option<&Frontier>,
        snapshot_download_ms: u64,
    ) -> SyncResult<u64> {
        let unseal_started = Instant::now();
        let snapshot = Snapshot::unseal(data, &target.crypto).await?;
        let unseal_decode_ms = elapsed_ms(unseal_started);
        let last_seq = snapshot.last_seq;
        if let Some(frontier) = photograph_frontier {
            let named_through = frontier.max_through();
            if last_seq != named_through {
                return Err(SyncError::Storage(format!(
                    "photograph frontier mismatch: snapshot last_seq={last_seq}, latest F={named_through}"
                )));
            }
        }
        tracing::info!(
            "restoring snapshot for '{}': {} namespaces, last_seq={}",
            target.label,
            snapshot.namespaces.len(),
            last_seq
        );
        self.prepare_serving_store_before_restore(target).await?;
        let restore_report = self.restore_snapshot_into_store(target, &snapshot).await?;
        if target.prefix.is_empty() {
            self.invoke_photograph_restore_barrier()
                .await
                .map_err(|error| {
                    SyncError::Storage(format!(
                        "refresh serving store after photograph restore: {error}"
                    ))
                })?;
        }
        tracing::info!(
            target: "fold_db::sync::photograph",
            target_label = %target.label,
            snapshot_bytes = data.len(),
            namespaces = restore_report.namespaces,
            entries = restore_report.entries,
            batches = restore_report.batches,
            download_ms = snapshot_download_ms,
            unseal_decode_ms,
            materialize_ms = restore_report.materialize_ms,
            final_flush_ms = restore_report.final_flush_ms,
            restore_total_ms = restore_report.total_ms(),
            ingress_total_ms = snapshot_download_ms.saturating_add(unseal_decode_ms),
            "photograph restore phases complete"
        );
        if target.prefix.is_empty() {
            self.warm_thumb_cache_after_restore(&snapshot, last_seq)
                .await;
        }
        Ok(last_seq)
    }

    /// Restore the unsealed snapshot into the store this target writes to.
    async fn restore_snapshot_into_store(
        &self,
        target: &SyncTarget,
        snapshot: &Snapshot,
    ) -> SyncResult<crate::sync::snapshot::SnapshotRestoreReport> {
        let report = if target.prefix.is_empty() {
            // AtRestEnc checkpoints must land on the raw store so ENC:
            // envelopes are not double-sealed by EncryptingNamespacedStore.
            // Logical (historical) snapshots restore through the encrypting
            // seam so puts re-seal under local at-rest crypto.
            if snapshot.is_at_rest_enc() {
                tracing::info!(
                    target: "fold_db::sync",
                    "bootstrap: restoring at_rest_enc snapshot into raw store"
                );
                snapshot.restore(self.cursor_store.as_ref()).await?
            } else {
                snapshot.restore(self.store.as_ref()).await?
            }
        } else {
            let restore_scopes = self
                .target_restore_scopes
                .lock()
                .await
                .get(&target.prefix)
                .cloned()
                .unwrap_or_else(|| vec![target.prefix.clone()]);
            snapshot
                .restore_scoped_to_prefixes(self.store.as_ref(), &restore_scopes)
                .await?
        };
        Ok(report)
    }

    /// Warm the thumbnail cache from a restored snapshot. Failure is logged,
    /// never fatal.
    async fn warm_thumb_cache_after_restore(&self, snapshot: &Snapshot, last_seq: u64) {
        let pack_id = format!("{last_seq}.enc");
        match self.warm_thumb_cache_for_snapshot(snapshot, &pack_id).await {
            Ok(0) => {}
            Ok(count) => tracing::info!(
                target: "fold_db::sync",
                pack_id = %pack_id,
                thumbnails = count,
                "bootstrap warmed thumbnail cache"
            ),
            Err(e) => tracing::warn!(
                target: "fold_db::sync",
                pack_id = %pack_id,
                error = %e,
                "bootstrap thumbnail cache warm failed (non-fatal)"
            ),
        }
    }
}
