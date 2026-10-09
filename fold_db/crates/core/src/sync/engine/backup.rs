//! Explicit snapshot backup + undecryptable-row scrub.

use super::super::error::{SyncError, SyncResult};
use super::super::snapshot::{ScrubReport, Snapshot, UndecryptableRow};
use super::*;

impl SyncEngine {
    /// Upload a sealed snapshot file; **re-presign on every attempt** so a
    /// multi-minute multi-GiB PUT does not reuse a cold URL across retries.
    pub(crate) async fn upload_snapshot_file_with_retry(
        &self,
        label: &str,
        snapshot_name: &str,
        path: &std::path::Path,
    ) -> SyncResult<()> {
        self.retry_s3(label, || {
            let name = snapshot_name.to_string();
            let path = path.to_path_buf();
            async move {
                let url = self.auth.presign_snapshot_upload(&name).await?;
                self.s3.upload_snapshot_file(&url, &path).await
            }
        })
        .await
    }

    /// Like [`Self::upload_snapshot_file_with_retry`] for org/share targets.
    pub(crate) async fn upload_snapshot_file_for_target_with_retry(
        &self,
        label: &str,
        target: &crate::sync::org_sync::SyncTarget,
        snapshot_name: &str,
        path: &std::path::Path,
    ) -> SyncResult<()> {
        self.retry_s3(label, || {
            let target = target.clone();
            let name = snapshot_name.to_string();
            let path = path.to_path_buf();
            async move {
                let url = self
                    .auth
                    .presign_snapshot_upload_for_target(&target, &name)
                    .await?;
                self.s3.upload_snapshot_file(&url, &path).await
            }
        })
        .await
    }

    /// Dual-upload sealed checkpoint bytes from a temp file (`{seq}.enc` then
    /// `latest.enc`) without `sealed.clone()` of a multi-hundred-MB buffer.
    pub(crate) async fn upload_sealed_snapshot_pair_from_path(
        &self,
        sealed_path: &std::path::Path,
        seq_name: &str,
    ) -> SyncResult<()> {
        self.upload_snapshot_file_with_retry(
            &format!("upload snapshot {seq_name}"),
            seq_name,
            sealed_path,
        )
        .await?;
        if let Err(e) = self.auth.confirm_snapshot_upload(seq_name).await {
            tracing::warn!(
                target: "fold_db::sync",
                error = %e,
                "confirm_snapshot_upload metering failed (non-fatal)"
            );
        }

        self.upload_snapshot_file_with_retry(
            "upload snapshot latest.enc",
            "latest.enc",
            sealed_path,
        )
        .await?;
        if let Err(e) = self.auth.confirm_snapshot_upload("latest.enc").await {
            tracing::warn!(
                target: "fold_db::sync",
                error = %e,
                "confirm_snapshot_upload metering failed for latest.enc (non-fatal)"
            );
        }
        Ok(())
    }

    /// Create a snapshot of the current local store and upload it to the cloud.
    ///
    /// The snapshot is sealed with the personal crypto provider, uploaded under
    /// both `snapshots/{seq}.enc` (point-in-time) and `snapshots/latest.enc`
    /// (the key read by `bootstrap_target(0)` on new-device restore). Unlike
    /// `compact`, this does NOT delete any log entries — it is an explicit
    /// backup checkpoint users or the CLI can trigger on demand.
    ///
    /// Returns the sequence number of the uploaded snapshot.
    pub async fn backup_snapshot(&self) -> SyncResult<u64> {
        // Hard interlock: intentional Cloud Sync Off forbids *all* cloud writes
        // immediately — including this legacy personal snapshot path. Without
        // this gate, outbox-overflow / staging-heal call sites in
        // `capture/worker.rs` could still force a real S3 PUT after the user
        // flipped Cloud Sync Off (cycle + backup_uploader already refuse).
        if !self.cloud_plane_allows_upload().await {
            return Err(SyncError::Storage(
                "cloud plane Off: legacy backup_snapshot refused (lastdb cloud off)".into(),
            ));
        }
        if !self.config.legacy_personal_cloud_sync {
            return Err(SyncError::Storage(
                "legacy personal snapshot upload is disabled for LastStore backup homes"
                    .to_string(),
            ));
        }
        match self.backup_snapshot_once().await {
            Ok(seq) => Ok(seq),
            Err(SyncError::Auth(_)) if self.auth_refresh.is_some() => {
                self.refresh_auth_once("backup_snapshot").await?;
                self.backup_snapshot_once().await
            }
            Err(e) => Err(e),
        }
    }

    pub(crate) async fn backup_snapshot_once(&self) -> SyncResult<u64> {
        // Defense in depth: same hard interlock as `backup_snapshot` so any
        // future direct caller cannot skip the Off gate.
        if !self.cloud_plane_allows_upload().await {
            return Err(SyncError::Storage(
                "cloud plane Off: legacy backup_snapshot refused (lastdb cloud off)".into(),
            ));
        }
        // Poison gate: `latest.enc` is what NEW devices bootstrap from, so a
        // wrong-key overwrite here is the highest-blast-radius poison of all.
        // Prove the current key still opens the existing personal prefix before
        // sealing and overwriting the snapshot. No-op (Ok) on a genuinely empty
        // prefix (first backup).
        let targets_snapshot = self.targets.lock().await.clone();
        let personal_target = targets_snapshot[0].clone();
        self.prove_prefix_decryptable(&personal_target).await?;

        // Seq seed, not a bare read. `self.seq` is a runtime counter that
        // starts at 0 in a fresh process and is only raised lazily — by
        // `outbox_meta()` (from the highest STAGED entry) or by the first
        // mint. Neither fires on a restart that snapshots before it writes,
        // and `outbox_meta()`'s floor is 0 anyway once the outbox has drained,
        // which is the steady state of a healthy node.
        //
        // The cut is what `latest.enc` is stamped with, and a restore replays
        // every cloud log ABOVE the snapshot's sequence. A snapshot published
        // at 0 therefore invites a replay of logs older than its own contents
        // — reversing a delete that only the snapshot captured. Seed from the
        // durable floor, and fail rather than publish a stale cut.
        let target_generation = self
            .target_config_generation
            .load(std::sync::atomic::Ordering::Acquire);
        let current_seq = self
            .seed_seq_from_durable_floor(target_generation, &targets_snapshot)
            .await
            .map_err(|e| {
                SyncError::Storage(format!(
                    "backup_snapshot refused: cannot seed the snapshot cut from the durable frontier: {e}"
                ))
            })?;
        tracing::info!(
            "backup_snapshot: creating snapshot at seq {} (device='{}')",
            current_seq,
            self.device_id,
        );

        // Ciphertext pass-through (Tom 2026-07-18 same-key rule): scan the
        // **raw** store (`cursor_store`) so we never decrypt every personal
        // row into logical plaintext before re-sealing under E2E. Values ride
        // as on-disk `ENC:…` envelopes; outer cloud seal still uses content key.
        // Fall back to logical create_reporting only if raw path is unavailable
        // (tests that never call set_cursor_store share store==cursor_store).
        let snapshot = Snapshot::create_at_rest_passthrough(
            self.cursor_store.as_ref(),
            &self.device_id,
            current_seq,
        )
        .await?;
        tracing::info!(
            target: "fold_db::sync",
            device = %self.device_id,
            value_encoding = "at_rest_enc",
            namespaces = snapshot.namespaces.len(),
            "backup_snapshot: ciphertext pass-through checkpoint (no decrypt-all)"
        );
        let namespace_count = snapshot.namespaces.len();
        let seq_name = format!("{current_seq}.enc");

        // Stream seal to disk: dual-upload from file so peak is O(one ciphertext
        // buffer) not O(ciphertext × 2) from sealed.clone().
        let sealed_dir = std::env::temp_dir().join("lastdb-snapshot-seal");
        let sealed_path = sealed_dir.join(format!(
            "{}-{}-{}.enc",
            self.device_id,
            current_seq,
            std::process::id()
        ));
        let sealed_bytes = snapshot
            .seal_to_path(&self.crypto, &sealed_path)
            .await
            .inspect_err(|_| {
                let _ = std::fs::remove_file(&sealed_path);
            })?;
        tracing::info!(
            target: "fold_db::sync::memory",
            path = %sealed_path.display(),
            sealed_bytes,
            "backup_snapshot: sealed checkpoint to temp file"
        );

        // Thumb pack needs the in-memory snapshot; run before drop. at_rest_enc
        // thumbs often no-op (values are ENC: envelopes) — non-fatal either way.
        match self
            .upload_thumb_pack_for_snapshot(&snapshot, &seq_name)
            .await
        {
            Ok(Some(count)) => tracing::info!(
                target: "fold_db::sync",
                pack_id = %seq_name,
                thumbnails = count,
                "backup_snapshot: uploaded thumbnail pack"
            ),
            Ok(None) => {}
            Err(e) => tracing::warn!(
                target: "fold_db::sync",
                pack_id = %seq_name,
                error = %e,
                "backup_snapshot: thumbnail pack upload failed (non-fatal)"
            ),
        }
        let upload_result = self
            .upload_sealed_snapshot_pair_from_path(&sealed_path, &seq_name)
            .await;
        let _ = tokio::fs::remove_file(&sealed_path).await;
        upload_result?;
        // Pass-through capture does not decrypt rows; scrub report is clean
        // (poison rows, if any, ride as opaque ENC: bytes under content key).
        let scrub = super::super::snapshot::SnapshotScrubReport::default();
        *self.last_snapshot_completion.lock().await = SnapshotCompletionStatus::from(&scrub);

        // C3: successful snapshot clears retired export-baseline capture pending to the
        // sealed snapshot payload. Re-reading the live store here can
        // incorrectly baseline writes that raced after snapshot enumeration.
        if let Err(e) = self.capture_reset_watermark_from_snapshot(&snapshot).await {
            tracing::warn!(
                error = %e,
                "capture pending clear after backup_snapshot failed (non-fatal)"
            );
        }
        drop(snapshot);

        tracing::info!(
            "backup_snapshot: uploaded {} namespaces at seq {} as 'latest.enc' and '{}' (device='{}')",
            namespace_count,
            current_seq,
            seq_name,
            self.device_id,
        );
        Ok(current_seq)
    }

    /// Enumerate every local at-rest **poison** row — a row whose value cannot
    /// be decrypted with any registered key — across all snapshot-eligible
    /// namespaces, and, when `delete` is set, remove them.
    ///
    /// Poison rows are unrecoverable: the plaintext is gone, so deleting them
    /// loses nothing that is not already lost, and it clears the rows that a
    /// non-reporting scan would otherwise trip over. This is a maintenance /
    /// repair operation:
    ///
    /// - `delete == false` → **read-only**: scans and returns a report of what
    ///   it found, mutating nothing. Safe to run for an audit.
    /// - `delete == true` → deletes each undecryptable row from the **local**
    ///   store only. It never touches the cloud (deletes go straight through
    ///   the encrypting namespaced store, which does not record a sync op).
    ///
    /// The returned [`ScrubReport`] lists each undecryptable row by namespace +
    /// key and, for a deleting run, how many were removed.
    pub async fn scrub_undecryptable_rows(&self, delete: bool) -> SyncResult<ScrubReport> {
        use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};

        let ns_names = self.store.list_namespaces().await?;
        let mut report = ScrubReport {
            delete_requested: delete,
            ..Default::default()
        };

        for ns_name in &ns_names {
            if snapshot_should_skip_namespace(ns_name) {
                continue;
            }
            report.scanned_namespaces += 1;

            let kv = self.store.open_namespace(ns_name).await?;
            let scan = kv.scan_prefix_partition_undecryptable(&[]).await?;

            for key in scan.undecryptable {
                report.undecryptable.push(UndecryptableRow {
                    namespace: ns_name.clone(),
                    key_b64: BASE64.encode(&key),
                });
                if delete {
                    match kv.delete(&key).await {
                        Ok(_) => report.deleted += 1,
                        Err(e) => {
                            // Best-effort: one failed delete must not abort the
                            // whole scrub. It is surfaced in the report (found >
                            // deleted) and logged.
                            tracing::error!(
                                namespace = %ns_name,
                                error = %e,
                                "scrub_undecryptable_rows: failed to delete undecryptable row"
                            );
                        }
                    }
                }
            }
        }

        if report.is_clean() {
            tracing::info!(
                scanned_namespaces = report.scanned_namespaces,
                "scrub_undecryptable_rows: no undecryptable rows found — local store is clean"
            );
        } else {
            tracing::warn!(
                found = report.found(),
                deleted = report.deleted,
                delete_requested = report.delete_requested,
                scanned_namespaces = report.scanned_namespaces,
                sample = ?report.undecryptable.iter().take(10).collect::<Vec<_>>(),
                "scrub_undecryptable_rows: found {} undecryptable at-rest row(s){}",
                report.found(),
                if delete {
                    format!(" — deleted {}", report.deleted)
                } else {
                    " — read-only (pass delete = true to remove)".to_string()
                },
            );
        }

        Ok(report)
    }

    // =========================================================================
    // Lock management
    // =========================================================================
}
