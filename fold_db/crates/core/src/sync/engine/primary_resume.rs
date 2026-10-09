//! Read-only proof for a one-writer, primary-authoritative cloud resume.

use super::*;
use crate::storage::laststore::{manifest_sha256_hex, validate_manifest_chain, BackupManifest};
use crate::storage::traits::NamespacedStore;
use crate::sync::auth::ops::BackupLatestPointer;
use crate::sync::auth::AuthClient;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

const PRIMARY_RESUME_MAX_LOG_OBJECTS: usize = 500_000;

mod mirror;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrimaryResumeLogInventory {
    pub cloud_log_objects: usize,
    pub cloud_writer_frontier: u64,
    pub local_writer_frontier: u64,
    pub cloud_log_fingerprint: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrimaryResumeCutIdentity {
    pub store_uuid: String,
    pub epoch: u64,
    pub counter: u64,
    pub manifest_sha256: String,
    #[serde(default)]
    pub previous_latest: Option<BackupLatestPointer>,
    #[serde(default)]
    pub local_missing_atom_groups: u64,
}

impl PrimaryResumeCutIdentity {
    fn matches_latest(&self, latest: &BackupLatestPointer) -> bool {
        latest.store_uuid == self.store_uuid
            && latest.epoch == self.epoch
            && latest.counter == self.counter
            && latest
                .manifest_sha256
                .eq_ignore_ascii_case(&self.manifest_sha256)
    }

    pub(crate) fn matches_previous_latest(&self, latest: &BackupLatestPointer) -> bool {
        self.previous_latest.as_ref().is_some_and(|previous| {
            latest.store_uuid == previous.store_uuid
                && latest.epoch == previous.epoch
                && latest.counter == previous.counter
                && latest
                    .manifest_sha256
                    .eq_ignore_ascii_case(&previous.manifest_sha256)
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrimaryResumeCut {
    pub before: PrimaryResumeLogInventory,
    pub writer_frontier: u64,
    pub manifest: PrimaryResumeCutIdentity,
}

impl PrimaryResumeLogInventory {
    /// A second inventory after snapshot commit must name the same cloud logs.
    pub fn same_cloud_logs(&self, after: &Self) -> bool {
        self.cloud_log_objects == after.cloud_log_objects
            && self.cloud_writer_frontier == after.cloud_writer_frontier
            && self.cloud_log_fingerprint == after.cloud_log_fingerprint
    }
}

fn include_primary_log_key(key: &str, writer: &str, frontier: &mut u64) -> SyncResult<()> {
    let Some((Some(found_writer), seq)) = parse_mutation_log_object_key(key) else {
        return Err(SyncError::Storage(
            "primary resume refused: cloud log has an unknown writer or key layout".into(),
        ));
    };
    if found_writer != writer {
        return Err(SyncError::Storage(
            "primary resume refused: another cloud log writer exists".into(),
        ));
    }
    *frontier = (*frontier).max(seq);
    Ok(())
}

/// Inspect a stopped copy without changing its cloud configuration or the
/// cloud. The caller must open the copy with the local encryption key.
pub async fn inspect_primary_resume_plan(
    store: &dyn NamespacedStore,
    auth: &AuthClient,
    db_hash: &str,
    writer: &str,
) -> SyncResult<PrimaryResumeLogInventory> {
    let local_writer_frontier =
        super::pin_log::primary_resume_durable_local_frontier(store, writer)
            .await
            .map_err(SyncError::Storage)?;
    inspect_primary_resume_log_inventory(auth, db_hash, writer, local_writer_frontier).await
}

async fn inspect_primary_resume_log_inventory(
    auth: &AuthClient,
    db_hash: &str,
    writer: &str,
    local_writer_frontier: u64,
) -> SyncResult<PrimaryResumeLogInventory> {
    let mut cloud_writer_frontier = 0;
    let mut cloud_log_hasher = Sha256::new();
    let mut prior_key: Option<String> = None;
    let cloud_log_objects = auth
        .visit_db_log_objects_at_most(db_hash, PRIMARY_RESUME_MAX_LOG_OBJECTS, |objects| {
            for object in objects {
                if prior_key.as_ref().is_some_and(|prior| prior >= &object.key) {
                    return Err(SyncError::Storage(
                        "primary resume refused: cloud log inventory is not ordered".into(),
                    ));
                }
                include_primary_log_key(&object.key, writer, &mut cloud_writer_frontier)?;
                cloud_log_hasher.update((object.key.len() as u64).to_be_bytes());
                cloud_log_hasher.update(object.key.as_bytes());
                cloud_log_hasher.update(object.size.to_be_bytes());
                cloud_log_hasher.update((object.last_modified.len() as u64).to_be_bytes());
                cloud_log_hasher.update(object.last_modified.as_bytes());
                prior_key = Some(object.key.clone());
            }
            Ok(())
        })
        .await?;
    Ok(PrimaryResumeLogInventory {
        cloud_log_objects,
        cloud_writer_frontier,
        local_writer_frontier,
        cloud_log_fingerprint: format!("{:x}", cloud_log_hasher.finalize()),
    })
}

impl SyncEngine {
    /// Select ReplayTail at one writer frontier while the strict paused-home
    /// snapshot path verifies each referenced cloud file.
    pub(crate) async fn set_primary_resume_frontier(&self, frontier: u64) -> SyncResult<()> {
        if !self
            .backup_only_mode
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(SyncError::Storage(
                "primary resume cut requires backup-only mode".into(),
            ));
        }
        *self.primary_resume_frontier.lock().await = Some(frontier);
        Ok(())
    }

    /// Confirm that no cloud log changed across the snapshot upload, then
    /// store the frontier that prevents the primary from pulling old logs.
    pub(crate) async fn confirm_primary_resume_snapshot(
        &self,
        before: &PrimaryResumeLogInventory,
        frontier: u64,
    ) -> SyncResult<PrimaryResumeLogInventory> {
        if *self.primary_resume_frontier.lock().await != Some(frontier) {
            return Err(SyncError::Storage(
                "primary resume cut frontier changed before confirmation".into(),
            ));
        }
        if frontier < before.cloud_writer_frontier {
            return Err(SyncError::Storage(
                "primary resume cut is below the old cloud writer frontier".into(),
            ));
        }
        let after = self.inspect_primary_resume_logs().await?;
        if !before.same_cloud_logs(&after) {
            return Err(SyncError::Storage(
                "primary resume refused: cloud logs changed across the snapshot".into(),
            ));
        }
        if after.local_writer_frontier < frontier {
            return Err(SyncError::Storage(
                "primary resume cut is above the durable local writer frontier".into(),
            ));
        }
        self.pin_log
            .incorporate_applied_frontier(&BTreeMap::from([(self.device_id.clone(), frontier)]))
            .await
            .map_err(SyncError::Storage)?;
        let durable_applied = self
            .pin_log
            .read_published_f_strict("personal")
            .await
            .map_err(SyncError::Storage)?
            .get(&self.device_id)
            .copied()
            .unwrap_or(0);
        if durable_applied < frontier {
            return Err(SyncError::Storage(
                "primary resume applied frontier is not durable".into(),
            ));
        }
        Ok(after)
    }

    /// A retry can prove that the exact fenced cut reached backup/latest
    /// before its prior process stopped. The proof does not accept a newer or
    /// different pointer as a substitute.
    pub(crate) async fn recover_primary_resume_snapshot(
        &self,
        cut: &PrimaryResumeCut,
    ) -> SyncResult<PrimaryResumeLogInventory> {
        if !self.is_backup_only_mode() {
            return Err(SyncError::Storage(
                "primary resume recovery requires backup-only mode".into(),
            ));
        }
        let latest = self.auth.backup_latest_get().await?;
        let expected = &cut.manifest;
        latest.latest.require_v1_format()?;
        if !expected.matches_latest(&latest.latest) {
            return Err(SyncError::Storage(
                "primary resume cut is not the current cloud backup/latest".into(),
            ));
        }
        if !self
            .auth
            .require_backup_manifest_present(&expected.manifest_sha256)
            .await?
        {
            return Err(SyncError::Storage(
                "primary resume cut manifest is absent from cloud".into(),
            ));
        }
        let presigned = self
            .auth
            .presign_backup_manifest_download(&expected.manifest_sha256)
            .await?;
        let bytes = self
            .s3
            .download_limited(&presigned, Some(16 * 1024 * 1024))
            .await?
            .ok_or_else(|| SyncError::Storage("primary resume cut manifest is absent".into()))?;
        let digest = format!("{:x}", Sha256::digest(&bytes));
        if !digest.eq_ignore_ascii_case(&expected.manifest_sha256) {
            return Err(SyncError::Crypto(
                "primary resume cut manifest hash mismatch".into(),
            ));
        }
        let manifest: BackupManifest = serde_json::from_slice(&bytes).map_err(|error| {
            SyncError::Storage(format!("decode primary resume cut manifest: {error}"))
        })?;
        let canonical = manifest_sha256_hex(&manifest).map_err(|error| {
            SyncError::Storage(format!("hash primary resume manifest: {error}"))
        })?;
        if !canonical.eq_ignore_ascii_case(&expected.manifest_sha256)
            || manifest.store_uuid != expected.store_uuid
            || manifest.epoch != expected.epoch
            || manifest.counter != expected.counter
        {
            return Err(SyncError::Storage(
                "primary resume cut manifest identity mismatch".into(),
            ));
        }
        validate_manifest_chain(None, &manifest).map_err(|error| {
            SyncError::Storage(format!(
                "primary resume cut is not an independent root: {error}"
            ))
        })?;
        let source = self.laststore_backup_source.as_ref().ok_or_else(|| {
            SyncError::Storage("primary resume requires a LastStore backup source".into())
        })?;
        source
            .commit_backup_manifest(&manifest)
            .map_err(|error| SyncError::Storage(format!("commit recovered backup cut: {error}")))?;
        self.require_durable_primary_resume_mirror(&manifest)?;
        self.set_primary_resume_frontier(cut.writer_frontier)
            .await?;
        self.confirm_primary_resume_snapshot(&cut.before, cut.writer_frontier)
            .await
    }

    /// Called only after the normal config file and durable resume marker
    /// show that the new backup/latest is safe to use.
    pub(crate) async fn finish_primary_resume(&self) {
        *self.primary_resume_frontier.lock().await = None;
        self.set_cloud_sync_disabled(false).await;
        self.backup_only_mode
            .store(false, std::sync::atomic::Ordering::Release);
    }

    /// Read the highest durable local writer sequence. Call again under the
    /// mutation fence immediately before the snapshot cut.
    pub(crate) async fn primary_resume_local_writer_frontier(&self) -> SyncResult<u64> {
        let personal = self
            .targets
            .lock()
            .await
            .first()
            .cloned()
            .ok_or_else(|| SyncError::Storage("personal sync target is absent".into()))?;
        self.pin_log
            .durable_frontier_floor_for_writer(&self.device_id, &[personal], true)
            .await
            .map_err(SyncError::Storage)
    }

    /// Raise the durable allocation floor before the cut. A later local
    /// mutation must sort above every old cloud log, even after a restart.
    pub(crate) async fn raise_primary_resume_writer_floor(
        &self,
        old_cloud_frontier: u64,
    ) -> SyncResult<u64> {
        if !self.is_backup_only_mode() {
            return Err(SyncError::Storage(
                "primary resume floor raise requires backup-only mode".into(),
            ));
        }
        let personal = self
            .targets
            .lock()
            .await
            .first()
            .cloned()
            .ok_or_else(|| SyncError::Storage("personal sync target is absent".into()))?;
        let frontier = self
            .pin_log
            .raise_durable_frontier_floor_for_writer(
                &self.device_id,
                &[personal],
                old_cloud_frontier,
            )
            .await
            .map_err(SyncError::Storage)?;
        let mut seq = self.seq.lock().await;
        *seq = (*seq).max(frontier);
        Ok(frontier)
    }

    /// Inspect every cloud log key. No cloud write or peer replay occurs.
    /// The cut raises a lower local frontier before it publishes a snapshot.
    pub async fn inspect_primary_resume_logs(&self) -> SyncResult<PrimaryResumeLogInventory> {
        let source = self.laststore_backup_source.as_ref().ok_or_else(|| {
            SyncError::Storage("primary resume requires a LastStore backup source".into())
        })?;
        let db_hash = source
            .cloud_db_hash()
            .map_err(|error| SyncError::Storage(format!("read cloud database identity: {error}")))?
            .ok_or_else(|| SyncError::Storage("cloud database identity is absent".into()))?;
        let local_writer_frontier = self.primary_resume_local_writer_frontier().await?;

        inspect_primary_resume_log_inventory(
            &self.auth,
            &db_hash,
            &self.device_id,
            local_writer_frontier,
        )
        .await
    }
}
