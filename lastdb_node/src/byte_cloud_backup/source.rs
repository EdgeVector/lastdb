use super::{model::Saved, BackupError, Phase, Result};
use cloud_backup_protocol::recovery::RecoveryProfile;
use cloud_backup_protocol::v2::Destination;
use cloud_backup_protocol::{AccountId, Digest, Id, RecoveryDescriptor};
use fold_db::crypto::E2eKeys;
use fold_db::storage::disk_pack::PackRuntime;
use std::sync::Arc;
use zeroize::Zeroizing;

pub(super) struct Source {
    runtime: Arc<PackRuntime>,
    pub account: AccountId,
    pub source_home: Id,
    pub recovery: RecoveryDescriptor,
    pub key: Zeroizing<[u8; 32]>,
    pub publication_stream: Option<Id>,
    pub retain_source_manifest: bool,
}
impl Source {
    pub fn from_host(host: &crate::host::Host) -> Result<Self> {
        let seed = Zeroizing::new(host.keypair.secret_key_bytes());
        let keys = E2eKeys::from_ed25519_seed(&seed).map_err(|_| BackupError::Identity)?;
        let runtime = host.db.db_ops().native_runtime();
        Ok(Self {
            account: AccountId::try_from(host.user_hash.clone())?,
            source_home: Id::try_from(runtime.native_home_id().to_string())?,
            recovery: RecoveryDescriptor {
                profile: RecoveryProfile::MiniPortableV1,
                account_public_key: Digest::try_from(fold_db::hex::hex_lower(
                    host.keypair.public_key_bytes(),
                ))?,
            },
            runtime,
            key: Zeroizing::new(keys.encryption_key()),
            publication_stream: None,
            retain_source_manifest: false,
        })
    }

    pub fn from_stopped(
        runtime: Arc<PackRuntime>,
        keypair: &fold_db::security::Ed25519KeyPair,
        account: AccountId,
    ) -> Result<Self> {
        let seed = Zeroizing::new(keypair.secret_key_bytes());
        let keys = E2eKeys::from_ed25519_seed(&seed).map_err(|_| BackupError::Identity)?;
        Ok(Self {
            source_home: Id::try_from(runtime.native_home_id().to_string())?,
            runtime,
            account,
            recovery: RecoveryDescriptor {
                profile: RecoveryProfile::MiniPortableV1,
                account_public_key: Digest::try_from(fold_db::hex::hex_lower(
                    keypair.public_key_bytes(),
                ))?,
            },
            key: Zeroizing::new(keys.encryption_key()),
            publication_stream: None,
            retain_source_manifest: true,
        })
    }

    pub fn from_publication_stopped(
        home: &std::path::Path,
        identity: crate::share_publication::PublicationIdentity,
    ) -> Result<Self> {
        let runtime = fold_db::storage::disk_pack::open_backup_runtime(
            &home.join("data"),
            &identity.content_key,
        )?;
        Ok(Self {
            source_home: Id::try_from(runtime.native_home_id().to_string())?,
            runtime,
            account: identity.account,
            // Keep the ordinary native format. Guests supply the publication key
            // separately instead of deriving the ordinary owner's content key.
            recovery: RecoveryDescriptor {
                profile: RecoveryProfile::MiniPortableV1,
                account_public_key: identity.publisher,
            },
            key: identity.content_key,
            publication_stream: Some(identity.id),
            retain_source_manifest: true,
        })
    }
    pub fn saved(&self, request_id: Id, endpoint: String) -> Saved {
        Saved {
            format: 2,
            request_id,
            work: super::work::Work::default(),
            automatic_revision: None,
            local_action: None,
            endpoint,
            account: self.account.clone(),
            source_home: self.source_home.clone(),
            begin: None,
            begin_submitted: false,
            commit: None,
            accepted: None,
            phase: Phase::Preparing,
            objects_verified: 0,
            metadata_pack_objects: 0,
            multi_file_pack_objects: 0,
            native_files: 0,
            native_bytes: 0,
            retained_source_bytes: None,
            blocked_reason: None,
        }
    }
    pub fn validate(&self, saved: &Saved) -> Result<()> {
        saved.validate()?;
        if saved.account != self.account || saved.source_home != self.source_home {
            return Err(BackupError::Identity);
        }
        if let Some(begin) = &saved.begin {
            if begin.required_destinations != [Destination::R2, Destination::B2]
                || self.publication_stream.as_ref().is_some_and(|id| {
                    &begin.job.stream.stream_id != id || begin.r2_multipart.is_some()
                })
            {
                return Err(BackupError::Identity);
            }
        }
        Ok(())
    }
    /// Snapshot jobs queue every local record file version without an
    /// upload marker, so versions from before the markers upload once.
    pub async fn queue_unuploaded_blobs(&self) -> Result<u64> {
        Ok(self.runtime.queue_unuploaded_blobs().await?)
    }
    pub async fn pending_blob_uploads(
        &self,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<fold_db::storage::disk_pack::BlobRow>> {
        Ok(self
            .runtime
            .pending_blob_uploads(&self.source_home, after, limit)
            .await?)
    }
    pub async fn pending_blob_releases(
        &self,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<fold_db::storage::disk_pack::BlobRow>> {
        Ok(self
            .runtime
            .pending_blob_releases(&self.source_home, after, limit)
            .await?)
    }
    pub async fn blob_bytes(&self, key: &str) -> Result<Option<Vec<u8>>> {
        Ok(self.runtime.record_blob_bytes(key, &self.key).await?)
    }
    pub async fn blobs_accepted(&self, uploaded: &[String], released: &[String]) -> Result<()> {
        Ok(self.runtime.blobs_accepted(uploaded, released).await?)
    }
    pub fn runtime(&self) -> &PackRuntime {
        &self.runtime
    }
    pub async fn has_external_backup_files(&self) -> Result<bool> {
        Ok(self.runtime.has_external_backup_files().await?)
    }
    pub async fn flush(&self) -> Result<()> {
        self.runtime.flush_native().await?;
        Ok(())
    }
    pub async fn prepare(
        &self,
        saved: &Saved,
        store: &super::store::Store,
    ) -> Result<super::capture::Capture> {
        use super::{capture::Capture, work::Work};
        let begin = saved.begin.as_ref().ok_or(BackupError::Invalid)?;
        saved.work.validate(Some(begin))?;
        match &saved.work {
            Work::Snapshot { minimum } => {
                let capture = if saved.begin_submitted {
                    self.runtime
                        .prepare_record_backup_capture(
                            begin.clone(),
                            &self.key,
                            self.recovery.clone(),
                        )
                        .await?
                } else {
                    self.runtime
                        .prepare_unsubmitted_record_backup_capture(
                            begin.clone(),
                            &self.key,
                            self.recovery.clone(),
                        )
                        .await?
                };
                let profile = capture.run_profile()?;
                if minimum
                    .as_ref()
                    .is_some_and(|min| profile.main.frontier.get() < min.main)
                {
                    return Err(BackupError::Identity);
                }
                Ok(Capture::Snapshot(capture))
            }
            Work::MutationLog { context } => {
                let core = self
                    .runtime
                    .prepare_mutation_log_capture(begin, &context.from)?;
                Ok(Capture::Log(Box::new(
                    super::log_capture::LogCapture::prepare(
                        core,
                        store,
                        context,
                        &self.recovery,
                        &self.key,
                    )?,
                )))
            }
        }
    }
    pub async fn resume(
        &self,
        saved: &Saved,
        store: &super::store::Store,
    ) -> Result<super::capture::Capture> {
        match &saved.work {
            super::work::Work::Snapshot { .. } => Ok(super::capture::Capture::Snapshot(
                self.runtime.resume_byte_backup_capture(&self.key).await?,
            )),
            super::work::Work::MutationLog { context } => {
                let core = self
                    .runtime
                    .resume_mutation_log_capture(&saved.request_id)?;
                Ok(super::capture::Capture::Log(Box::new(
                    super::log_capture::LogCapture::prepare(
                        core,
                        store,
                        context,
                        &self.recovery,
                        &self.key,
                    )?,
                )))
            }
        }
    }
    pub async fn cancel(&self, saved: &Saved) -> Result<()> {
        let begin = saved.begin.as_ref().ok_or(BackupError::Invalid)?;
        match &saved.work {
            super::work::Work::Snapshot { .. } => {
                self.runtime
                    .cancel_unsubmitted_byte_backup_capture(begin, &self.key)
                    .await?;
            }
            super::work::Work::MutationLog { .. } => self
                .runtime
                .cancel_unsubmitted_mutation_log_capture(begin)?,
        }
        Ok(())
    }
    pub fn cancel_replacement(&self, saved: &Saved) -> Result<()> {
        self.validate(saved)?;
        if saved.local_action != Some(super::model::LocalAction::ReplaceWithSnapshot) {
            return Err(BackupError::Invalid);
        }
        self.runtime
            .cancel_unmaterialized_mutation_log_capture(&saved.request_id)?;
        Ok(())
    }
}
