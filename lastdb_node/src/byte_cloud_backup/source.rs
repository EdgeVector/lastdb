use super::{model::Saved, BackupError, Phase, Result};
use cloud_backup_protocol::recovery::RecoveryProfile;
use cloud_backup_protocol::{AccountId, Digest, Id, RecoveryDescriptor};
use fold_db::crypto::E2eKeys;
use fold_db::storage::disk_pack::byte_backup::{ByteBackupCapture, StandaloneByteBackupSource};
use fold_db::storage::disk_pack::PackRuntime;
use std::path::PathBuf;
use std::sync::Arc;
use zeroize::Zeroizing;

pub(super) struct Source {
    pub home: PathBuf,
    backend: Backend,
    pub account: AccountId,
    pub source_home: Id,
    pub recovery: RecoveryDescriptor,
    pub key: Zeroizing<[u8; 32]>,
}
enum Backend {
    Live(Arc<PackRuntime>),
    Standalone {
        source: StandaloneByteBackupSource,
        identity: super::standalone::identity::Identity,
    },
}
impl Source {
    pub fn from_host(host: &crate::host::Host) -> Result<Self> {
        let seed = Zeroizing::new(host.keypair.secret_key_bytes());
        let keys = E2eKeys::from_ed25519_seed(&seed).map_err(|_| BackupError::Identity)?;
        let runtime = host
            .db
            .db_ops()
            .namespaced_store()
            .disk_pack_runtime()
            .ok_or(BackupError::Source)?;
        Ok(Self {
            home: host.home.clone(),
            account: AccountId::try_from(host.user_hash.clone())?,
            source_home: Id::try_from(runtime.native_home_id().to_string())?,
            recovery: RecoveryDescriptor {
                profile: RecoveryProfile::MiniPortableV1,
                account_public_key: Digest::try_from(fold_db::hex::hex_lower(
                    host.keypair.public_key_bytes(),
                ))?,
            },
            backend: Backend::Live(runtime),
            key: Zeroizing::new(keys.encryption_key()),
        })
    }
    pub fn saved(&self, request_id: Id, endpoint: String) -> Saved {
        Saved {
            format: 2,
            request_id,
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
            standalone: match &self.backend {
                Backend::Live(_) => None,
                Backend::Standalone { .. } => Some(super::model::StandaloneBinding {
                    source_home: self.home.clone(),
                    native_root: None,
                    capture_started_at: None,
                }),
            },
        }
    }
    pub fn validate(&self, saved: &Saved) -> Result<()> {
        saved.validate()?;
        if saved.account != self.account || saved.source_home != self.source_home {
            return Err(BackupError::Identity);
        }
        if let Some(begin) = &saved.begin {
            let descriptor = fold_db::storage::disk_pack::byte_backup::Bootstrap {
                recovery: self.recovery.clone(),
            }
            .descriptor()?;
            if begin.descriptor != descriptor {
                return Err(BackupError::Identity);
            }
        }
        match (&self.backend, &saved.standalone) {
            (Backend::Live(_), None) => {}
            (Backend::Standalone { identity, .. }, Some(binding))
                if binding.source_home == self.home =>
            {
                identity.check()?;
            }
            _ => return Err(BackupError::Identity),
        }
        Ok(())
    }
    pub fn standalone(
        identity: super::standalone::identity::Identity,
        source: StandaloneByteBackupSource,
    ) -> Result<Self> {
        let keys =
            E2eKeys::from_ed25519_seed(identity.seed()).map_err(|_| BackupError::Identity)?;
        Ok(Self {
            home: identity.home().to_path_buf(),
            account: identity.account().clone(),
            source_home: Id::try_from(source.native_home_id().to_string())?,
            recovery: identity.recovery().clone(),
            key: Zeroizing::new(keys.encryption_key()),
            backend: Backend::Standalone { source, identity },
        })
    }
    pub async fn flush(&self) -> Result<()> {
        if let Backend::Live(runtime) = &self.backend {
            runtime.flush_native().await?;
        }
        Ok(())
    }
    pub async fn prepare(
        &self,
        begin: cloud_backup_protocol::v2::Begin,
    ) -> Result<ByteBackupCapture> {
        Ok(match &self.backend {
            Backend::Live(runtime) => {
                runtime
                    .prepare_byte_backup_capture(begin, &self.key)
                    .await?
            }
            Backend::Standalone { source, identity } => {
                identity.check()?;
                let capture = source.prepare(begin, &self.key).await?;
                identity.check()?;
                capture
            }
        })
    }
    pub async fn resume(&self) -> Result<ByteBackupCapture> {
        Ok(match &self.backend {
            Backend::Live(runtime) => runtime.resume_byte_backup_capture(&self.key).await?,
            Backend::Standalone { source, identity } => {
                identity.check()?;
                let capture = source.resume(&self.key).await?;
                identity.check()?;
                capture
            }
        })
    }
    pub async fn cancel(&self, begin: &cloud_backup_protocol::v2::Begin) -> Result<()> {
        match &self.backend {
            Backend::Live(runtime) => {
                runtime
                    .cancel_unsubmitted_byte_backup_capture(begin, &self.key)
                    .await?;
            }
            Backend::Standalone { source, identity } => {
                identity.check()?;
                source.cancel_unsubmitted(begin, &self.key).await?;
            }
        }
        Ok(())
    }
}
