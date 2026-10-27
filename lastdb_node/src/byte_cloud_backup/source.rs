use super::{model::Saved, BackupError, Phase, Result};
use cloud_backup_protocol::recovery::RecoveryProfile;
use cloud_backup_protocol::v2::Destination;
use cloud_backup_protocol::{AccountId, Digest, Id, RecoveryDescriptor};
use fold_db::crypto::E2eKeys;
use fold_db::storage::disk_pack::byte_backup::ByteBackupCapture;
use fold_db::storage::disk_pack::PackRuntime;
use std::path::PathBuf;
use std::sync::Arc;
use zeroize::Zeroizing;

pub(super) struct Source {
    pub home: PathBuf,
    runtime: Arc<PackRuntime>,
    pub account: AccountId,
    pub source_home: Id,
    pub recovery: RecoveryDescriptor,
    pub key: Zeroizing<[u8; 32]>,
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
            runtime,
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
        }
    }
    pub fn validate(&self, saved: &Saved) -> Result<()> {
        saved.validate()?;
        if saved.account != self.account || saved.source_home != self.source_home {
            return Err(BackupError::Identity);
        }
        if let Some(begin) = &saved.begin {
            if begin.required_destinations != [Destination::R2, Destination::B2] {
                return Err(BackupError::Identity);
            }
        }
        Ok(())
    }
    pub async fn flush(&self) -> Result<()> {
        self.runtime.flush_native().await?;
        Ok(())
    }
    pub async fn prepare(
        &self,
        begin: cloud_backup_protocol::v2::Begin,
    ) -> Result<ByteBackupCapture> {
        Ok(self
            .runtime
            .prepare_record_backup_capture(begin, &self.key, self.recovery.clone())
            .await?)
    }
    pub async fn resume(&self) -> Result<ByteBackupCapture> {
        Ok(self.runtime.resume_byte_backup_capture(&self.key).await?)
    }
    pub async fn cancel(&self, begin: &cloud_backup_protocol::v2::Begin) -> Result<()> {
        self.runtime
            .cancel_unsubmitted_byte_backup_capture(begin, &self.key)
            .await?;
        Ok(())
    }
}
