use super::{model::Saved, store::Store, BackupError, Phase, Result};
use cloud_backup_protocol::recovery::RecoveryProfile;
use cloud_backup_protocol::{AccountId, CloudScope, DecimalU64, Digest, Id, RecoveryDescriptor};
use fold_db::crypto::E2eKeys;
use fold_db::storage::disk_pack::PackRuntime;
use fold_db::sync::auth::native_backup::NativeBackupClient;
use std::path::PathBuf;
use std::sync::Arc;
use zeroize::Zeroizing;

pub(super) struct Source {
    pub home: PathBuf,
    pub runtime: Arc<PackRuntime>,
    pub account: AccountId,
    pub source_home: Id,
    pub recovery: RecoveryDescriptor,
    pub key: Zeroizing<[u8; 32]>,
}
impl Source {
    pub fn from_host(host: &crate::host::Host) -> Result<Self> {
        let account = AccountId::try_from(host.user_hash.clone())?;
        let recovery = RecoveryDescriptor {
            profile: RecoveryProfile::MiniPortableV1,
            account_public_key: Digest::try_from(fold_db::hex::hex_lower(
                host.keypair.public_key_bytes(),
            ))?,
        };
        recovery.validate_for(&CloudScope {
            account_id: account.clone(),
            account_generation: DecimalU64::new(0),
        })?;
        let seed = Zeroizing::new(host.keypair.secret_key_bytes());
        let keys = E2eKeys::from_ed25519_seed(&seed).map_err(|_| BackupError::Identity)?;
        let runtime = host
            .db
            .db_ops()
            .namespaced_store()
            .disk_pack_runtime()
            .ok_or(BackupError::Source)?;
        let source_home = Id::try_from(runtime.native_home_id().to_string())?;
        Ok(Self {
            home: host.home.clone(),
            runtime,
            account,
            source_home,
            recovery,
            key: Zeroizing::new(keys.encryption_key()),
        })
    }
    pub fn saved(&self, request_id: Id) -> Saved {
        Saved {
            format: 1,
            request_id,
            account: self.account.clone(),
            source_home: self.source_home.clone(),
            recovery: self.recovery.clone(),
            intent: None,
            begin: None,
            commit: None,
            phase: Phase::Preparing,
            native_objects_verified: 0,
            external_files_verified: 0,
            manifest_pages_verified: 0,
            native_files: None,
            native_bytes: None,
            metadata_pack_objects_verified: Some(0),
            multi_file_pack_objects_verified: Some(0),
            retained_source_bytes: None,
            blocked_reason: None,
        }
    }
    pub fn validate(&self, saved: &Saved) -> Result<()> {
        saved.validate()?;
        if saved.account != self.account
            || saved.source_home != self.source_home
            || saved.recovery != self.recovery
        {
            return Err(BackupError::Identity);
        }
        Ok(())
    }
    pub fn client(&self) -> Result<NativeBackupClient> {
        crate::native_backup_access::client_from_home(&self.home).map_err(|_| BackupError::Identity)
    }
    pub fn coordinator_bytes(&self, store: &Store) -> Result<u64> {
        store
            .bytes()
            .checked_add(super::store::CONTROL_RESERVE)
            .ok_or(BackupError::Limit)
    }
}
