use super::{model::Saved, BackupError, Phase, Result};
use cloud_backup_protocol::recovery::RecoveryProfile;
use cloud_backup_protocol::{AccountId, Digest, Id, RecoveryDescriptor};
use fold_db::crypto::E2eKeys;
use fold_db::storage::disk_pack::PackRuntime;
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
        Ok(())
    }
}
