//! Client-owned native validation after a transport proves its complete closure.
use super::{RestoreReport, Result};
use cloud_backup_protocol::{Digest, NativeRoot};
use std::path::{Path, PathBuf};

pub(crate) struct VerifiedNativeContext<'a> {
    pub root: &'a Path,
    pub destination: &'a Path,
    pub session_id: uuid::Uuid,
    pub commit_sha256: &'a Digest,
    pub native_root: &'a NativeRoot,
    pub receipt_name: &'static str,
    pub receipt_version: u8,
}

#[derive(Clone, Copy)]
pub(crate) struct NativeInventory {
    pub files: u64,
    pub bytes: u64,
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeFileProof {
    pub path: String,
    pub sha256: Digest,
    pub bytes: u64,
}

impl VerifiedNativeContext<'_> {
    pub fn data_root(&self) -> PathBuf {
        self.root.join("home/data")
    }
    pub fn published(&self) -> Result<Option<RestoreReport>> {
        super::publish::published_context(self)
    }
    pub fn finish_run(
        &self,
        identity_seed: &[u8; 32],
        inventory: NativeInventory,
        proof: &fold_db::storage::disk_pack::run_restore::RunRestoreGuard,
    ) -> Result<RestoreReport> {
        super::publish::finish_run_context(self, identity_seed, inventory, proof)
    }
}
