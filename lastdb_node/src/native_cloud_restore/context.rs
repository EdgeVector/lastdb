//! Client-owned native validation after a transport proves its complete closure.
use super::{RecoveryKeys, RestoreReport, Result};
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
    pub dependencies: u64,
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeFileProof {
    pub path: String,
    pub sha256: Digest,
    pub bytes: u64,
}

impl VerifiedNativeContext<'_> {
    pub fn physical(&self) -> PathBuf {
        self.root.join("home/data/disk-pack")
    }
    pub fn data_root(&self) -> PathBuf {
        self.root.join("home/data")
    }
    pub fn verify_records(&self, key: &[u8; 32], external_count: u64) -> Result<u64> {
        super::records::verify_context(self, key, external_count)
    }
    pub fn verify_metadata(&self, key: &[u8; 32]) -> Result<()> {
        super::records::verify_metadata_context(self, key)
    }
    pub fn published(&self) -> Result<Option<RestoreReport>> {
        super::install::published_context(self)
    }
    pub fn finish(
        &self,
        keys: &RecoveryKeys,
        inventory: NativeInventory,
        atoms: u64,
        entry: impl Fn(&str) -> Result<NativeFileProof>,
    ) -> Result<RestoreReport> {
        super::install::finish_context(self, keys, inventory, atoms, entry)
    }
}

pub(crate) fn check_capacity(root: &Path, bytes: u64) -> Result<()> {
    super::download::check_capacity(root, bytes)
}
pub(crate) fn verify_file(path: &Path, sha: &Digest, bytes: u64) -> Result<()> {
    super::download::verify_file(path, sha, bytes)
}
pub(crate) fn bounded_bytes(path: &Path, maximum: u64) -> Result<Vec<u8>> {
    super::download::bytes(path, maximum)
}
