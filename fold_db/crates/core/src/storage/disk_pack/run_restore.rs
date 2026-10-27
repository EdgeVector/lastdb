//! Verify an inactive paired run home without a writer, cleanup, or conversion.
mod files;
mod node;
mod proof;

use super::byte_backup::RunBackupProfile;
use super::{StorageError, StorageResult};
use cloud_backup_protocol::NativeRoot;
use laststore::disk_run::home::RunRead;
use std::path::Path;

pub use node::{validate_restored_node, RunNodeGuard};

pub struct RunRestoreFile {
    pub path: String,
    pub bytes: u64,
}

/// Complete semantic proof retains the physical account guard until publication.
pub struct RunRestoreGuard {
    native: RunNodeGuard,
    atoms: u64,
}

impl RunRestoreGuard {
    pub fn verified_atoms(&self) -> u64 {
        self.atoms
    }

    pub fn files(&self) -> impl Iterator<Item = &RunRestoreFile> {
        self.native.files.iter()
    }

    pub fn totals(&self) -> StorageResult<(u64, u64)> {
        let count =
            u64::try_from(self.native.files.len()).map_err(|_| invalid("run file count"))?;
        let bytes = self.native.files.iter().try_fold(0u64, |sum, file| {
            sum.checked_add(file.bytes)
                .ok_or_else(|| invalid("run file byte sum"))
        })?;
        Ok((count, bytes))
    }

    pub fn check_binding(&self) -> StorageResult<()> {
        self.native.check_binding()
    }

    pub fn validate_context(&self, data_root: &Path, root: &NativeRoot) -> StorageResult<()> {
        if self.native.root != data_root {
            return Err(invalid("run restore publication root differs"));
        }
        self.native.profile.validate_root(root)?;
        self.check_binding()
    }
}

/// Complete physical closure precedes account and logical record validation.
pub fn verify(
    data_root: &Path,
    profile: &RunBackupProfile,
    native_root: &NativeRoot,
    account_key: &[u8; 32],
    scratch_root: &Path,
) -> StorageResult<RunRestoreGuard> {
    profile.validate_root(native_root)?;
    files::directory(scratch_root)?;
    let native = RunNodeGuard::open(data_root, account_key)?;
    if native.profile != *profile {
        return Err(invalid("run restore captured profile differs"));
    }
    let atoms = verify_readers_for_profile(
        &native.main,
        &native.cas,
        account_key,
        scratch_root,
        native.profile.version,
    )?;
    let guard = RunRestoreGuard { native, atoms };
    guard.check_binding()?;
    Ok(guard)
}

pub(crate) fn verify_readers_for_profile(
    main: &RunRead,
    cas: &RunRead,
    account_key: &[u8; 32],
    scratch_root: &Path,
    version: u16,
) -> StorageResult<u64> {
    files::directory(scratch_root)?;
    proof::verify(main, cas, account_key, scratch_root, version)
}

fn invalid(message: &str) -> StorageError {
    StorageError::InvalidOperation(message.into())
}
