//! Format one supports only file ciphertext inside the native snapshot.
use super::{RestoreError, Result, VerifiedNativeContext};
use cloud_backup_protocol::{Digest, ExternalFileDependency};
use fold_db::storage::disk_pack::file_recovery::{self, RecoveryDirectory};

pub(super) fn prepare(context: &VerifiedNativeContext<'_>, account: &Digest) -> Result<()> {
    file_recovery::prepare_directory(
        &context.data_root(),
        &RecoveryDirectory {
            version: 1,
            home_id: context.native_root.home_id.clone(),
            source_current_sha256: context.native_root.current_sha256.clone(),
            commit_sha256: context.commit_sha256.clone(),
            account_public_key: account.clone(),
        },
    )
    .map_err(|_| RestoreError::Invalid("file recovery directory"))
}

pub(super) fn require_native_coverage(dependencies: &[ExternalFileDependency]) -> Result<()> {
    if !dependencies.is_empty() {
        return Err(RestoreError::Unsupported(
            "external file source in client format one",
        ));
    }
    Ok(())
}
