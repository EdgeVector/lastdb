//! Restore the exact normal pointer authenticated by a source-free descriptor.

use super::*;

pub async fn restore_laststore_cloud_backup_from_latest_pointer(
    auth: &AuthClient,
    s3: &S3Client,
    store: &LastStoreNamespacedStore,
    pointer: &BackupLatestGetResponse,
    progress: Option<&RestoreProgress>,
) -> Result<LastStoreCloudRestoreReport, S0RestoreFailure> {
    restore_laststore_cloud_backup_from_latest_pointer_with_cache(
        auth, s3, store, pointer, progress, None,
    )
    .await
}

/// Reuse only digest-verified file bytes from a prior stopped home.
pub async fn restore_laststore_cloud_backup_from_latest_pointer_with_cache(
    auth: &AuthClient,
    s3: &S3Client,
    store: &LastStoreNamespacedStore,
    pointer: &BackupLatestGetResponse,
    progress: Option<&RestoreProgress>,
    cache: Option<&RestoreChunkCache>,
) -> Result<LastStoreCloudRestoreReport, S0RestoreFailure> {
    restore_into_with_cache(
        auth,
        s3,
        store,
        progress,
        cache,
        Some((pointer.clone(), false)),
    )
    .await
}
