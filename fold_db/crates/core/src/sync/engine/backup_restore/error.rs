//! Typed S0 restore failure boundary and the private restore-target seam.

use super::*;

/// The S0 operation that returned an error, not a diagnosis of its cause.
///
/// Unit variants deliberately carry no object identity or private error text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum S0RestoreBoundary {
    LatestPointer,
    ManifestDownload,
    ManifestDecode,
    ManifestValidation,
    LatestPointerValidation,
    SourceScopeValidation,
    DestinationValidation,
    ChunkDownload,
    ChunkInstall,
    DestinationIntegrity,
    DestinationCommit,
}

/// Detailed library failure. This type must not be serialized: `source` can
/// contain private paths, presigned URLs, object identities, or server text.
/// Product JSON callers map only the boundary and the source error variant.
#[derive(Debug)]
pub struct S0RestoreFailure {
    pub boundary: S0RestoreBoundary,
    pub source: SyncError,
}

impl std::fmt::Display for S0RestoreFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.source, formatter)
    }
}

impl std::error::Error for S0RestoreFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

pub(super) fn at_s0_boundary<T>(
    boundary: S0RestoreBoundary,
    result: SyncResult<T>,
) -> Result<T, S0RestoreFailure> {
    result.map_err(|source| S0RestoreFailure { boundary, source })
}

// Only the four synchronous restore target operations form this private fault
// seam. Cloud clients, storage formats, and general NamespacedStore access do
// not gain an abstraction or a configurable backend here.
pub(super) trait S0RestoreTarget {
    fn validate_restore_candidate(&self, manifest: &BackupManifest) -> SyncResult<()>;
    fn install_backup_chunk(&self, chunk: &BackupChunkRef, bytes: &[u8]) -> SyncResult<()>;
    fn verify_integrity(&self) -> SyncResult<()>;
    fn commit_restored_backup_manifest(&self, manifest: &BackupManifest) -> SyncResult<u64>;
}

impl S0RestoreTarget for LastStoreNamespacedStore {
    fn validate_restore_candidate(&self, manifest: &BackupManifest) -> SyncResult<()> {
        Self::validate_restore_candidate(self, manifest)
            .map_err(|error| SyncError::Storage(error.to_string()))
    }

    fn install_backup_chunk(&self, chunk: &BackupChunkRef, bytes: &[u8]) -> SyncResult<()> {
        Self::install_backup_chunk(self, chunk, bytes)
            .map_err(|error| SyncError::Storage(error.to_string()))
    }

    fn verify_integrity(&self) -> SyncResult<()> {
        Self::verify_integrity(self).map_err(|error| SyncError::Storage(error.to_string()))
    }

    fn commit_restored_backup_manifest(&self, manifest: &BackupManifest) -> SyncResult<u64> {
        Self::commit_restored_backup_manifest(self, manifest)
            .map_err(|error| SyncError::Storage(error.to_string()))
    }
}
