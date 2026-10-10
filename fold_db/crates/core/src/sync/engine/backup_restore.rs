//! LastStore cloud backup restore path.

use super::super::auth::ops::{BackupLatestGetResponse, BackupLatestPointer, RescueS0Pointer};
use super::super::auth::AuthClient;
use super::super::error::{SyncError, SyncResult};
use super::super::s3::S3Client;
mod packs;
mod pointer;
use super::pin_log::MutationLogReplayReport;
use super::restore_progress::{self as progress, RestorePhase, RestoreProgress, TransferOperation};
use super::RestoreChunkCache;
use crate::hex::sha256_hex;
use crate::storage::laststore::{
    cloud_db_hash_for_store_uuid, manifest_sha256_hex, validate_manifest_chain, BackupChunkRef,
    BackupManifest, BackupManifestRole, LastStoreNamespacedStore,
};
use crate::sync::snapshot_log::Frontier;
use futures::{stream::FuturesOrdered, StreamExt};
pub use pointer::{
    restore_laststore_cloud_backup_from_latest_pointer,
    restore_laststore_cloud_backup_from_latest_pointer_with_cache,
};
use serde::Serialize;
use std::collections::BTreeSet;
use std::time::Instant;

#[path = "backup_restore_identity.rs"]
mod identity;

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

fn at_s0_boundary<T>(
    boundary: S0RestoreBoundary,
    result: SyncResult<T>,
) -> Result<T, S0RestoreFailure> {
    result.map_err(|source| S0RestoreFailure { boundary, source })
}

// Only the four synchronous restore target operations form this private fault
// seam. Cloud clients, storage formats, and general NamespacedStore access do
// not gain an abstraction or a configurable backend here.
trait S0RestoreTarget {
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

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct LastStoreCloudRestoreReport {
    pub manifest_sha256: String,
    pub latest_key: String,
    pub counter: u64,
    pub cut_csn: u64,
    pub manifests_walked: usize,
    pub chunks_installed: usize,
    pub bytes_installed: u64,
    pub chunks_reused: usize,
    pub bytes_reused: u64,
    pub restored_epoch: u64,
    /// True when the manifest store identity matched an explicit AuthClient
    /// database scope. Product restore requires this proof.
    pub source_scope_verified: bool,
    /// True only when the product restore command kept its cloud write
    /// interlock armed through the final local shutdown flush.
    pub remote_read_only: bool,
    /// Continuous mutation-log replay after S0 when the caller runs phase 2.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mutation_log_replay: Option<MutationLogReplayReport>,
    /// Writer positions pinned before the restored snapshot's first file cut.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mutation_log_snapshot_frontier: Option<Frontier>,
}

pub async fn restore_laststore_cloud_backup(
    auth: &AuthClient,
    s3: &S3Client,
    store: &LastStoreNamespacedStore,
) -> SyncResult<LastStoreCloudRestoreReport> {
    restore_laststore_cloud_backup_detailed(auth, s3, store)
        .await
        .map_err(|failure| failure.source)
}

/// Restore S0 with a typed operation boundary while preserving the source
/// error. The legacy entry point returns that source unchanged.
pub async fn restore_laststore_cloud_backup_detailed(
    auth: &AuthClient,
    s3: &S3Client,
    store: &LastStoreNamespacedStore,
) -> Result<LastStoreCloudRestoreReport, S0RestoreFailure> {
    restore_laststore_cloud_backup_with_progress(auth, s3, store, None).await
}

pub async fn restore_laststore_cloud_backup_with_progress(
    auth: &AuthClient,
    s3: &S3Client,
    store: &LastStoreNamespacedStore,
    progress: Option<&RestoreProgress>,
) -> Result<LastStoreCloudRestoreReport, S0RestoreFailure> {
    restore_laststore_cloud_backup_with_cache(auth, s3, store, progress, None).await
}

/// Restore from the current authenticated manifest, with optional verified local bytes.
pub async fn restore_laststore_cloud_backup_with_cache(
    auth: &AuthClient,
    s3: &S3Client,
    store: &LastStoreNamespacedStore,
    progress: Option<&RestoreProgress>,
    cache: Option<&RestoreChunkCache>,
) -> Result<LastStoreCloudRestoreReport, S0RestoreFailure> {
    restore_into_with_cache(auth, s3, store, progress, cache, None).await
}

/// Restore one immutable S0 rescue cut without a read of normal backup/latest.
pub async fn restore_laststore_cloud_backup_from_rescue_with_cache(
    auth: &AuthClient,
    s3: &S3Client,
    store: &LastStoreNamespacedStore,
    rescue: &RescueS0Pointer,
    progress: Option<&RestoreProgress>,
) -> Result<LastStoreCloudRestoreReport, S0RestoreFailure> {
    at_s0_boundary(
        S0RestoreBoundary::LatestPointerValidation,
        rescue.validate(),
    )?;
    let pointer = BackupLatestGetResponse {
        key: format!("rescue/s0/{}.json", rescue.manifest_sha256),
        latest: BackupLatestPointer {
            store_uuid: rescue.store_uuid.clone(),
            epoch: rescue.epoch,
            counter: rescue.counter,
            manifest_sha256: rescue.manifest_sha256.clone(),
            updated_at_unix_secs: 0,
            format_version: Some(1),
        },
        etag: String::new(),
    };
    restore_into_with_cache(auth, s3, store, progress, None, Some((pointer, true))).await
}

fn require_independent_rescue_root(manifests: &[BackupManifest]) -> SyncResult<()> {
    match manifests {
        [root] if root.previous_manifest_sha256.is_none() => Ok(()),
        _ => Err(SyncError::Storage(
            "S0 rescue manifest must be an independent root".to_string(),
        )),
    }
}

async fn restore_into_with_cache<T: S0RestoreTarget + ?Sized>(
    auth: &AuthClient,
    s3: &S3Client,
    store: &T,
    progress: Option<&RestoreProgress>,
    cache: Option<&RestoreChunkCache>,
    pointer_override: Option<(BackupLatestGetResponse, bool)>,
) -> Result<LastStoreCloudRestoreReport, S0RestoreFailure> {
    use S0RestoreBoundary as Boundary;
    progress::phase(progress, RestorePhase::LatestPointer);
    let rescue_root = pointer_override.as_ref().is_some_and(|(_, rescue)| *rescue);
    let latest = match pointer_override {
        Some((pointer, _)) => pointer,
        None => at_s0_boundary(
            Boundary::LatestPointer,
            progress::measure(
                progress,
                TransferOperation::Authorization,
                auth.backup_latest_get(),
            )
            .await,
        )?,
    };
    // Reject unknown formats before the first manifest request.
    at_s0_boundary(
        Boundary::LatestPointerValidation,
        latest.latest.require_supported_format(),
    )?;
    progress::phase(progress, RestorePhase::ManifestChain);
    let manifests =
        download_manifest_chain_with_progress(auth, s3, &latest.latest.manifest_sha256, progress)
            .await?;
    if rescue_root {
        at_s0_boundary(
            Boundary::ManifestValidation,
            require_independent_rescue_root(&manifests),
        )?;
    }
    let manifest = at_s0_boundary(
        Boundary::LatestPointerValidation,
        manifests.first().ok_or_else(|| {
            SyncError::Storage("backup restore found no valid manifests".to_string())
        }),
    )?;
    // Defense-in-depth: `download_manifest_chain` already verified the tip
    // digest during its walk; re-check pointer fields against the body so a
    // mismatched latest row cannot install the wrong cut.
    if latest.latest.format_version() != manifest.version
        || latest.latest.store_uuid != manifest.store_uuid
        || latest.latest.epoch != manifest.epoch
        || latest.latest.counter != manifest.counter
        || latest.latest.manifest_sha256
            != at_s0_boundary(
                Boundary::LatestPointerValidation,
                manifest_sha256_hex_sync(manifest),
            )?
    {
        return at_s0_boundary(
            Boundary::LatestPointerValidation,
            Err(SyncError::Storage(
                "backup latest pointer does not match manifest body".to_string(),
            )),
        );
    }
    at_s0_boundary(
        Boundary::SourceScopeValidation,
        validate_manifest_source_scope(auth, manifest),
    )?;
    let install_origins = at_s0_boundary(
        Boundary::ManifestValidation,
        identity::rewritten_segment_origins(&manifests),
    )?;
    at_s0_boundary(
        Boundary::DestinationValidation,
        store.validate_restore_candidate(manifest),
    )?;

    let mut chunks = manifest
        .atom_chunks
        .iter()
        .chain(manifest.mutable_chunks.iter())
        .collect::<Vec<_>>();
    chunks.sort_by_key(|chunk| backup_restore_chunk_order(chunk));

    let mut chunks_installed = 0usize;
    let mut bytes_installed = 0u64;
    let mut chunks_reused = 0usize;
    let mut bytes_reused = 0u64;
    progress::update(progress, |p| {
        p.chunks_total = Some(chunks.len());
        p.bytes_declared = Some(
            chunks
                .iter()
                .fold(0u64, |sum, c| sum.saturating_add(c.bytes)),
        );
    });
    progress::phase(progress, RestorePhase::ChunkTransfer);
    let pack_cache = packs::VerifiedPackCache::default();
    let mut downloads = FuturesOrdered::new();
    let mut next = 0;
    let mut reserved = 0u64;
    while next < chunks.len() || !downloads.is_empty() {
        while next < chunks.len() && download_fits(downloads.len(), reserved, chunks[next]) {
            let chunk = chunks[next];
            reserved = reserved.saturating_add(download_reservation(chunk));
            let mut install = BackupChunkRef::clone(chunk);
            if let Some(origin) = install_origins.get(&identity::address(chunk)) {
                install.chunk_uuid.clone_from(origin);
            }
            let pack_cache_ref = &pack_cache;
            downloads.push_back(async move {
                if let Some(bytes) = cache.and_then(|cache| {
                    progress::measure_cache_read(progress, || {
                        cache.read(chunk, &install.chunk_uuid)
                    })
                }) {
                    return (chunk, install, true, Ok(bytes));
                }
                (
                    chunk,
                    install,
                    false,
                    download_backup_chunk_with_progress(
                        auth,
                        s3,
                        chunk,
                        progress,
                        Some(pack_cache_ref),
                    )
                    .await,
                )
            });
            next += 1;
        }
        progress::update(progress, |p| {
            p.queued_downloads = downloads.len();
            p.reserved_bytes = reserved;
        });
        // Futures stay owned by the queue. Error/cancellation drops all requests.
        // Release reservation only after ordered install, including buffered bodies.
        let Some((chunk, install, reused, result)) = downloads.next().await else {
            break;
        };
        let bytes = at_s0_boundary(Boundary::ChunkDownload, result)?;
        let started = Instant::now();
        let result = store.install_backup_chunk(&install, &bytes);
        progress::update(progress, |p| {
            p.install_ms = p.install_ms.saturating_add(progress::millis(started));
        });
        at_s0_boundary(Boundary::ChunkInstall, result)?;
        chunks_installed += 1;
        if reused {
            chunks_reused += 1;
            bytes_reused = bytes_reused.saturating_add(bytes.len() as u64);
        }
        bytes_installed = bytes_installed.saturating_add(bytes.len() as u64);
        reserved = reserved.saturating_sub(download_reservation(chunk));
        pack_cache.trim().await;
        progress::update(progress, |p| {
            p.chunks_installed = chunks_installed;
            p.bytes_installed = bytes_installed;
            p.chunks_reused = chunks_reused;
            p.bytes_reused = bytes_reused;
            p.queued_downloads = downloads.len();
            p.reserved_bytes = reserved;
        });
    }

    progress::phase(progress, RestorePhase::Integrity);
    at_s0_boundary(Boundary::DestinationIntegrity, store.verify_integrity())?;
    progress::phase(progress, RestorePhase::Commit);
    let restored_epoch = at_s0_boundary(
        Boundary::DestinationCommit,
        store.commit_restored_backup_manifest(manifest),
    )?;

    Ok(LastStoreCloudRestoreReport {
        manifest_sha256: latest.latest.manifest_sha256,
        latest_key: latest.key,
        counter: manifest.counter,
        cut_csn: manifest.cut_csn,
        manifests_walked: manifests.len(),
        chunks_installed,
        bytes_installed,
        chunks_reused,
        bytes_reused,
        restored_epoch,
        source_scope_verified: auth.db_hash_scope().is_some(),
        remote_read_only: false,
        mutation_log_replay: None,
        mutation_log_snapshot_frontier: None,
    })
}

/// Published S0 cut only: latest pointer + tip manifest, no chunk install.
///
/// Used when dest already has a committed LastStore backup (restore --into
/// failed during mutation-log apply, or an operator resumes). `cut_csn` is the
/// frontier for [`super::SyncEngine::restore_mutation_log_after_s0`].
pub async fn laststore_published_backup_cut(
    auth: &AuthClient,
    s3: &S3Client,
) -> SyncResult<LastStoreCloudRestoreReport> {
    laststore_published_backup_cut_detailed(auth, s3)
        .await
        .map_err(|failure| failure.source)
}

/// Read the published S0 cut with typed failure boundaries, without a target
/// store, chunk install, or change to the legacy resume error contract.
pub async fn laststore_published_backup_cut_detailed(
    auth: &AuthClient,
    s3: &S3Client,
) -> Result<LastStoreCloudRestoreReport, S0RestoreFailure> {
    use S0RestoreBoundary as Boundary;

    let latest = at_s0_boundary(Boundary::LatestPointer, auth.backup_latest_get().await)?;
    at_s0_boundary(
        Boundary::LatestPointerValidation,
        latest.latest.require_supported_format(),
    )?;
    let manifests = download_manifest_chain(auth, s3, &latest.latest.manifest_sha256).await?;
    let manifest = at_s0_boundary(
        Boundary::LatestPointerValidation,
        manifests.first().ok_or_else(|| {
            SyncError::Storage("backup restore found no valid manifests".to_string())
        }),
    )?;
    if latest.latest.format_version() != manifest.version
        || latest.latest.store_uuid != manifest.store_uuid
        || latest.latest.epoch != manifest.epoch
        || latest.latest.counter != manifest.counter
        || latest.latest.manifest_sha256
            != at_s0_boundary(
                Boundary::LatestPointerValidation,
                manifest_sha256_hex_sync(manifest),
            )?
    {
        return at_s0_boundary(
            Boundary::LatestPointerValidation,
            Err(SyncError::Storage(
                "backup latest pointer does not match manifest body".to_string(),
            )),
        );
    }
    at_s0_boundary(
        Boundary::SourceScopeValidation,
        validate_manifest_source_scope(auth, manifest),
    )?;
    Ok(LastStoreCloudRestoreReport {
        manifest_sha256: latest.latest.manifest_sha256,
        latest_key: latest.key,
        counter: manifest.counter,
        cut_csn: manifest.cut_csn,
        manifests_walked: manifests.len(),
        chunks_installed: 0,
        bytes_installed: 0,
        chunks_reused: 0,
        bytes_reused: 0,
        restored_epoch: manifest.epoch,
        source_scope_verified: auth.db_hash_scope().is_some(),
        remote_read_only: false,
        mutation_log_replay: None,
        mutation_log_snapshot_frontier: None,
    })
}

/// Bind a restore manifest to the exact database root selected by the caller.
///
/// An unscoped client keeps the legacy library contract. The product restore
/// command always supplies a source-home scope and therefore takes this strict
/// branch before any chunk is installed or any mutation-log segment is read.
fn validate_manifest_source_scope(auth: &AuthClient, manifest: &BackupManifest) -> SyncResult<()> {
    let Some(expected_db_hash) = auth.db_hash_scope() else {
        return Ok(());
    };
    let manifest_db_hash = cloud_db_hash_for_store_uuid(&manifest.store_uuid);
    if manifest_db_hash != expected_db_hash {
        return Err(SyncError::Storage(
            "backup manifest store identity does not match the source restore scope".to_string(),
        ));
    }
    Ok(())
}

pub(super) async fn download_manifest_chain(
    auth: &AuthClient,
    s3: &S3Client,
    latest_manifest_sha256: &str,
) -> Result<Vec<BackupManifest>, S0RestoreFailure> {
    download_manifest_chain_with_progress(auth, s3, latest_manifest_sha256, None).await
}

async fn download_manifest_chain_with_progress(
    auth: &AuthClient,
    s3: &S3Client,
    latest_manifest_sha256: &str,
    progress: Option<&RestoreProgress>,
) -> Result<Vec<BackupManifest>, S0RestoreFailure> {
    use S0RestoreBoundary as Boundary;

    let mut expected_sha = latest_manifest_sha256.to_string();
    let mut visited_manifest_shas = BTreeSet::new();
    let mut manifests = Vec::new();
    loop {
        at_s0_boundary(
            Boundary::ManifestValidation,
            record_manifest_visit(&mut visited_manifest_shas, &expected_sha),
        )?;
        let presigned = at_s0_boundary(
            Boundary::ManifestDownload,
            progress::measure(
                progress,
                TransferOperation::Authorization,
                auth.presign_backup_manifest_download(&expected_sha),
            )
            .await,
        )?;
        let downloaded = at_s0_boundary(
            Boundary::ManifestDownload,
            progress::measure(
                progress,
                TransferOperation::Download,
                s3.download_limited(&presigned, Some(16 * 1024 * 1024)),
            )
            .await,
        )?;
        let bytes = at_s0_boundary(
            Boundary::ManifestDownload,
            downloaded.ok_or_else(|| {
                SyncError::Storage(format!("backup manifest {expected_sha} missing"))
            }),
        )?;
        progress::update(progress, |p| {
            p.response_body_bytes = p.response_body_bytes.saturating_add(bytes.len() as u64);
        });
        let actual = sha256_hex(&bytes);
        if actual != expected_sha {
            return at_s0_boundary(
                Boundary::ManifestValidation,
                Err(SyncError::Crypto(format!(
                    "backup manifest sha256 mismatch: expected {expected_sha}, got {actual}"
                ))),
            );
        }
        let manifest: BackupManifest = at_s0_boundary(
            Boundary::ManifestDecode,
            serde_json::from_slice(&bytes)
                .map_err(|e| SyncError::Storage(format!("decode backup manifest: {e}"))),
        )?;
        let canonical = at_s0_boundary(
            Boundary::ManifestValidation,
            manifest_sha256_hex_sync(&manifest),
        )?;
        if canonical != expected_sha {
            return at_s0_boundary(
                Boundary::ManifestValidation,
                Err(SyncError::Crypto(format!(
                    "backup manifest canonical hash mismatch: expected {expected_sha}, got {canonical}"
                ))),
            );
        }
        let previous = manifest.previous_manifest_sha256.clone();
        manifests.push(manifest);
        progress::update(progress, |p| p.manifests_verified = manifests.len());
        let Some(previous) = previous else {
            break;
        };
        expected_sha = previous;
    }

    let root_to_latest = manifests.iter().rev().collect::<Vec<_>>();
    if let Some(root) = root_to_latest.first() {
        at_s0_boundary(
            Boundary::ManifestValidation,
            validate_manifest_chain(None, root).map_err(|e| SyncError::Storage(e.to_string())),
        )?;
    }
    for pair in root_to_latest.windows(2) {
        at_s0_boundary(
            Boundary::ManifestValidation,
            validate_manifest_chain(Some(pair[0]), pair[1])
                .map_err(|e| SyncError::Storage(e.to_string())),
        )?;
    }
    Ok(manifests)
}

fn record_manifest_visit(
    visited_manifest_shas: &mut BTreeSet<String>,
    expected_sha: &str,
) -> SyncResult<()> {
    if !visited_manifest_shas.insert(expected_sha.to_string()) {
        return Err(SyncError::Storage(format!(
            "backup manifest chain cycle detected at {expected_sha}"
        )));
    }
    Ok(())
}

/// Hard fence on a restore-time size-cap retry. Matches the manifest download
/// budget so a poisoned Content-Length cannot pin RSS.
const BACKUP_CHUNK_DOWNLOAD_HARD_CAP: usize = 16 * 1024 * 1024;

const RESTORE_DOWNLOAD_CONCURRENCY: usize = 8;
const RESTORE_DOWNLOAD_BUFFER_BYTES: u64 = 128 * 1024 * 1024;

fn download_reservation(chunk: &BackupChunkRef) -> u64 {
    let file_and_pack = chunk
        .pack
        .as_ref()
        .map_or(chunk.bytes, |pack| chunk.bytes.saturating_add(pack.bytes));
    file_and_pack.max(BACKUP_CHUNK_DOWNLOAD_HARD_CAP as u64)
}

fn download_fits(count: usize, reserved: u64, chunk: &BackupChunkRef) -> bool {
    count == 0
        || (count < RESTORE_DOWNLOAD_CONCURRENCY
            && download_reservation(chunk)
                <= RESTORE_DOWNLOAD_BUFFER_BYTES.saturating_sub(reserved))
}

fn backup_chunk_id(chunk: &BackupChunkRef) -> String {
    format!("chunk_uuid={} sha256={}", chunk.chunk_uuid, chunk.sha256)
}

/// Parse the Content-Length named by a `download_limited` preflight refusal.
///
/// Only the header-preflight wording is retryable. The streamed "no
/// Content-Length" path has no observed size, so restore must not guess a cap.
fn oversize_content_length(err: &SyncError) -> Option<usize> {
    let SyncError::S3(msg) = err else {
        return None;
    };
    let rest = msg.strip_prefix("object content-length ")?;
    let n_str = rest.split_whitespace().next()?;
    n_str.parse().ok()
}

fn annotate_backup_chunk_error(err: SyncError, chunk: &BackupChunkRef) -> SyncError {
    let id = backup_chunk_id(chunk);
    match err {
        SyncError::S3(msg) if !msg.contains("chunk_uuid=") => {
            SyncError::S3(format!("{msg} ({id})"))
        }
        SyncError::Storage(msg) if !msg.contains("chunk_uuid=") => {
            SyncError::Storage(format!("{msg} ({id})"))
        }
        SyncError::Crypto(msg) if !msg.contains("chunk_uuid=") => {
            SyncError::Crypto(format!("{msg} ({id})"))
        }
        other => other,
    }
}

pub(super) async fn download_backup_chunk(
    auth: &AuthClient,
    s3: &S3Client,
    chunk: &BackupChunkRef,
) -> SyncResult<Vec<u8>> {
    download_backup_chunk_with_progress(auth, s3, chunk, None, None).await
}

async fn download_backup_chunk_with_progress(
    auth: &AuthClient,
    s3: &S3Client,
    chunk: &BackupChunkRef,
    progress: Option<&RestoreProgress>,
    pack_cache: Option<&packs::VerifiedPackCache>,
) -> SyncResult<Vec<u8>> {
    if chunk.pack.is_some() {
        return packs::download_packed_file(auth, s3, chunk, progress, pack_cache).await;
    }
    let presigned = progress::measure(
        progress,
        TransferOperation::Authorization,
        auth.presign_backup_chunk_download(&chunk.sha256),
    )
    .await?;
    let declared = usize::try_from(chunk.bytes).map_err(|_| {
        SyncError::Storage(format!(
            "backup chunk too large: {} ({})",
            chunk.bytes,
            backup_chunk_id(chunk)
        ))
    })?;
    let first = progress::measure(
        progress,
        TransferOperation::Download,
        s3.download_limited(&presigned, Some(declared)),
    )
    .await;
    let mut bytes = match first {
        Ok(Some(bytes)) => bytes,
        Ok(None) => {
            return Err(SyncError::Storage(format!(
                "backup chunk {} missing",
                backup_chunk_id(chunk)
            )));
        }
        Err(err) => {
            // Restore-only retry: a content-addressed dedup hit can leave a
            // stored object whose Content-Length disagrees with this
            // manifest's `chunk.bytes` (observed 746 vs declared 701 on a
            // `sync_pin_log` chunk). ENC:/ENZ:/ENB: reseal changes the file
            // hash, so those format flips mint a new key and do not cause
            // this. Existence-only upload HEAD cannot repair the stored
            // object; sha256 of the downloaded bytes is the authority.
            let Some(observed) = oversize_content_length(&err) else {
                return Err(annotate_backup_chunk_error(err, chunk));
            };
            if observed > BACKUP_CHUNK_DOWNLOAD_HARD_CAP {
                return Err(SyncError::S3(format!(
                    "object content-length {observed} exceeds max_download_entry_bytes {declared} ({})",
                    backup_chunk_id(chunk)
                )));
            }
            progress::measure(
                progress,
                TransferOperation::Download,
                s3.download_limited(&presigned, Some(observed)),
            )
            .await
            .map_err(|e| annotate_backup_chunk_error(e, chunk))?
            .ok_or_else(|| {
                SyncError::Storage(format!("backup chunk {} missing", backup_chunk_id(chunk)))
            })?
        }
    };
    progress::update(progress, |p| {
        p.response_body_bytes = p.response_body_bytes.saturating_add(bytes.len() as u64);
    });
    let actual = sha256_hex(&bytes);
    if actual != chunk.sha256 {
        // Old uploaders read a live plain segment after the manifest cut.
        // The stored object can contain the exact cut plus later appends.
        // Recover only the declared prefix whose digest proves every byte.
        // No length-only fallback can authorize data from a wrong object.
        if bytes.len() > declared && sha256_hex(&bytes[..declared]) == chunk.sha256 {
            tracing::warn!(
                target: "fold_db::sync::restore",
                chunk_uuid = %chunk.chunk_uuid,
                sha256 = %chunk.sha256,
                manifest_bytes = chunk.bytes,
                downloaded_bytes = bytes.len(),
                "recovered exact manifest bytes from a legacy backup object with later appends"
            );
            bytes.truncate(declared);
            return Ok(bytes);
        }
        return Err(SyncError::Crypto(format!(
            "backup chunk sha256 mismatch: expected {}, got {actual} {} downloaded_bytes={}",
            chunk.sha256,
            backup_chunk_id(chunk),
            bytes.len()
        )));
    }
    if bytes.len() as u64 != chunk.bytes {
        tracing::warn!(
            target: "fold_db::sync::restore",
            chunk_uuid = %chunk.chunk_uuid,
            sha256 = %chunk.sha256,
            manifest_bytes = chunk.bytes,
            downloaded_bytes = bytes.len(),
            "backup chunk declared size disagreed with stored object; sha256 matched, accepting stored bytes"
        );
    }
    Ok(bytes)
}

fn manifest_sha256_hex_sync(manifest: &BackupManifest) -> SyncResult<String> {
    manifest_sha256_hex(manifest).map_err(|e| SyncError::Storage(e.to_string()))
}

pub(super) fn backup_restore_chunk_order(
    chunk: &BackupChunkRef,
) -> (u8, u64, String, u16, Option<u32>, String) {
    let role = match chunk.role {
        BackupManifestRole::Atom => 0,
        BackupManifestRole::Mutable if chunk.collection == "log" => 1,
        BackupManifestRole::Mutable => 2,
    };
    (
        role,
        chunk.end_csn,
        chunk.collection.clone(),
        chunk.shard,
        chunk.group_id,
        chunk.chunk_uuid.clone(),
    )
}
