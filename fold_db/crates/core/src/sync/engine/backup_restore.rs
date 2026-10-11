//! LastStore cloud backup restore path.

use super::super::auth::ops::{BackupLatestGetResponse, BackupLatestPointer, RescueS0Pointer};
use super::super::auth::AuthClient;
use super::super::error::{SyncError, SyncResult};
use super::super::s3::S3Client;
mod chunk_download;
mod error;
mod manifest_chain;
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
pub(super) use chunk_download::{backup_restore_chunk_order, download_backup_chunk};
use chunk_download::{
    download_backup_chunk_with_progress, download_fits, download_reservation,
    BACKUP_CHUNK_DOWNLOAD_HARD_CAP,
};
use error::{at_s0_boundary, S0RestoreTarget};
pub use error::{S0RestoreBoundary, S0RestoreFailure};
use futures::{stream::FuturesOrdered, StreamExt};
pub(super) use manifest_chain::download_manifest_chain;
use manifest_chain::{
    download_manifest_chain_with_progress, manifest_sha256_hex_sync, validate_manifest_source_scope,
};
pub use pointer::restore_laststore_cloud_backup_from_latest_pointer_with_cache;
use serde::Serialize;
use std::collections::BTreeMap;
use std::time::Instant;

#[path = "backup_restore_identity.rs"]
mod identity;

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
    let latest = resolve_latest_pointer(auth, progress, pointer_override).await?;
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
    require_pointer_matches_manifest(&latest, manifest)?;
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

    let totals =
        install_manifest_chunks(auth, s3, store, manifest, &install_origins, progress, cache)
            .await?;

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
        chunks_installed: totals.chunks_installed,
        bytes_installed: totals.bytes_installed,
        chunks_reused: totals.chunks_reused,
        bytes_reused: totals.bytes_reused,
        restored_epoch,
        source_scope_verified: auth.db_hash_scope().is_some(),
        remote_read_only: false,
        mutation_log_replay: None,
        mutation_log_snapshot_frontier: None,
    })
}

/// Use the caller's pointer override or read the authenticated latest pointer,
/// then reject unknown formats before the first manifest request.
async fn resolve_latest_pointer(
    auth: &AuthClient,
    progress: Option<&RestoreProgress>,
    pointer_override: Option<(BackupLatestGetResponse, bool)>,
) -> Result<BackupLatestGetResponse, S0RestoreFailure> {
    use S0RestoreBoundary as Boundary;
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
    at_s0_boundary(
        Boundary::LatestPointerValidation,
        latest.latest.require_supported_format(),
    )?;
    Ok(latest)
}

/// Defense-in-depth: `download_manifest_chain` already verified the tip
/// digest during its walk; re-check pointer fields against the body so a
/// mismatched latest row cannot install the wrong cut.
fn require_pointer_matches_manifest(
    latest: &BackupLatestGetResponse,
    manifest: &BackupManifest,
) -> Result<(), S0RestoreFailure> {
    use S0RestoreBoundary as Boundary;
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
    Ok(())
}

/// What the ordered chunk install loop wrote into the destination.
struct ChunkInstallTotals {
    chunks_installed: usize,
    bytes_installed: u64,
    chunks_reused: usize,
    bytes_reused: u64,
}

/// Download chunks with a bounded in-flight window and install them in
/// manifest order.
async fn install_manifest_chunks<T: S0RestoreTarget + ?Sized>(
    auth: &AuthClient,
    s3: &S3Client,
    store: &T,
    manifest: &BackupManifest,
    install_origins: &BTreeMap<identity::ChunkAddress, String>,
    progress: Option<&RestoreProgress>,
    cache: Option<&RestoreChunkCache>,
) -> Result<ChunkInstallTotals, S0RestoreFailure> {
    use S0RestoreBoundary as Boundary;
    let mut chunks = manifest
        .atom_chunks
        .iter()
        .chain(manifest.mutable_chunks.iter())
        .collect::<Vec<_>>();
    chunks.sort_by_key(|chunk| backup_restore_chunk_order(chunk));

    let mut totals = ChunkInstallTotals {
        chunks_installed: 0,
        bytes_installed: 0,
        chunks_reused: 0,
        bytes_reused: 0,
    };
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
        totals.chunks_installed += 1;
        if reused {
            totals.chunks_reused += 1;
            totals.bytes_reused = totals.bytes_reused.saturating_add(bytes.len() as u64);
        }
        totals.bytes_installed = totals.bytes_installed.saturating_add(bytes.len() as u64);
        reserved = reserved.saturating_sub(download_reservation(chunk));
        pack_cache.trim().await;
        progress::update(progress, |p| {
            p.chunks_installed = totals.chunks_installed;
            p.bytes_installed = totals.bytes_installed;
            p.chunks_reused = totals.chunks_reused;
            p.bytes_reused = totals.bytes_reused;
            p.queued_downloads = downloads.len();
            p.reserved_bytes = reserved;
        });
    }
    Ok(totals)
}
