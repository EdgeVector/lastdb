//! Manifest chain download and validation for the S0 restore path.

use super::*;
use std::collections::BTreeSet;

/// Bind a restore manifest to the exact database root selected by the caller.
///
/// An unscoped client keeps the legacy library contract. The product restore
/// command always supplies a source-home scope and therefore takes this strict
/// branch before any chunk is installed or any mutation-log segment is read.
pub(super) fn validate_manifest_source_scope(
    auth: &AuthClient,
    manifest: &BackupManifest,
) -> SyncResult<()> {
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

pub(in crate::sync::engine) async fn download_manifest_chain(
    auth: &AuthClient,
    s3: &S3Client,
    latest_manifest_sha256: &str,
) -> Result<Vec<BackupManifest>, S0RestoreFailure> {
    download_manifest_chain_with_progress(auth, s3, latest_manifest_sha256, None).await
}

pub(super) async fn download_manifest_chain_with_progress(
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
        let manifest = fetch_verified_manifest(auth, s3, &expected_sha, progress).await?;
        let previous = manifest.previous_manifest_sha256.clone();
        manifests.push(manifest);
        progress::update(progress, |p| p.manifests_verified = manifests.len());
        let Some(previous) = previous else {
            break;
        };
        expected_sha = previous;
    }
    validate_chain_links(&manifests)?;
    Ok(manifests)
}

/// Download one manifest and prove its raw digest and canonical digest both
/// equal `expected_sha`.
async fn fetch_verified_manifest(
    auth: &AuthClient,
    s3: &S3Client,
    expected_sha: &str,
    progress: Option<&RestoreProgress>,
) -> Result<BackupManifest, S0RestoreFailure> {
    use S0RestoreBoundary as Boundary;

    let presigned = at_s0_boundary(
        Boundary::ManifestDownload,
        progress::measure(
            progress,
            TransferOperation::Authorization,
            auth.presign_backup_manifest_download(expected_sha),
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
        downloaded
            .ok_or_else(|| SyncError::Storage(format!("backup manifest {expected_sha} missing"))),
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
    Ok(manifest)
}

/// Check every root-to-latest link of a downloaded chain (latest first).
fn validate_chain_links(manifests: &[BackupManifest]) -> Result<(), S0RestoreFailure> {
    use S0RestoreBoundary as Boundary;

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
    Ok(())
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

pub(super) fn manifest_sha256_hex_sync(manifest: &BackupManifest) -> SyncResult<String> {
    manifest_sha256_hex(manifest).map_err(|e| SyncError::Storage(e.to_string()))
}
