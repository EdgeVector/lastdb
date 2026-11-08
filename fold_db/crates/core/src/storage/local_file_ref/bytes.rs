//! Read a referenced file, and seal it into cas for one backup capture.

use super::super::disk_pack::{run_blocking, PackRuntime};
use super::super::error::{StorageError, StorageResult};
use super::super::format::records::{require_row, seal_file_row};
use super::{catalog_lock, checked_source, load, node_dir, sidecar_path, Entry};
use crate::error::FoldDbError;
use lastdb_model::hex::hex_lower;
use lastdb_model::record_value::FileMarker;
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

/// The external file for `cas_key`, checked against `marker`.
/// `Ok(None)` when this record version has no local reference.
pub(crate) async fn read_referenced(
    runtime: &PackRuntime,
    cas_key: &str,
    marker: &FileMarker,
) -> Result<Option<Vec<u8>>, FoldDbError> {
    let entry = {
        let _guard = catalog_lock()?;
        let path = sidecar_path(runtime)?;
        load(&path)?.files.get(cas_key).cloned()
    };
    let Some(entry) = entry else {
        return Ok(None);
    };
    if entry.sha256 != marker.sha256 || entry.size_bytes != marker.size_bytes {
        return Err(FoldDbError::SecurityError(
            "local file reference does not match its record marker".into(),
        ));
    }
    let path = PathBuf::from(entry.path);
    let size_bytes = entry.size_bytes;
    let sha256 = entry.sha256;
    let bytes = run_blocking(move || read_plaintext(&path, size_bytes, &sha256)).await?;
    Ok(Some(bytes))
}

fn read_plaintext(path: &std::path::Path, size_bytes: u64, sha256: &str) -> StorageResult<Vec<u8>> {
    if size_bytes > lastdb_storage_policy::native_write::LOCAL_FILE_INPUT_MEMORY_BYTES {
        return Err(StorageError::WriteResourceLimit {
            detail: "local file reference exceeds the in-memory file cap".into(),
        });
    }
    let metadata = std::fs::metadata(path).map_err(|error| {
        StorageError::InvalidOperation(format!(
            "local file {} is not available: {error}",
            path.display()
        ))
    })?;
    if !metadata.is_file() || metadata.len() != size_bytes {
        return Err(StorageError::InvalidOperation(format!(
            "local file {} does not match its record marker",
            path.display()
        )));
    }
    let mut file = File::open(path)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(usize::try_from(size_bytes).unwrap_or(usize::MAX))
        .map_err(|_| StorageError::WriteResourceLimit {
            detail: "local file reference allocation refused".into(),
        })?;
    file.read_to_end(&mut bytes)?;
    if bytes.len() as u64 != size_bytes || hex_lower(Sha256::digest(&bytes)) != sha256 {
        return Err(StorageError::InvalidOperation(format!(
            "local file {} does not match its record marker",
            path.display()
        )));
    }
    Ok(bytes)
}

/// Seal every referenced file into cas. The returned keys are the rows to
/// delete after the hold exists. A failure deletes only the rows this call put.
pub(crate) async fn materialize(
    runtime: &std::sync::Arc<PackRuntime>,
    account_key: &[u8; 32],
) -> StorageResult<Vec<Vec<u8>>> {
    let entries = {
        let _guard = catalog_lock()?;
        load(&sidecar_path(runtime)?)?.files
    };
    if entries.is_empty() {
        return Ok(Vec::new());
    }
    let account_key = *account_key;
    let mut release_keys = Vec::with_capacity(entries.len());
    let mut put_keys = Vec::new();
    for (cas_key, entry) in entries {
        let key = cas_key.into_bytes();
        release_keys.push(key.clone());
        match materialize_one(runtime, &account_key, &key, &entry).await {
            Ok(true) => put_keys.push(key),
            Ok(false) => {}
            Err(error) => {
                let _ = release(runtime, &put_keys).await;
                return Err(error);
            }
        }
    }
    Ok(release_keys)
}

async fn materialize_one(
    runtime: &std::sync::Arc<PackRuntime>,
    account_key: &[u8; 32],
    key: &[u8],
    entry: &Entry,
) -> StorageResult<bool> {
    let node = node_dir(runtime)?;
    let path = PathBuf::from(&entry.path);
    let size_bytes = entry.size_bytes;
    let canonical = run_blocking(move || checked_source(&node, &path, size_bytes)).await?;
    if let Some(row) = runtime.cas_get(key).await? {
        let marker = FileMarker {
            name: None,
            media_type: None,
            size_bytes: entry.size_bytes,
            sha256: entry.sha256.clone(),
        };
        if require_row(Some(account_key), &marker, &row).is_ok() {
            return Ok(false);
        }
    }
    if entry.size_bytes > lastdb_storage_policy::native_write::LOCAL_FILE_INPUT_MEMORY_BYTES {
        return Err(StorageError::WriteResourceLimit {
            detail: format!(
                "local file {} exceeds the in-memory seal cap",
                canonical.display()
            ),
        });
    }
    let sha256 = entry.sha256.clone();
    let size_bytes = entry.size_bytes;
    let plaintext = run_blocking(move || read_plaintext(&canonical, size_bytes, &sha256)).await?;
    let (row, sealed_sha) = seal_file_row(account_key, &plaintext).map_err(|error| {
        StorageError::InvalidOperation(format!("local file seal failed: {error}"))
    })?;
    if sealed_sha != entry.sha256 {
        return Err(StorageError::InvalidOperation(
            "sealed local file does not match its record marker".into(),
        ));
    }
    runtime.cas_put(key, row).await?;
    Ok(true)
}

pub(crate) async fn release(
    runtime: &std::sync::Arc<PackRuntime>,
    keys: &[Vec<u8>],
) -> StorageResult<()> {
    if keys.is_empty() {
        return Ok(());
    }
    runtime.cas_delete(keys.to_vec()).await
}

/// Delete cas rows for the current catalog. Absent rows publish nothing.
pub(crate) async fn release_catalog(runtime: &std::sync::Arc<PackRuntime>) -> StorageResult<()> {
    let keys = {
        let _guard = catalog_lock()?;
        load(&sidecar_path(runtime)?)?
            .files
            .into_keys()
            .map(String::into_bytes)
            .collect::<Vec<_>>()
    };
    release(runtime, &keys).await
}
