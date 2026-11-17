//! Candidate file rows and temporary replay pins. External bytes stay out of metadata.

use super::metadata::require_row;
use super::records::RecordFormat;
use super::{inline_files, CAS_COLLECTION};
use crate::error::FoldDbError;
use crate::schema::types::{FileWrite, SchemaError};
use crate::storage::current_keys::families::record_file_path;
use crate::storage::disk_pack::{run_blocking, transaction, PackRuntime};
use crate::storage::local_file_ref::{self, SidecarPlan};
use crate::storage::{KvMutation, StorageError};
use lastdb_model::hex::sha256_hex;
use lastdb_model::portable_mutation::Position;
use lastdb_model::record_value::FileMarker;
use lastdb_model::storage_policy::native_write::LOCAL_FILE_INPUT_MEMORY_BYTES;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub(crate) const FILE_REF_MAX: usize = 4096;

pub(crate) enum PinState {
    Absent,
    Ready,
    Damaged,
}

pub(crate) enum AdmittedBody {
    Row { marker: FileMarker, row: Vec<u8> },
    Local { marker: FileMarker, path: PathBuf },
    Clear,
}

pub(crate) fn admit_write(
    format: &RecordFormat,
    file: &FileWrite,
) -> Result<AdmittedBody, SchemaError> {
    let foreign = |msg: &str| SchemaError::InvalidData(msg.into());
    match file {
        FileWrite::Put(sealed) => {
            let marker = sealed.marker().clone();
            let row = format
                .issuer
                .admit(sealed.clone())
                .ok_or_else(|| foreign("foreign file payload"))?;
            require_row(format.content_key(), &marker, &row)?;
            Ok(AdmittedBody::Row { marker, row })
        }
        FileWrite::LocalPath(local) => {
            let marker = local.marker().clone();
            let path = format
                .issuer
                .admit_local(local)
                .ok_or_else(|| foreign("foreign local file ref"))?
                .to_path_buf();
            Ok(AdmittedBody::Local { marker, path })
        }
        FileWrite::Clear => Ok(AdmittedBody::Clear),
    }
}

pub(crate) async fn cas_state(
    format: &RecordFormat,
    path: &str,
    marker: &FileMarker,
) -> Result<PinState, SchemaError> {
    match cas_row(format, path).await? {
        Some(row) => {
            require_row(format.content_key(), marker, &row)?;
            Ok(PinState::Ready)
        }
        None => Ok(PinState::Absent),
    }
}

pub(crate) fn catalog_state(
    runtime: &PackRuntime,
    path: &str,
    marker: &FileMarker,
) -> Result<PinState, SchemaError> {
    match local_file_ref::referenced_path(runtime, path, marker).map_err(invalid) {
        Ok(Some((_, true))) => Ok(PinState::Ready),
        Ok(Some((_, false))) => Ok(PinState::Damaged),
        Ok(None) => Ok(PinState::Absent),
        Err(error) => Err(error),
    }
}

pub(crate) async fn put_cas_row(
    format: &RecordFormat,
    path: &str,
    row: Vec<u8>,
) -> Result<(), SchemaError> {
    let store = format
        .runtime
        .open_store(CAS_COLLECTION)
        .await
        .map_err(storage_err)?;
    store
        .batch_mutate(vec![KvMutation::put(path.as_bytes().to_vec(), row)])
        .await
        .map_err(storage_err)
}

pub(crate) async fn read_inline_rows(
    format: &RecordFormat,
    records: &[String],
) -> Result<Vec<Option<Vec<u8>>>, SchemaError> {
    if records.is_empty() {
        return Ok(Vec::new());
    }
    let keys = records
        .iter()
        .map(|key| inline_files::address(key).into_bytes())
        .collect();
    format.main.get_many(keys).await.map_err(storage_err)
}

pub(crate) async fn read_winning_bytes(
    format: &RecordFormat,
    address: &str,
    version: &Position,
    marker: &FileMarker,
    file_ref: Option<&str>,
) -> Result<Vec<u8>, SchemaError> {
    if let Some(bytes) = format
        .open_owned_record_file(address, version, marker)
        .await
        .map_err(fold_err)?
    {
        return Ok(bytes);
    }
    if let Some(path) = file_ref {
        return read_external(path, marker).await;
    }
    let key = record_file_path(address, version).map_err(invalid)?;
    let Some((path, intact)) =
        local_file_ref::referenced_path(&format.runtime, &key, marker).map_err(invalid)?
    else {
        return Err(invalid("winning file bytes are missing"));
    };
    if !intact {
        return Err(invalid("winning file differs from its marker"));
    }
    read_external(
        path.to_str()
            .ok_or_else(|| invalid("non-UTF-8 replay file path"))?,
        marker,
    )
    .await
}

pub(crate) async fn verify_external(path: &str, marker: &FileMarker) -> Result<(), SchemaError> {
    let _ = read_external(path, marker).await?;
    Ok(())
}

pub(crate) fn retire_files(
    runtime: &Arc<PackRuntime>,
    paths: Vec<String>,
) -> Result<(), SchemaError> {
    if paths.is_empty() {
        return Ok(());
    }
    let keys: Vec<Vec<u8>> = paths.iter().map(|path| path.as_bytes().to_vec()).collect();
    runtime
        .stage_record_file_deletes(&keys)
        .map_err(storage_err)?;
    let tracked = Arc::clone(runtime);
    transaction::after_commit(runtime.as_ref(), async move {
        let plan = SidecarPlan {
            additions: Vec::new(),
            removals: paths,
        };
        if let Err(error) = run_blocking(move || local_file_ref::apply(&tracked, plan)).await {
            tracing::error!(%error, "replay file pin removal failed after commit");
        }
        Ok(())
    })
    .map_err(storage_err)
}

async fn cas_row(format: &RecordFormat, path: &str) -> Result<Option<Vec<u8>>, SchemaError> {
    format
        .runtime
        .open_store(CAS_COLLECTION)
        .await
        .map_err(storage_err)?
        .get(path.as_bytes())
        .await
        .map_err(storage_err)
}

async fn read_external(path: &str, marker: &FileMarker) -> Result<Vec<u8>, SchemaError> {
    if path.len() > FILE_REF_MAX || path.is_empty() {
        return Err(invalid("replay file reference is empty or too long"));
    }
    let owned = path.to_owned();
    let expected = marker.clone();
    run_blocking(move || read_checked(Path::new(&owned), &expected))
        .await
        .map_err(invalid)
}

fn read_checked(path: &Path, marker: &FileMarker) -> Result<Vec<u8>, StorageError> {
    if marker.size_bytes > LOCAL_FILE_INPUT_MEMORY_BYTES {
        return Err(StorageError::write_limit(
            "local file exceeds in-memory cap",
        ));
    }
    let file = std::fs::File::open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.len() != marker.size_bytes {
        return Err(StorageError::invalid(
            "winning file differs from its marker",
        ));
    }
    let bound = marker.size_bytes + 1;
    let capacity = usize::try_from(bound)
        .map_err(|_| StorageError::write_limit("winning file exceeds addressable memory"))?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(capacity)
        .map_err(|_| StorageError::write_limit("winning file allocation failed"))?;
    file.take(bound).read_to_end(&mut bytes)?;
    let matches = bytes.len() as u64 == marker.size_bytes && sha256_hex(&bytes) == marker.sha256;
    matches
        .then_some(bytes)
        .ok_or_else(|| StorageError::invalid("winning file differs from its marker"))
}

fn fold_err(error: FoldDbError) -> SchemaError {
    match error {
        FoldDbError::Schema(error) => error,
        other => invalid(other),
    }
}

fn storage_err(error: impl std::fmt::Display) -> SchemaError {
    SchemaError::StorageRead {
        detail: error.to_string(),
    }
}

fn invalid(error: impl std::fmt::Display) -> SchemaError {
    SchemaError::InvalidData(error.to_string())
}
