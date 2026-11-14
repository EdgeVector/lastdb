//! Per-home local file reference sidecar catalog and verified external file reads.

use super::disk_pack::{run_blocking, PackRuntime};
use super::error::{StorageError, StorageResult};
use crate::error::FoldDbError;
use lastdb_model::hex::hex_lower;
use lastdb_model::record_value::FileMarker;
use lastdb_model::storage_policy::native_write::LOCAL_FILE_INPUT_MEMORY_BYTES;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{ErrorKind, Read};
use std::path::{Path, PathBuf};
use std::sync::MutexGuard;

pub(crate) type DescribedFile = (PathBuf, Option<String>, u64, String);

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    path: String,
    sha256: String,
    size_bytes: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Catalog {
    version: u32,
    files: BTreeMap<String, Entry>,
}

pub(crate) struct SidecarAddition {
    pub(crate) cas_key: String,
    pub(crate) path: PathBuf,
    pub(crate) sha256: String,
    pub(crate) size_bytes: u64,
}

#[derive(Default)]
pub(crate) struct SidecarPlan {
    pub(crate) additions: Vec<SidecarAddition>,
    pub(crate) removals: Vec<String>,
}

impl SidecarPlan {
    pub(crate) fn is_empty(&self) -> bool {
        self.additions.is_empty() && self.removals.is_empty()
    }
}

fn invalid(message: impl std::fmt::Display) -> StorageError {
    StorageError::InvalidOperation(message.to_string())
}

fn catalog_lock(runtime: &PackRuntime) -> StorageResult<MutexGuard<'_, ()>> {
    runtime
        .local_file_refs_lock
        .lock()
        .map_err(|_| StorageError::backend("local file reference catalog lock"))
}

fn sidecar_path(runtime: &PackRuntime) -> StorageResult<(PathBuf, PathBuf)> {
    let node = runtime
        .physical
        .path()
        .parent()
        .ok_or_else(|| invalid("no node dir"))?;
    Ok((node.join("local-file-refs.json"), node.to_path_buf()))
}

fn load(path: &Path) -> StorageResult<Catalog> {
    match fs::read(path) {
        Ok(bytes) => {
            let catalog: Catalog = serde_json::from_slice(&bytes)
                .map_err(|e| invalid(format!("local file reference catalog unreadable: {e}")))?;
            (catalog.version == 1)
                .then_some(catalog)
                .ok_or_else(|| invalid("unsupported catalog version"))
        }
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(Catalog {
            version: 1,
            files: BTreeMap::new(),
        }),
        Err(e) => Err(e.into()),
    }
}

pub(super) fn has_references(runtime: &PackRuntime) -> StorageResult<bool> {
    let _guard = catalog_lock(runtime)?;
    Ok(!load(&sidecar_path(runtime)?.0)?.files.is_empty())
}

pub(crate) fn describe(runtime: &PackRuntime, path: &Path) -> StorageResult<DescribedFile> {
    let (_, node) = sidecar_path(runtime)?;
    let display = path.display();
    let canonical =
        fs::canonicalize(path).map_err(|e| invalid(format!("local file {display}: {e}")))?;
    if canonical.starts_with(fs::canonicalize(&node).unwrap_or(node)) {
        return Err(invalid(
            "a local file reference must stay outside the node home",
        ));
    }
    let metadata = fs::metadata(&canonical)?;
    let canonical_display = canonical.display();
    if !metadata.is_file() {
        return Err(invalid(format!(
            "local file {canonical_display} is not a regular file"
        )));
    }
    let mut file = File::open(&canonical)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 65536];
    let mut length = 0u64;
    while let count @ 1.. = file.read(&mut buffer)? {
        length = length
            .checked_add(count as u64)
            .ok_or_else(|| invalid("length overflow"))?;
        hasher.update(&buffer[..count]);
    }
    if length != metadata.len() {
        return Err(invalid(format!(
            "local file {canonical_display} changed during hash"
        )));
    }
    let file_name = canonical
        .file_name()
        .and_then(|name| name.to_str())
        .map(Into::into);
    Ok((canonical, file_name, length, hex_lower(hasher.finalize())))
}

pub(crate) fn apply(runtime: &PackRuntime, plan: SidecarPlan) -> StorageResult<()> {
    if plan.is_empty() {
        return Ok(());
    }
    let _guard = catalog_lock(runtime)?;
    let (path, _) = sidecar_path(runtime)?;
    let mut catalog = load(&path)?;
    for key in &plan.removals {
        catalog.files.remove(key);
    }
    for addition in plan.additions {
        let path_text = addition
            .path
            .to_str()
            .ok_or_else(|| invalid("non-UTF-8 path"))?
            .into();
        catalog.files.insert(
            addition.cas_key,
            Entry {
                path: path_text,
                sha256: addition.sha256,
                size_bytes: addition.size_bytes,
            },
        );
    }
    if catalog.files.is_empty() {
        return match fs::remove_file(&path) {
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
            res => Ok(res?),
        };
    }
    let tmp = path.with_extension("json.tmp");
    let mut file = File::create(&tmp)?;
    serde_json::to_writer(&mut file, &catalog)
        .map_err(|e| invalid(format!("local file reference catalog write failed: {e}")))?;
    file.sync_all()?;
    Ok(fs::rename(&tmp, &path)?)
}

pub(crate) fn remember(
    runtime: &PackRuntime,
    cas_key: &str,
    path: &Path,
    sha256: &str,
    size_bytes: u64,
) -> StorageResult<()> {
    apply(
        runtime,
        SidecarPlan {
            additions: vec![SidecarAddition {
                cas_key: cas_key.into(),
                path: path.to_path_buf(),
                sha256: sha256.into(),
                size_bytes,
            }],
            removals: Vec::new(),
        },
    )
}

pub(crate) async fn read_referenced(
    runtime: &PackRuntime,
    cas_key: &str,
    marker: &FileMarker,
) -> Result<Option<Vec<u8>>, FoldDbError> {
    let entry = {
        let _guard = catalog_lock(runtime)?;
        load(&sidecar_path(runtime)?.0)?.files.remove(cas_key)
    };
    let Some(entry) = entry else { return Ok(None) };
    if entry.sha256 != marker.sha256 || entry.size_bytes != marker.size_bytes {
        return Err(FoldDbError::SecurityError(
            "local file reference does not match its record marker".into(),
        ));
    }
    let bytes = run_blocking(move || read_verified_local_file(&entry)).await?;
    Ok(Some(bytes))
}

fn read_verified_local_file(entry: &Entry) -> StorageResult<Vec<u8>> {
    if entry.size_bytes > LOCAL_FILE_INPUT_MEMORY_BYTES {
        return Err(StorageError::write_limit(
            "local file exceeds in-memory cap",
        ));
    }
    let path = Path::new(&entry.path);
    let display = path.display();
    let mismatch = || invalid(format!("local file {display} marker mismatch"));
    let metadata = fs::metadata(path).map_err(|e| invalid(format!("local file {display}: {e}")))?;
    if !metadata.is_file() || metadata.len() != entry.size_bytes {
        return Err(mismatch());
    }
    let mut bytes = Vec::new();
    let capacity = usize::try_from(entry.size_bytes).unwrap_or(usize::MAX);
    bytes
        .try_reserve_exact(capacity)
        .map_err(|_| StorageError::write_limit("alloc refused"))?;
    File::open(path)?.read_to_end(&mut bytes)?;
    let matches =
        bytes.len() as u64 == entry.size_bytes && hex_lower(Sha256::digest(&bytes)) == entry.sha256;
    matches.then_some(bytes).ok_or_else(mismatch)
}
