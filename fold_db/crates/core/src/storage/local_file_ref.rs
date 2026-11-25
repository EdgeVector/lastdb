//! Per-home local file reference sidecar catalog and verified external file reads.

use super::disk_pack::{admit, PackRuntime};
use super::error::{StorageError, StorageResult};
use lastdb_model::hex::{hex_lower, sha256_hex};
use lastdb_model::record_value::FileMarker;
use lastdb_model::storage_policy::native_write::LOCAL_FILE_INPUT_MEMORY_BYTES;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{ErrorKind, Read};
use std::path::{Path, PathBuf};
use std::sync::MutexGuard;

/// Largest plaintext file a record may name. `describe` refuses a larger one at the write.
///
/// A sealed file row adds `1 + NONCE_SIZE + TAG_SIZE` (29 bytes) and must fit
/// inside a single `laststore::disk_run::MAX_RUN_VALUE_BYTES` (32 MiB) run value.
pub const MAX_RECORD_FILE_BYTES: u64 = (laststore::disk_run::MAX_RUN_VALUE_BYTES
    - (1 + crate::crypto::envelope::NONCE_SIZE + crate::crypto::envelope::TAG_SIZE))
    as u64;

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

pub(super) fn catalog_lock(runtime: &PackRuntime) -> StorageResult<MutexGuard<'_, ()>> {
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

/// The space a snapshot needs to seal every local file reference into a private
/// copy of the files store, or `None` when the home keeps no local reference.
pub(super) fn snapshot_budget(node: &Path) -> StorageResult<Option<u64>> {
    let catalog = load(&node.join("local-file-refs.json"))?;
    if catalog.files.is_empty() {
        return Ok(None);
    }
    let mut bytes = 1u64 << 20;
    for entry in catalog.files.values() {
        bytes = entry
            .size_bytes
            .checked_add(2048)
            .and_then(|size| size.checked_mul(2))
            .and_then(|size| bytes.checked_add(size))
            .ok_or_else(|| invalid("snapshot file size overflow"))?;
    }
    Ok(Some(bytes))
}

/// Visit one verified, sealed reference at a time for a private snapshot CAS.
pub(super) fn snapshot_rows(
    node: &Path,
    content_key: &[u8; 32],
    mut accept: impl FnMut(String, Vec<u8>) -> StorageResult<()>,
) -> StorageResult<()> {
    for (path, entry) in load(&node.join("local-file-refs.json"))?.files {
        if !path.starts_with(crate::storage::format::RECORD_FILE_PREFIX) {
            return Err(invalid("snapshot reference has an invalid file key"));
        }
        let bytes = read_verified_local_file(&entry)?;
        let (sha256, row) = crate::storage::format::metadata::seal_row(content_key, &bytes)
            .map_err(|error| invalid(format!("snapshot file encryption: {error}")))?;
        if sha256 != entry.sha256 {
            return Err(invalid("snapshot file differs from its reference marker"));
        }
        accept(path, row)?;
    }
    Ok(())
}

pub(in crate::storage) fn sha256_regular_file(
    path: &Path,
    expected_size: Option<u64>,
) -> StorageResult<(u64, String)> {
    let display = path.display();
    let file = admit::open_nofollow_read(path)
        .map_err(|e| invalid(format!("local file {display}: {e}")))?;
    let held = file
        .metadata()
        .map_err(|e| invalid(format!("local file {display}: {e}")))?;
    if !held.is_file() {
        return Err(invalid(format!(
            "local file {display} is not a regular file"
        )));
    }
    if held.len() > MAX_RECORD_FILE_BYTES {
        return Err(invalid(format!(
            "local file {display} is {} bytes; a record file holds at most {MAX_RECORD_FILE_BYTES} bytes",
            held.len()
        )));
    }
    if expected_size.is_some_and(|size| held.len() != size) {
        return Err(invalid(format!("local file {display} size mismatch")));
    }
    let limit = expected_size.unwrap_or(held.len());
    let (mut reader, mut hasher, mut buffer, mut seen) = (
        file.take(limit.saturating_add(1)),
        Sha256::new(),
        [0u8; 65536],
        0u64,
    );
    while let count @ 1.. = reader.read(&mut buffer)? {
        seen = seen
            .checked_add(count as u64)
            .ok_or_else(|| invalid("length overflow"))?;
        hasher.update(&buffer[..count]);
    }
    (seen == held.len())
        .then(|| (seen, hex_lower(hasher.finalize())))
        .ok_or_else(|| invalid(format!("local file {display} changed during hash")))
}

pub(crate) fn describe(runtime: &PackRuntime, path: &Path) -> StorageResult<DescribedFile> {
    let (_, node) = sidecar_path(runtime)?;
    let display = path.display();
    let canonical =
        fs::canonicalize(path).map_err(|e| invalid(format!("local file {display}: {e}")))?;
    canonical
        .to_str()
        .ok_or_else(|| invalid(format!("local file {display}: non-UTF-8 path")))?;
    if canonical.starts_with(fs::canonicalize(&node).unwrap_or(node)) {
        return Err(invalid(
            "a local file reference must stay outside the node home",
        ));
    }
    // A snapshot and a receiving device hold the file as one value of the file
    // store. A file the store cannot hold is refused inside sha256_regular_file,
    // at the write, because nothing after the write can back it up.
    let (length, sha256) = sha256_regular_file(&canonical, None)?;
    let file_name = canonical
        .file_name()
        .and_then(|name| name.to_str())
        .map(Into::into);
    Ok((canonical, file_name, length, sha256))
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

/// The referenced path of a record file, and whether a regular file of the
/// recorded size is still there. The bytes are not read, so a same-size edit
/// is seen only by a reader that checks the hash.
pub(crate) fn referenced_path(
    runtime: &PackRuntime,
    cas_key: &str,
    marker: &FileMarker,
) -> StorageResult<Option<(PathBuf, bool)>> {
    let entry = {
        let _guard = catalog_lock(runtime)?;
        load(&sidecar_path(runtime)?.0)?.files.remove(cas_key)
    };
    let Some(entry) = entry else { return Ok(None) };
    if entry.sha256 != marker.sha256 || entry.size_bytes != marker.size_bytes {
        return Err(invalid(
            "local file reference does not match its record marker",
        ));
    }
    let path = PathBuf::from(&entry.path);
    let intact = fs::metadata(&path).is_ok_and(|m| m.is_file() && m.len() == entry.size_bytes);
    Ok(Some((path, intact)))
}

fn read_verified_local_file(entry: &Entry) -> StorageResult<Vec<u8>> {
    if entry.size_bytes > LOCAL_FILE_INPUT_MEMORY_BYTES.min(MAX_RECORD_FILE_BYTES) {
        return Err(StorageError::write_limit(
            "local file exceeds in-memory cap",
        ));
    }
    let path = Path::new(&entry.path);
    let display = path.display();
    let mismatch = || invalid(format!("local file {display} marker mismatch"));
    let file = admit::open_nofollow_read(path)
        .map_err(|e| invalid(format!("local file {display}: {e}")))?;
    let held = file
        .metadata()
        .map_err(|e| invalid(format!("local file {display}: {e}")))?;
    if !held.is_file() || held.len() != entry.size_bytes {
        return Err(mismatch());
    }
    let capacity = usize::try_from(held.len()).map_err(|_| mismatch())?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(capacity)
        .map_err(|_| StorageError::write_limit("alloc refused"))?;
    file.take(entry.size_bytes.saturating_add(1))
        .read_to_end(&mut bytes)?;
    let matches = bytes.len() as u64 == held.len() && sha256_hex(&bytes) == entry.sha256;
    matches.then_some(bytes).ok_or_else(mismatch)
}
