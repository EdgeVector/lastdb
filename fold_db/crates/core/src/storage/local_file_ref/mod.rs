//! Local file references for record file blobs.
//!
//! The record stores a normal file marker. The path stays in
//! `<node>/local-file-refs.json`, beside `disk-run` and `disk-cas`, so a
//! backup hold never hard-links it and the cloud never sees the path.
//! Backup seals each referenced file into `cas_blobs` before it captures the
//! run, then deletes those rows. The hold keeps the sealed run. A fetch with
//! no cas row reads the external file and checks it against the marker.

use super::disk_pack::PackRuntime;
use super::error::{StorageError, StorageResult};
use lastdb_model::hex::hex_lower;
use serde::{Deserialize, Serialize};

mod bytes;
pub(crate) use bytes::{materialize, read_referenced, release, release_catalog};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

const SIDECAR_NAME: &str = "local-file-refs.json";
const CATALOG_VERSION: u32 = 1;

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

impl Default for Catalog {
    fn default() -> Self {
        Self {
            version: CATALOG_VERSION,
            files: BTreeMap::new(),
        }
    }
}

/// One sidecar update applied after the record commit succeeds.
#[derive(Default)]
pub(crate) struct SidecarPlan {
    pub(crate) remember: Vec<Remember>,
    pub(crate) forget: Vec<String>,
}

impl SidecarPlan {
    pub(crate) fn is_empty(&self) -> bool {
        self.remember.is_empty() && self.forget.is_empty()
    }
}

pub(crate) struct Remember {
    pub(crate) cas_key: String,
    pub(crate) path: PathBuf,
    pub(crate) sha256: String,
    pub(crate) size_bytes: u64,
}

/// A canonical regular file outside the node home, hashed without a size cap.
pub(crate) struct DescribedFile {
    pub(crate) path: PathBuf,
    pub(crate) file_name: Option<String>,
    pub(crate) size_bytes: u64,
    pub(crate) sha256: String,
}

fn catalog_lock() -> StorageResult<std::sync::MutexGuard<'static, ()>> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .map_err(|_| StorageError::BackendError("local file reference catalog lock".into()))
}

fn node_dir(runtime: &PackRuntime) -> StorageResult<PathBuf> {
    runtime
        .physical
        .path()
        .parent()
        .map(Path::to_path_buf)
        .ok_or_else(|| StorageError::InvalidOperation("disk-run has no node directory".into()))
}

fn sidecar_path(runtime: &PackRuntime) -> StorageResult<PathBuf> {
    Ok(node_dir(runtime)?.join(SIDECAR_NAME))
}

fn load(path: &Path) -> StorageResult<Catalog> {
    match fs::read(path) {
        Ok(bytes) => {
            let catalog: Catalog = serde_json::from_slice(&bytes).map_err(|error| {
                StorageError::InvalidOperation(format!(
                    "local file reference catalog is unreadable: {error}"
                ))
            })?;
            if catalog.version != CATALOG_VERSION {
                return Err(StorageError::InvalidOperation(
                    "local file reference catalog version is unsupported".into(),
                ));
            }
            Ok(catalog)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Catalog::default()),
        Err(error) => Err(error.into()),
    }
}

fn save(path: &Path, catalog: &Catalog) -> StorageResult<()> {
    if catalog.files.is_empty() {
        return match fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        };
    }
    let temporary = path.with_extension("json.tmp");
    {
        let mut file = File::create(&temporary)?;
        serde_json::to_writer(&mut file, catalog).map_err(|error| {
            StorageError::InvalidOperation(format!(
                "local file reference catalog could not be written: {error}"
            ))
        })?;
        file.sync_all()?;
    }
    fs::rename(&temporary, path)?;
    Ok(())
}

fn ensure_outside_node(runtime: &PackRuntime, file: &Path) -> StorageResult<()> {
    let node = node_dir(runtime)?;
    let node = fs::canonicalize(&node).unwrap_or(node);
    if file.starts_with(&node) {
        return Err(StorageError::InvalidOperation(
            "a local file reference must stay outside the node home".into(),
        ));
    }
    Ok(())
}

/// Canonicalize `path` off the async executor and refuse a file inside the node.
fn checked_source(node: &Path, path: &Path, size_bytes: u64) -> StorageResult<PathBuf> {
    let canonical = fs::canonicalize(path).map_err(|error| {
        StorageError::InvalidOperation(format!(
            "local file {} is not available for backup: {error}",
            path.display()
        ))
    })?;
    let node = fs::canonicalize(node).unwrap_or_else(|_| node.to_path_buf());
    if canonical.starts_with(&node) {
        return Err(StorageError::InvalidOperation(
            "a local file reference must stay outside the node home".into(),
        ));
    }
    let metadata = fs::metadata(&canonical)?;
    if !metadata.is_file() || metadata.len() != size_bytes {
        return Err(StorageError::InvalidOperation(format!(
            "local file {} does not match its record marker",
            canonical.display()
        )));
    }
    Ok(canonical)
}

fn hash_file(path: &Path) -> StorageResult<(u64, String)> {
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    let mut size = 0u64;
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        size = size
            .checked_add(count as u64)
            .ok_or_else(|| StorageError::InvalidOperation("local file length overflow".into()))?;
        hasher.update(&buffer[..count]);
    }
    Ok((size, hex_lower(hasher.finalize())))
}

/// Canonicalize `path`, require a regular file outside the node, and hash it.
pub(crate) fn describe(runtime: &PackRuntime, path: &Path) -> StorageResult<DescribedFile> {
    let canonical = fs::canonicalize(path).map_err(|error| {
        StorageError::InvalidOperation(format!(
            "local file {} is not available: {error}",
            path.display()
        ))
    })?;
    let metadata = fs::metadata(&canonical)?;
    if !metadata.is_file() {
        return Err(StorageError::InvalidOperation(format!(
            "local file {} is not a regular file",
            canonical.display()
        )));
    }
    ensure_outside_node(runtime, &canonical)?;
    let (size_bytes, sha256) = hash_file(&canonical)?;
    if size_bytes != metadata.len() {
        return Err(StorageError::InvalidOperation(format!(
            "local file {} changed while it was hashed",
            canonical.display()
        )));
    }
    let file_name = canonical
        .file_name()
        .and_then(|name| name.to_str())
        .map(str::to_string);
    Ok(DescribedFile {
        path: canonical,
        file_name,
        size_bytes,
        sha256,
    })
}

pub(crate) fn apply(runtime: &PackRuntime, plan: SidecarPlan) -> StorageResult<()> {
    if plan.is_empty() {
        return Ok(());
    }
    let _guard = catalog_lock()?;
    let path = sidecar_path(runtime)?;
    let mut catalog = load(&path)?;
    for key in &plan.forget {
        catalog.files.remove(key);
    }
    for item in plan.remember {
        let path = item.path.to_str().ok_or_else(|| {
            StorageError::InvalidOperation(
                "a local file reference path is not valid Unicode".into(),
            )
        })?;
        catalog.files.insert(
            item.cas_key,
            Entry {
                path: path.to_string(),
                sha256: item.sha256,
                size_bytes: item.size_bytes,
            },
        );
    }
    save(&path, &catalog)
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
            remember: vec![Remember {
                cas_key: cas_key.to_string(),
                path: path.to_path_buf(),
                sha256: sha256.to_string(),
                size_bytes,
            }],
            forget: Vec::new(),
        },
    )
}
