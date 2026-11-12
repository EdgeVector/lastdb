//! Local file references for record file blobs.

use super::disk_pack::{run_blocking, PackRuntime};
use super::error::{StorageError, StorageResult};
use super::format::records::{require_row, seal_file_row};
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
use std::sync::{Arc, Mutex, OnceLock};

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

pub(crate) struct DescribedFile {
    pub(crate) path: PathBuf,
    pub(crate) file_name: Option<String>,
    pub(crate) size_bytes: u64,
    pub(crate) sha256: String,
}

fn invalid(detail: impl std::fmt::Display) -> StorageError {
    StorageError::InvalidOperation(detail.to_string())
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
        .ok_or_else(|| invalid("disk-run has no node directory"))
}

fn load(path: &Path) -> StorageResult<Catalog> {
    match fs::read(path) {
        Ok(bytes) => {
            let catalog: Catalog = serde_json::from_slice(&bytes)
                .map_err(|e| invalid(format!("local file reference catalog is unreadable: {e}")))?;
            (catalog.version == CATALOG_VERSION)
                .then_some(catalog)
                .ok_or_else(|| invalid("local file reference catalog version is unsupported"))
        }
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(Catalog {
            version: CATALOG_VERSION,
            files: BTreeMap::new(),
        }),
        Err(e) => Err(e.into()),
    }
}

fn load_files(runtime: &PackRuntime) -> StorageResult<BTreeMap<String, Entry>> {
    let _guard = catalog_lock()?;
    Ok(load(&node_dir(runtime)?.join(SIDECAR_NAME))?.files)
}

fn save(path: &Path, catalog: &Catalog) -> StorageResult<()> {
    if catalog.files.is_empty() {
        return match fs::remove_file(path) {
            Err(e) if e.kind() != ErrorKind::NotFound => Err(e.into()),
            _ => Ok(()),
        };
    }
    let tmp = path.with_extension("json.tmp");
    let mut file = File::create(&tmp)?;
    serde_json::to_writer(&mut file, catalog)
        .map_err(|e| invalid(format!("local file reference catalog write failed: {e}")))?;
    file.sync_all()?;
    drop(file);
    Ok(fs::rename(&tmp, path)?)
}

fn resolve_regular(
    node: &Path,
    path: &Path,
    expected_size: Option<u64>,
    context: &str,
) -> StorageResult<(PathBuf, u64)> {
    let canonical = fs::canonicalize(path)
        .map_err(|e| invalid(format!("local file {} {context}: {e}", path.display())))?;
    if canonical.starts_with(fs::canonicalize(node).unwrap_or_else(|_| node.to_path_buf())) {
        return Err(invalid(
            "a local file reference must stay outside the node home",
        ));
    }
    let meta = fs::metadata(&canonical)?;
    if !meta.is_file() {
        return Err(invalid(format!(
            "local file {} is not a regular file",
            canonical.display()
        )));
    }
    if expected_size.is_some_and(|size| meta.len() != size) {
        return Err(invalid(format!(
            "local file {} does not match its record marker",
            canonical.display()
        )));
    }
    Ok((canonical, meta.len()))
}

pub(crate) fn describe(runtime: &PackRuntime, path: &Path) -> StorageResult<DescribedFile> {
    let (canonical, expected_len) =
        resolve_regular(&node_dir(runtime)?, path, None, "is not available")?;
    let mut file = File::open(&canonical)?;
    let (mut hasher, mut buf, mut size_bytes) = (Sha256::new(), [0u8; 64 * 1024], 0u64);
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        size_bytes = size_bytes
            .checked_add(n as u64)
            .ok_or_else(|| invalid("local file length overflow"))?;
        hasher.update(&buf[..n]);
    }
    if size_bytes != expected_len {
        return Err(invalid(format!(
            "local file {} changed while it was hashed",
            canonical.display()
        )));
    }
    let file_name = canonical
        .file_name()
        .and_then(|n| n.to_str())
        .map(str::to_string);
    Ok(DescribedFile {
        path: canonical,
        file_name,
        size_bytes,
        sha256: hex_lower(hasher.finalize()),
    })
}

pub(crate) fn apply(runtime: &PackRuntime, plan: SidecarPlan) -> StorageResult<()> {
    if plan.is_empty() {
        return Ok(());
    }
    let _guard = catalog_lock()?;
    let path = node_dir(runtime)?.join(SIDECAR_NAME);
    let mut catalog = load(&path)?;
    for key in &plan.forget {
        catalog.files.remove(key);
    }
    for item in plan.remember {
        let p = item
            .path
            .to_str()
            .ok_or_else(|| invalid("a local file reference path is not valid Unicode"))?;
        catalog.files.insert(
            item.cas_key,
            Entry {
                path: p.to_string(),
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
    let item = Remember {
        cas_key: cas_key.to_string(),
        path: path.to_path_buf(),
        sha256: sha256.to_string(),
        size_bytes,
    };
    apply(
        runtime,
        SidecarPlan {
            remember: vec![item],
            forget: Vec::new(),
        },
    )
}

pub(crate) async fn read_referenced(
    runtime: &PackRuntime,
    cas_key: &str,
    marker: &FileMarker,
) -> Result<Option<Vec<u8>>, FoldDbError> {
    let Some(entry) = load_files(runtime)?.remove(cas_key) else {
        return Ok(None);
    };
    if entry.sha256 != marker.sha256 || entry.size_bytes != marker.size_bytes {
        return Err(FoldDbError::SecurityError(
            "local file reference does not match its record marker".into(),
        ));
    }
    let path = PathBuf::from(entry.path);
    Ok(Some(
        run_blocking(move || read_plaintext(&path, entry.size_bytes, &entry.sha256)).await?,
    ))
}

fn read_plaintext(path: &Path, size_bytes: u64, sha256: &str) -> StorageResult<Vec<u8>> {
    if size_bytes > LOCAL_FILE_INPUT_MEMORY_BYTES {
        return Err(StorageError::WriteResourceLimit {
            detail: "local file reference exceeds the in-memory file cap".into(),
        });
    }
    let mismatch = || {
        invalid(format!(
            "local file {} does not match its record marker",
            path.display()
        ))
    };
    let meta = fs::metadata(path).map_err(|e| {
        invalid(format!(
            "local file {} is not available: {e}",
            path.display()
        ))
    })?;
    if !meta.is_file() || meta.len() != size_bytes {
        return Err(mismatch());
    }
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(usize::try_from(size_bytes).unwrap_or(usize::MAX))
        .map_err(|_| StorageError::WriteResourceLimit {
            detail: "local file reference allocation refused".into(),
        })?;
    File::open(path)?.read_to_end(&mut bytes)?;
    if bytes.len() as u64 != size_bytes || hex_lower(Sha256::digest(&bytes)) != sha256 {
        return Err(mismatch());
    }
    Ok(bytes)
}

pub(crate) async fn materialize(
    runtime: &Arc<PackRuntime>,
    account_key: &[u8; 32],
) -> StorageResult<Vec<Vec<u8>>> {
    let node = node_dir(runtime)?;
    let entries = load_files(runtime)?;
    let (mut release_keys, mut put_keys) = (Vec::with_capacity(entries.len()), Vec::new());
    for (cas_key, entry) in entries {
        let key = cas_key.into_bytes();
        release_keys.push(key.clone());
        let step = async {
            let (n, path, size) = (node.clone(), PathBuf::from(&entry.path), entry.size_bytes);
            let (canonical, _) = run_blocking(move || {
                resolve_regular(&n, &path, Some(size), "is not available for backup")
            })
            .await?;
            if let Some(row) = runtime.cas_get(&key).await? {
                let marker = FileMarker {
                    name: None,
                    media_type: None,
                    size_bytes: size,
                    sha256: entry.sha256.clone(),
                };
                if require_row(account_key, &marker, &row).is_ok() {
                    return Ok(false);
                }
            }
            let sha = entry.sha256.clone();
            let plain = run_blocking(move || read_plaintext(&canonical, size, &sha)).await?;
            let (row, sealed_sha) = seal_file_row(account_key, &plain)
                .map_err(|e| invalid(format!("local file seal failed: {e}")))?;
            if sealed_sha != entry.sha256 {
                return Err(invalid(
                    "sealed local file does not match its record marker",
                ));
            }
            runtime.cas_put(&key, row).await?;
            Ok(true)
        };
        match step.await {
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

pub(crate) async fn release(runtime: &Arc<PackRuntime>, keys: &[Vec<u8>]) -> StorageResult<()> {
    if keys.is_empty() {
        return Ok(());
    }
    runtime.cas_delete(keys.to_vec()).await
}

pub(crate) async fn release_catalog(runtime: &Arc<PackRuntime>) -> StorageResult<()> {
    let keys: Vec<_> = load_files(runtime)?
        .into_keys()
        .map(String::into_bytes)
        .collect();
    release(runtime, &keys).await
}
