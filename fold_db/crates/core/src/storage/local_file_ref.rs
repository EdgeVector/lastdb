//! Local file references for record file blobs.

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

#[derive(Default)]
pub(crate) struct SidecarPlan {
    pub(crate) remember: Vec<(String, PathBuf, String, u64)>,
    pub(crate) forget: Vec<String>,
}

impl SidecarPlan {
    pub(crate) fn is_empty(&self) -> bool {
        self.remember.is_empty() && self.forget.is_empty()
    }
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

fn resolve_regular(node: &Path, path: &Path) -> StorageResult<(PathBuf, u64)> {
    let canonical = fs::canonicalize(path).map_err(|e| {
        invalid(format!(
            "local file {} is not available: {e}",
            path.display()
        ))
    })?;
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
    Ok((canonical, meta.len()))
}

pub(crate) fn describe(runtime: &PackRuntime, path: &Path) -> StorageResult<DescribedFile> {
    let (canonical, expected_len) = resolve_regular(&node_dir(runtime)?, path)?;
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
    Ok(DescribedFile {
        file_name: canonical
            .file_name()
            .and_then(std::ffi::OsStr::to_str)
            .map(Into::into),
        path: canonical,
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
    for (cas_key, item_path, sha256, size_bytes) in plan.remember {
        let p = item_path
            .to_str()
            .ok_or_else(|| invalid("a local file reference path is not valid Unicode"))?;
        catalog.files.insert(
            cas_key,
            Entry {
                path: p.to_string(),
                sha256,
                size_bytes,
            },
        );
    }
    if catalog.files.is_empty() {
        return match fs::remove_file(&path) {
            Err(e) if e.kind() != ErrorKind::NotFound => Err(e.into()),
            _ => Ok(()),
        };
    }
    let tmp = path.with_extension("json.tmp");
    let mut file = File::create(&tmp)?;
    serde_json::to_writer(&mut file, &catalog)
        .map_err(|e| invalid(format!("local file reference catalog write failed: {e}")))?;
    file.sync_all()?;
    drop(file);
    Ok(fs::rename(&tmp, &path)?)
}

pub(crate) fn remember(
    rt: &PackRuntime,
    key: &str,
    p: &Path,
    sha: &str,
    size: u64,
) -> StorageResult<()> {
    let remember = vec![(key.into(), p.to_path_buf(), sha.into(), size)];
    apply(
        rt,
        SidecarPlan {
            remember,
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
    let disp = path.display();
    let mismatch = || {
        invalid(format!(
            "local file {disp} does not match its record marker"
        ))
    };
    let meta = fs::metadata(path)
        .map_err(|e| invalid(format!("local file {disp} is not available: {e}")))?;
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
    (bytes.len() as u64 == size_bytes && hex_lower(Sha256::digest(&bytes)) == sha256)
        .then_some(bytes)
        .ok_or_else(mismatch)
}
