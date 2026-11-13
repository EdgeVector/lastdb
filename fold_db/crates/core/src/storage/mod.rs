//! Native local storage, error types, traits, retired-format checks, and local file references.

pub mod cloud_backup_plan;
pub mod disk_backup;
pub mod disk_cleanup;
pub mod disk_pack;
pub mod error;
pub(crate) mod format;

pub(in crate::storage) use disk_cleanup::retired;
pub use disk_pack::transaction::traits::{self, KvMutation};
pub use error::{config, config::DatabaseConfig, StorageError};
pub(crate) use format::{content_key_proof, current_keys};

pub(crate) mod local_file_ref {
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

    type Rt = PackRuntime;
    type Described = (PathBuf, Option<String>, u64, String);
    pub(crate) type SidecarAdd = (String, PathBuf, String, u64);

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
    pub(crate) struct SidecarPlan(pub(crate) Vec<SidecarAdd>, pub(crate) Vec<String>);
    impl SidecarPlan {
        pub(crate) fn is_empty(&self) -> bool {
            self.0.is_empty() && self.1.is_empty()
        }
    }

    type OptBytes = Result<Option<Vec<u8>>, FoldDbError>;

    fn inv(m: impl std::fmt::Display) -> StorageError {
        StorageError::InvalidOperation(m.to_string())
    }
    fn catalog_lock() -> StorageResult<std::sync::MutexGuard<'static, ()>> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        let err = |_| StorageError::backend("local file reference catalog lock");
        LOCK.get_or_init(Mutex::default).lock().map_err(err)
    }
    fn sidecar_path(rt: &Rt) -> StorageResult<(PathBuf, PathBuf)> {
        let n = rt
            .physical
            .path()
            .parent()
            .ok_or_else(|| inv("no node dir"))?;
        Ok((n.join("local-file-refs.json"), n.to_path_buf()))
    }
    fn load(path: &Path) -> StorageResult<Catalog> {
        match fs::read(path) {
            Ok(bytes) => {
                let c: Catalog = serde_json::from_slice(&bytes)
                    .map_err(|e| inv(format!("local file reference catalog unreadable: {e}")))?;
                (c.version == 1)
                    .then_some(c)
                    .ok_or_else(|| inv("unsupported catalog version"))
            }
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(Catalog {
                version: 1,
                files: BTreeMap::new(),
            }),
            Err(e) => Err(e.into()),
        }
    }

    pub(super) fn has_references(rt: &Rt) -> StorageResult<bool> {
        let _guard = catalog_lock()?;
        Ok(!load(&sidecar_path(rt)?.0)?.files.is_empty())
    }

    pub(crate) fn describe(rt: &Rt, path: &Path) -> StorageResult<Described> {
        let (_, node) = sidecar_path(rt)?;
        let disp = path.display();
        let canon = fs::canonicalize(path).map_err(|e| inv(format!("local file {disp}: {e}")))?;
        if canon.starts_with(fs::canonicalize(&node).unwrap_or(node)) {
            let msg = "a local file reference must stay outside the node home";
            return Err(inv(msg));
        }
        let (meta, cdisp) = (fs::metadata(&canon)?, canon.display());
        if !meta.is_file() {
            return Err(inv(format!("local file {cdisp} is not a regular file")));
        }
        let (mut f, mut h, mut buf, mut len) =
            (File::open(&canon)?, Sha256::new(), [0u8; 65536], 0u64);
        while let n @ 1.. = f.read(&mut buf)? {
            len = len
                .checked_add(n as u64)
                .ok_or_else(|| inv("length overflow"))?;
            h.update(&buf[..n]);
        }
        if len != meta.len() {
            return Err(inv(format!("local file {cdisp} changed during hash")));
        }
        let fname = canon.file_name().and_then(|s| s.to_str()).map(Into::into);
        Ok((canon, fname, len, hex_lower(h.finalize())))
    }

    pub(crate) fn apply(rt: &Rt, plan: SidecarPlan) -> StorageResult<()> {
        if plan.is_empty() {
            return Ok(());
        }
        let _guard = catalog_lock()?;
        let (path, _) = sidecar_path(rt)?;
        let mut catalog = load(&path)?;
        plan.1.iter().for_each(|k| drop(catalog.files.remove(k)));
        for (cas_key, item_path, sha256, size_bytes) in plan.0 {
            let path = item_path
                .to_str()
                .ok_or_else(|| inv("non-UTF-8 path"))?
                .into();
            catalog.files.insert(
                cas_key,
                Entry {
                    path,
                    sha256,
                    size_bytes,
                },
            );
        }
        if catalog.files.is_empty() {
            return match fs::remove_file(&path) {
                Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
                r => Ok(r?),
            };
        }
        let tmp = path.with_extension("json.tmp");
        let mut file = File::create(&tmp)?;
        serde_json::to_writer(&mut file, &catalog)
            .map_err(|e| inv(format!("local file reference catalog write failed: {e}")))?;
        file.sync_all()?;
        Ok(fs::rename(&tmp, &path)?)
    }

    pub(crate) fn remember(rt: &Rt, k: &str, p: &Path, sha: &str, sz: u64) -> StorageResult<()> {
        let add = vec![(k.into(), p.to_path_buf(), sha.into(), sz)];
        apply(rt, SidecarPlan(add, Vec::new()))
    }

    pub(crate) async fn read_referenced(rt: &Rt, cas_key: &str, marker: &FileMarker) -> OptBytes {
        let entry = {
            let _guard = catalog_lock()?;
            load(&sidecar_path(rt)?.0)?.files.remove(cas_key)
        };
        let Some(entry) = entry else { return Ok(None) };
        if entry.sha256 != marker.sha256 || entry.size_bytes != marker.size_bytes {
            let msg = "local file reference does not match its record marker";
            return Err(FoldDbError::SecurityError(msg.into()));
        }
        let bytes = run_blocking(move || {
            if entry.size_bytes > LOCAL_FILE_INPUT_MEMORY_BYTES {
                let msg = "local file exceeds in-memory cap";
                return Err(StorageError::write_limit(msg));
            }
            let (path, disp) = (Path::new(&entry.path), Path::new(&entry.path).display());
            let mismatch = || inv(format!("local file {disp} marker mismatch"));
            let meta = fs::metadata(path).map_err(|e| inv(format!("local file {disp}: {e}")))?;
            if !meta.is_file() || meta.len() != entry.size_bytes {
                return Err(mismatch());
            }
            let mut bytes = Vec::new();
            let cap = usize::try_from(entry.size_bytes).unwrap_or(usize::MAX);
            bytes
                .try_reserve_exact(cap)
                .map_err(|_| StorageError::write_limit("alloc refused"))?;
            File::open(path)?.read_to_end(&mut bytes)?;
            let ok = bytes.len() as u64 == entry.size_bytes
                && hex_lower(Sha256::digest(&bytes)) == entry.sha256;
            ok.then_some(bytes).ok_or_else(mismatch)
        })
        .await?;
        Ok(Some(bytes))
    }
}
