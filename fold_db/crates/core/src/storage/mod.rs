//! Native local storage, typed helpers, local file references, and maintenance adapters.

pub mod cloud_backup_plan;
pub(crate) use format::{content_key_proof, retired};
pub mod disk_backup;
pub mod disk_cleanup;
pub mod disk_pack;
pub mod error;
pub(crate) mod format;

pub mod config {
    use std::path::PathBuf;

    #[derive(Clone, Debug)]
    pub struct DatabaseConfig {
        pub path: PathBuf,
    }

    impl DatabaseConfig {
        pub fn local(path: PathBuf) -> Self {
            Self { path }
        }
    }
}

pub mod traits {
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum KvMutation {
        Put { key: Vec<u8>, value: Vec<u8> },
        Delete { key: Vec<u8> },
    }

    impl KvMutation {
        pub fn put(key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self {
            Self::Put {
                key: key.into(),
                value: value.into(),
            }
        }
        pub fn delete(key: impl Into<Vec<u8>>) -> Self {
            Self::Delete { key: key.into() }
        }
        pub fn key(&self) -> &[u8] {
            let (Self::Put { key, .. } | Self::Delete { key }) = self;
            key
        }
    }

    #[derive(Debug, Clone, Default, PartialEq, Eq)]
    pub struct YieldedScan {
        pub rows: Vec<(Vec<u8>, Vec<u8>)>,
        pub resume: Option<Vec<u8>>,
    }
}

pub(crate) mod current_keys {
    use super::error::{StorageError, StorageResult};
    use super::format::{
        records::{applied_deletes, inline_files},
        CURRENT_FAMILIES, CURRENT_KEY_FORMAT_KEY, DELETE_REPLAY_CUTOFF_KEY, MAIN_NAMESPACE,
    };
    use lastdb_model::field_key_codec::decode_hash_range_suffix;
    use lastdb_model::field_key_id::{is_canonical_field_key_id, ALTERNATE_ENTRY_PREFIX};
    use lastdb_model::record_space::RECORD_PREFIX;

    pub(crate) mod families {
        pub(crate) use crate::storage::format::RECORD_FILE_PREFIX;
        use lastdb_model::{
            hex::hex_lower, portable_mutation::Position, record_space::RECORD_PREFIX,
        };
        use sha2::{Digest, Sha256};

        pub(crate) fn record_file_path(
            record: &str,
            version: &Position,
        ) -> super::StorageResult<String> {
            let space = record
                .strip_prefix(RECORD_PREFIX)
                .and_then(|r| r.split(':').next())
                .filter(|s| !s.is_empty())
                .ok_or_else(|| super::unsupported("a record file path needs a record key"))?;
            let hash = hex_lower(Sha256::digest(record.as_bytes()));
            Ok(format!(
                "{RECORD_FILE_PREFIX}{space}:{hash}:{}:{:020}",
                version.source_stream, version.sequence
            ))
        }
    }

    pub(crate) fn validate(namespace: &str, key: &[u8]) -> StorageResult<()> {
        if super::retired::is_journal_key(namespace, key) {
            return Err(StorageError::RetiredFormat(super::retired::JOURNAL_RETIRED));
        }
        if namespace != MAIN_NAMESPACE {
            return Ok(());
        }
        let key = std::str::from_utf8(key).map_err(|_| unsupported("non-UTF-8 main key"))?;
        if matches!(key, CURRENT_KEY_FORMAT_KEY | DELETE_REPLAY_CUTOFF_KEY) {
            return Ok(());
        }
        if !CURRENT_FAMILIES.contains(&key.split([':', '\0']).next().unwrap_or_default()) {
            return Err(unsupported("unknown key family"));
        }
        let rest = if let Some(rec) =
            applied_deletes::record_key(key).or_else(|| inline_files::record_key(key))
        {
            Some(
                rec.strip_prefix(RECORD_PREFIX)
                    .ok_or_else(|| unsupported("invalid applied-delete or inline file key"))?,
            )
        } else {
            key.strip_prefix(ALTERNATE_ENTRY_PREFIX)
                .or_else(|| key.strip_prefix(RECORD_PREFIX))
        };
        if let Some(rest) = rest {
            let Some((fk, sfx)) = rest.split_once(':') else {
                return Err(unsupported("invalid field key"));
            };
            if !is_canonical_field_key_id(fk) || decode_hash_range_suffix(sfx).is_none() {
                return Err(unsupported("invalid field key"));
            }
        }
        Ok(())
    }

    fn unsupported(reason: &str) -> StorageError {
        StorageError::invalid(format!(
            "unsupported key format: {reason}; start with a fresh home"
        ))
    }
}

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
    pub(crate) struct SidecarPlan(
        pub(crate) Vec<(String, PathBuf, String, u64)>,
        pub(crate) Vec<String>,
    );

    impl SidecarPlan {
        pub(crate) fn is_empty(&self) -> bool {
            self.0.is_empty() && self.1.is_empty()
        }
    }

    fn inv(m: impl std::fmt::Display) -> StorageError {
        StorageError::InvalidOperation(m.to_string())
    }

    fn catalog_lock() -> StorageResult<std::sync::MutexGuard<'static, ()>> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(Mutex::default)
            .lock()
            .map_err(|_| StorageError::backend("local file reference catalog lock"))
    }

    fn sidecar_path(runtime: &PackRuntime) -> StorageResult<(PathBuf, PathBuf)> {
        let node = runtime
            .physical
            .path()
            .parent()
            .map(Path::to_path_buf)
            .ok_or_else(|| inv("disk-run has no node directory"))?;
        Ok((node.join("local-file-refs.json"), node))
    }

    fn load(path: &Path) -> StorageResult<Catalog> {
        match fs::read(path) {
            Ok(bytes) => {
                let cat: Catalog = serde_json::from_slice(&bytes)
                    .map_err(|e| inv(format!("local file reference catalog is unreadable: {e}")))?;
                (cat.version == 1)
                    .then_some(cat)
                    .ok_or_else(|| inv("local file reference catalog version is unsupported"))
            }
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(Catalog {
                version: 1,
                files: BTreeMap::new(),
            }),
            Err(e) => Err(e.into()),
        }
    }

    pub(crate) fn describe(
        runtime: &PackRuntime,
        path: &Path,
    ) -> StorageResult<(PathBuf, Option<String>, u64, String)> {
        let (_, node) = sidecar_path(runtime)?;
        let canon = fs::canonicalize(path).map_err(|e| {
            inv(format!(
                "local file {} is not available: {e}",
                path.display()
            ))
        })?;
        if canon.starts_with(fs::canonicalize(&node).unwrap_or(node)) {
            return Err(inv(
                "a local file reference must stay outside the node home",
            ));
        }
        let meta = fs::metadata(&canon)?;
        if !meta.is_file() {
            return Err(inv(format!(
                "local file {} is not a regular file",
                canon.display()
            )));
        }
        let (mut f, mut h, mut buf, mut len) =
            (File::open(&canon)?, Sha256::new(), [0u8; 65536], 0u64);
        loop {
            let n = f.read(&mut buf)?;
            if n == 0 {
                break;
            }
            len = len
                .checked_add(n as u64)
                .ok_or_else(|| inv("local file length overflow"))?;
            h.update(&buf[..n]);
        }
        if len != meta.len() {
            return Err(inv(format!(
                "local file {} changed while it was hashed",
                canon.display()
            )));
        }
        let name = canon
            .file_name()
            .and_then(std::ffi::OsStr::to_str)
            .map(Into::into);
        Ok((canon, name, len, hex_lower(h.finalize())))
    }

    pub(crate) fn apply(runtime: &PackRuntime, plan: SidecarPlan) -> StorageResult<()> {
        if plan.is_empty() {
            return Ok(());
        }
        let _guard = catalog_lock()?;
        let (path, _) = sidecar_path(runtime)?;
        let mut catalog = load(&path)?;
        for key in &plan.1 {
            catalog.files.remove(key);
        }
        for (cas_key, item_path, sha256, size_bytes) in plan.0 {
            let p = item_path
                .to_str()
                .ok_or_else(|| inv("a local file reference path is not valid Unicode"))?;
            catalog.files.insert(
                cas_key,
                Entry {
                    path: p.into(),
                    sha256,
                    size_bytes,
                },
            );
        }
        if catalog.files.is_empty() {
            return match fs::remove_file(&path) {
                Ok(()) => Ok(()),
                Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
                Err(e) => Err(e.into()),
            };
        }
        let tmp = path.with_extension("json.tmp");
        let mut file = File::create(&tmp)?;
        serde_json::to_writer(&mut file, &catalog)
            .map_err(|e| inv(format!("local file reference catalog write failed: {e}")))?;
        file.sync_all()?;
        Ok(fs::rename(&tmp, &path)?)
    }

    pub(crate) fn remember(
        rt: &PackRuntime,
        key: &str,
        p: &Path,
        sha: &str,
        size: u64,
    ) -> StorageResult<()> {
        apply(
            rt,
            SidecarPlan(
                vec![(key.into(), p.to_path_buf(), sha.into(), size)],
                Vec::new(),
            ),
        )
    }

    pub(crate) async fn read_referenced(
        runtime: &PackRuntime,
        cas_key: &str,
        marker: &FileMarker,
    ) -> Result<Option<Vec<u8>>, FoldDbError> {
        let entry = {
            let _guard = catalog_lock()?;
            load(&sidecar_path(runtime)?.0)?.files.remove(cas_key)
        };
        let Some(entry) = entry else { return Ok(None) };
        if entry.sha256 != marker.sha256 || entry.size_bytes != marker.size_bytes {
            return Err(FoldDbError::SecurityError(
                "local file reference does not match its record marker".into(),
            ));
        }
        Ok(Some(
            run_blocking(move || {
                if entry.size_bytes > LOCAL_FILE_INPUT_MEMORY_BYTES {
                    return Err(StorageError::write_limit(
                        "local file reference exceeds the in-memory file cap",
                    ));
                }
                let path = Path::new(&entry.path);
                let disp = path.display();
                let mismatch = || {
                    inv(format!(
                        "local file {disp} does not match its record marker"
                    ))
                };
                let meta = fs::metadata(path)
                    .map_err(|e| inv(format!("local file {disp} is not available: {e}")))?;
                if !meta.is_file() || meta.len() != entry.size_bytes {
                    return Err(mismatch());
                }
                let mut bytes = Vec::new();
                bytes
                    .try_reserve_exact(usize::try_from(entry.size_bytes).unwrap_or(usize::MAX))
                    .map_err(|_| {
                        StorageError::write_limit("local file reference allocation refused")
                    })?;
                File::open(path)?.read_to_end(&mut bytes)?;
                (bytes.len() as u64 == entry.size_bytes
                    && hex_lower(Sha256::digest(&bytes)) == entry.sha256)
                    .then_some(bytes)
                    .ok_or_else(mismatch)
            })
            .await?,
        ))
    }
}

pub use config::DatabaseConfig;
pub use error::StorageError;
pub use traits::KvMutation;
