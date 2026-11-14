//! Node home creation, format admission, and v2 run home opening.

use super::admit::admit_stopped_home;
use super::{
    cas_delete, kv, map, physical, NativeReadView, PackNamespacedStore, PackRuntime, StorageError,
    StorageResult,
};
use crate::storage::content_key_proof;
use crate::storage::disk_backup::{accepted::Log, prearm};
use crate::storage::disk_cleanup::CleanupJob;
use crate::storage::format::metadata::{
    initial_local_author, FILE_RETENTION_COMPLETE, FILE_RETENTION_KEY, LOCAL_AUTHOR_KEY,
};
use crate::storage::format::{
    CAS_KEY_LABEL, CREATION_INTENT, CURRENT_KEY_FORMAT_KEY, CURRENT_KEY_FORMAT_VALUE,
    HOME_KEY_LABEL, MAIN_NAMESPACE, METADATA_NAMESPACE, NODE_MARKER, RECORD_NODE_BYTES,
    STAGED_NODE_MARKER,
};
use laststore::disk_run::home::{HomeMutation, RunHome};
use laststore::space::SpacePolicy;
use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Write};
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex, RwLock};

pub use crate::storage::format::{CAS_DIR, RUN_DIR};

pub(in crate::storage) fn home_key(node_key: &[u8; 32]) -> zeroize::Zeroizing<[u8; 32]> {
    derive_key(node_key, HOME_KEY_LABEL)
}

pub(in crate::storage) fn cas_key(node_key: &[u8; 32]) -> zeroize::Zeroizing<[u8; 32]> {
    derive_key(node_key, CAS_KEY_LABEL)
}

fn derive_key(node_key: &[u8; 32], label: &[u8]) -> zeroize::Zeroizing<[u8; 32]> {
    let mut key = zeroize::Zeroizing::new([0u8; 32]);
    hkdf::Hkdf::<sha2::Sha256>::new(None, node_key)
        .expand(label, key.as_mut())
        .expect("HKDF");
    key
}

fn creation_stopped(path: &Path) -> StorageResult<bool> {
    Ok(path.join(CREATION_INTENT).try_exists()? && !path.join(NODE_MARKER).try_exists()?)
}

fn require_key(node_key: Option<&[u8; 32]>) -> StorageResult<&[u8; 32]> {
    node_key.ok_or_else(|| StorageError::invalid("a format v2 node home needs the encryption key"))
}

fn initialize_home(home: &RunHome, node_key: &[u8; 32]) -> StorageResult<()> {
    let author = initial_local_author(home.identity())?;
    let proof = content_key_proof::create(node_key)?;
    let items = [
        (
            MAIN_NAMESPACE,
            CURRENT_KEY_FORMAT_KEY.as_bytes(),
            CURRENT_KEY_FORMAT_VALUE.to_vec(),
        ),
        (content_key_proof::NAMESPACE, content_key_proof::KEY, proof),
        (METADATA_NAMESPACE, LOCAL_AUTHOR_KEY.as_bytes(), author),
        (
            METADATA_NAMESPACE,
            FILE_RETENTION_KEY.as_bytes(),
            FILE_RETENTION_COMPLETE.to_vec(),
        ),
    ];
    let ops = items
        .into_iter()
        .map(|(ns, k, body)| {
            let (col, key) = kv::coordinate(ns, k);
            HomeMutation::new(col, key, Some(body))
        })
        .collect();
    home.commit(ops).map(|_| ()).map_err(map)
}

fn clean_stopped_creation(path: &Path) -> StorageResult<()> {
    let mut partial = Vec::new();
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let (kind, fname) = (entry.file_type()?, entry.file_name());
        let name = fname.to_str().unwrap_or_default();
        if kind.is_file() && name == CREATION_INTENT {
            continue;
        }
        let is_tmp = |home| {
            name.strip_prefix('.')
                .and_then(|rest| rest.strip_prefix(home))
                .is_some_and(|rest| rest.starts_with(".create-"))
        };
        let run_home = [RUN_DIR, CAS_DIR]
            .into_iter()
            .any(|h| name == h || is_tmp(h));
        if (kind.is_dir() && run_home) || (kind.is_file() && name == STAGED_NODE_MARKER) {
            partial.push((entry.path(), kind.is_dir()));
        } else {
            let lossy = fname.to_string_lossy();
            return Err(StorageError::invalid(format!(
                "creation stopped beside unexpected entry {lossy}; move it away and retry"
            )));
        }
    }
    for (entry_path, is_dir) in partial {
        if is_dir {
            fs::remove_dir_all(&entry_path)?;
        } else {
            fs::remove_file(&entry_path)?;
        }
    }
    Ok(())
}

impl PackNamespacedStore {
    pub(crate) fn open_keyed(
        path: &Path,
        key: Option<&[u8; 32]>,
        capture_accepted: bool,
        capture_initial_snapshot: bool,
    ) -> StorageResult<Self> {
        let is_new = match fs::read_dir(path) {
            Ok(mut dir) => dir.next().is_none(),
            Err(e) if e.kind() == ErrorKind::NotFound => true,
            Err(e) => return Err(e.into()),
        };
        if is_new || creation_stopped(path)? {
            return Self::create_run(path, require_key(key)?, capture_accepted);
        }
        Self::open_run(path, key, capture_accepted, capture_initial_snapshot)
    }

    fn create_run(path: &Path, node_key: &[u8; 32], capture_accepted: bool) -> StorageResult<Self> {
        fs::create_dir_all(path)?;
        let folder = File::open(path)?;
        folder.try_lock().map_err(|e| {
            StorageError::invalid(format!(
                "another owner is creating this home or lock failed: {e}"
            ))
        })?;
        if !path.join(NODE_MARKER).try_exists()? {
            if creation_stopped(path)? {
                clean_stopped_creation(path)?;
            } else if fs::read_dir(path)?.next().is_some() {
                return Err(StorageError::invalid(
                    "format v2 creation requires an empty node home",
                ));
            }
            drop(File::create(path.join(CREATION_INTENT))?);
            folder.sync_all()?;
            let main = RunHome::create(&path.join(RUN_DIR), &home_key(node_key)).map_err(map)?;
            drop(RunHome::create(&path.join(CAS_DIR), &cas_key(node_key)).map_err(map)?);
            initialize_home(&main, node_key)?;
            drop(main);
            let staged = path.join(STAGED_NODE_MARKER);
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&staged)?;
            file.write_all(RECORD_NODE_BYTES)?;
            file.sync_all()?;
            fs::rename(&staged, path.join(NODE_MARKER))?;
            folder.sync_all()?;
        }
        let store = Self::open_run(path, Some(node_key), capture_accepted, false);
        drop(folder);
        store
    }

    fn open_run(
        path: &Path,
        node_key: Option<&[u8; 32]>,
        capture_accepted: bool,
        capture_initial_snapshot: bool,
    ) -> StorageResult<Self> {
        let node_key = require_key(node_key)?;
        admit_stopped_home(path, node_key)?;
        let mut space_policy = SpacePolicy::default();
        if let Some(raw) = std::env::var_os("LASTDB_DISK_CAP_BYTES") {
            let parsed = raw
                .to_str()
                .and_then(|s| s.parse::<u64>().ok())
                .filter(|&n| n > 0);
            space_policy.disk_cap_bytes = parsed
                .ok_or_else(|| StorageError::invalid("LASTDB_DISK_CAP_BYTES must be positive"))?;
        }
        let home = Arc::new(RunHome::open(&path.join(RUN_DIR), &home_key(node_key)).map_err(map)?);
        let cas = Arc::new(RunHome::open(&path.join(CAS_DIR), &cas_key(node_key)).map_err(map)?);
        let mut physical = physical::V2Home::new(home, cas);
        physical.cas_delete_recovery = cas_delete::recover(&physical)?;
        let current = physical.current_root();
        let cleanup_job = Arc::new(CleanupJob::open(&physical));
        let accepted = capture_accepted.then(|| {
            let log = Log::open(physical.path(), current.frontier());
            if capture_initial_snapshot {
                prearm::prepare_capture(&physical, &log, node_key);
            }
            log
        });
        let initializing = AtomicBool::new(path.join(CREATION_INTENT).try_exists()?);
        let view = NativeReadView {
            root: current,
            revision: 0,
        };
        let runtime = Arc::new(PackRuntime {
            physical,
            cleanup_job,
            accepted,
            local_file_refs_lock: Mutex::default(),
            view: Arc::new(RwLock::new(view)),
            prepare_gate: Arc::default(),
            write_gate: Arc::default(),
            queue: Mutex::default(),
            completed: tokio::sync::watch::channel(0).0,
            core: Arc::default(),
            failed: AtomicBool::new(false),
            serving: AtomicBool::new(false),
            initializing,
            failed_write: Mutex::new(None),
            space_policy,
        });
        runtime.spawn_merges();
        Ok(Self { runtime })
    }
}
