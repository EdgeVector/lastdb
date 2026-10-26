//! A node home in disk format v2: the node marker and two run homes.
//!
//! ```text
//! <node>/laststore-pack-node-v1   "format=3\nhome=disk-run\ncas=disk-cas\n"
//! <node>/disk-run/                the run home for every collection except CAS
//! <node>/disk-cas/                the run home for the CAS files (`cas_blobs`)
//! ```
//!
//! The CAS files are separate files so that a backup can send them to a
//! different store (B2) than the rest of the data (R2).
//!
//! Every node home is v2: a node that opens an absent or empty home creates
//! one (see `is_new_home`), and [`PackNamespacedStore::create_run`] creates
//! one on request. Both need the node encryption key. A home in any other
//! format is refused (see `not_v2`). The run homes are complete before the
//! marker appears, so a crash during creation leaves the creation intent
//! without a marker. The
//! runtime opens only after the marker, so that home holds no data: the next
//! open removes the partial run homes and creates the home again. A lock on
//! the node folder serializes creators, and a creator holds it until its
//! runtime is open, so the partial state of a live creator is never removed.
//!
//! A new home holds an intent file until the node writes its key-format
//! proofs. Until then the runtime admits only that first batch;
//! a crash before it leaves the intent, and the next open retries it.
//!
//! Each run home key derives from the node encryption key with its own label
//! and is never written to a file.

use super::*;
use laststore::disk_run::home::RunHome;
use rand::{rngs::OsRng, RngCore};
use std::io::Write;

pub(super) const RUN_NODE_BYTES: &[u8] = b"format=3\nhome=disk-run\ncas=disk-cas\n";
/// The marker of a v2 home from before the CAS run home. No such home has
/// user data; it is refused with a clear message.
pub(super) const EARLY_RUN_NODE_BYTES: &[u8] = b"format=3\nhome=disk-run\n";
pub(super) const RUN_DIR: &str = "disk-run";
pub(super) const CAS_DIR: &str = "disk-cas";
const INTENT: &str = "laststore-run-initializing";
const STAGED_MARKER: &str = ".laststore-pack-node-v1.tmp";
const HOME_KEY_LABEL: &[u8] = b"lastdb/run-home/v2";
const CAS_KEY_LABEL: &[u8] = b"lastdb/run-home/v2/cas";

/// True when `path` holds a v2 node marker.
pub(super) fn is_run_node(path: &Path) -> StorageResult<bool> {
    let marker = match std::fs::read(path.join(NODE_MARKER)) {
        Ok(bytes) => Some(bytes),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error.into()),
    };
    if marker.as_deref() == Some(EARLY_RUN_NODE_BYTES) {
        return Err(StorageError::InvalidOperation(
            "this format v2 home is from before the CAS run home (disk-cas); create it again"
                .into(),
        ));
    }
    Ok(marker.as_deref().is_some_and(|bytes| {
        bytes == RUN_NODE_BYTES || bytes == super::run_admission::TYPED_NODE_BYTES
    }))
}

/// True when `path` is absent or empty: a new node home, which is v2.
pub(super) fn is_new_home(path: &Path) -> StorageResult<bool> {
    let entries = match std::fs::read_dir(path) {
        Ok(entries) => Some(entries),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error.into()),
    };
    Ok(entries.is_none_or(|mut entries| entries.next().is_none()))
}

/// True while a v2 home holds its creation intent: from the start of its
/// creation until the node writes its key-format proofs.
pub(super) fn creation_pending(path: &Path) -> StorageResult<bool> {
    Ok(path.join(INTENT).try_exists()?)
}

/// True when a v2 home creation stopped before its node marker: the creation
/// intent is there and the marker is not.
pub(super) fn creation_stopped(path: &Path) -> StorageResult<bool> {
    Ok(creation_pending(path)? && !path.join(NODE_MARKER).try_exists()?)
}

/// Remove what a creation that stopped before its node marker left: the run
/// homes, their staging folders, and the staged marker. None of them holds
/// data. The caller holds the creation lock. Another entry stops the removal
/// before it starts, and nothing is removed.
fn clear_stopped_creation(path: &Path) -> StorageResult<()> {
    let staging = |name: &str, home: &str| {
        name.strip_prefix('.')
            .and_then(|rest| rest.strip_prefix(home))
            .is_some_and(|rest| rest.starts_with(".create-"))
    };
    let mut partial = Vec::new();
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let kind = entry.file_type()?;
        let file_name = entry.file_name();
        let name = file_name.to_str().unwrap_or_default();
        if kind.is_file() && name == INTENT {
            continue;
        }
        let run_home = [RUN_DIR, CAS_DIR]
            .into_iter()
            .any(|home| name == home || staging(name, home));
        if (kind.is_dir() && run_home) || (kind.is_file() && name == STAGED_MARKER) {
            partial.push((entry.path(), kind.is_dir()));
            continue;
        }
        return Err(StorageError::InvalidOperation(format!(
            "a format v2 node home creation stopped beside the unexpected entry {}; the home \
             holds no data, so move that entry away and start the node again",
            file_name.to_string_lossy()
        )));
    }
    for (entry, is_dir) in partial {
        if is_dir {
            std::fs::remove_dir_all(&entry)?;
        } else {
            std::fs::remove_file(&entry)?;
        }
    }
    Ok(())
}

/// The main run home key: HKDF-SHA256 of the node encryption key. The copy
/// is cleared when it drops.
pub(super) fn home_key(node_key: &[u8; 32]) -> zeroize::Zeroizing<[u8; 32]> {
    derive_key(node_key, HOME_KEY_LABEL)
}

/// The CAS run home key, with its own label.
pub(super) fn cas_key(node_key: &[u8; 32]) -> zeroize::Zeroizing<[u8; 32]> {
    derive_key(node_key, CAS_KEY_LABEL)
}

fn derive_key(node_key: &[u8; 32], label: &[u8]) -> zeroize::Zeroizing<[u8; 32]> {
    let hkdf = hkdf::Hkdf::<sha2::Sha256>::new(None, node_key);
    let mut key = zeroize::Zeroizing::new([0u8; 32]);
    hkdf.expand(label, key.as_mut())
        .expect("32 bytes is a valid HKDF-SHA256 output length");
    key
}

/// The refusal for a node home that is not disk format v2, such as a format
/// v1 home. This program has no reader and no converter for another format.
pub(super) fn not_v2(path: &Path) -> StorageError {
    StorageError::InvalidOperation(format!(
        "{} is not a disk format v2 node home; this program opens only format v2",
        path.display()
    ))
}

pub(super) fn require_key(node_key: Option<&[u8; 32]>) -> StorageResult<&[u8; 32]> {
    node_key.ok_or_else(|| {
        StorageError::InvalidOperation(
            "a format v2 node home needs the node encryption key; every new node home is v2".into(),
        )
    })
}

impl PackNamespacedStore {
    /// Create a new format v2 node home in `path` and open it. The home must
    /// be absent, empty, or a creation that stopped before its node marker;
    /// this creation replaces the partial run homes of a stopped creation.
    pub fn create_run(path: &Path, node_key: &[u8; 32]) -> StorageResult<Self> {
        crate::db_operations::AtomStore::validate_boot_key_encoding_settings()?;
        std::fs::create_dir_all(path)?;
        // The folder handle is the creation lock and the directory barrier.
        let folder = std::fs::File::open(path)?;
        folder.try_lock().map_err(|error| {
            StorageError::InvalidOperation(format!(
                "another owner is creating this format v2 node home or its lock failed: {error}"
            ))
        })?;
        // Another creator can finish between the caller's check and the lock.
        if !is_run_node(path)? {
            if creation_stopped(path)? {
                clear_stopped_creation(path)?;
            } else if std::fs::read_dir(path)?.next().is_some() {
                return Err(StorageError::InvalidOperation(
                    "format v2 creation requires an empty node home".into(),
                ));
            }
            drop(std::fs::File::create(path.join(INTENT))?);
            folder.sync_all()?;
            drop(RunHome::create(&path.join(RUN_DIR), &home_key(node_key)).map_err(map)?);
            drop(RunHome::create(&path.join(CAS_DIR), &cas_key(node_key)).map_err(map)?);
            let staged = path.join(STAGED_MARKER);
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&staged)?;
            file.write_all(RUN_NODE_BYTES)?;
            file.sync_all()?;
            std::fs::rename(&staged, path.join(NODE_MARKER))?;
            folder.sync_all()?;
        }
        // Keep the lock until the runtime holds the run home locks.
        let store = Self::open_run(path, Some(node_key));
        drop(folder);
        store
    }

    /// Create a new format v2 node home in `path`, which must be absent or
    /// empty, under a random node key that no one keeps: no later open can
    /// read the home. For in-process callers without a node identity, such as
    /// a benchmark on a temporary directory.
    pub fn create_ephemeral(path: &Path) -> StorageResult<Self> {
        if !is_new_home(path)? {
            return Err(StorageError::InvalidOperation(
                "an ephemeral node home needs an absent or empty folder".into(),
            ));
        }
        let mut node_key = zeroize::Zeroizing::new([0u8; 32]);
        OsRng.fill_bytes(node_key.as_mut());
        Self::create_run(path, &node_key)
    }

    pub(super) fn open_run(path: &Path, node_key: Option<&[u8; 32]>) -> StorageResult<Self> {
        let node_key = require_key(node_key)?;
        let range_order = super::run_admission::admit(path, node_key)?;
        let home = Arc::new(RunHome::open(&path.join(RUN_DIR), &home_key(node_key)).map_err(map)?);
        let cas = Arc::new(RunHome::open(&path.join(CAS_DIR), &cas_key(node_key)).map_err(map)?);
        let v2 = physical::V2Home::new(home, cas, range_order);
        let root = v2.current_root();
        let native_backup_boundary = tokio::sync::watch::channel(CoreNativeBackupBoundary {
            source_home: v2.home.identity(),
            frontier: root.frontier(),
        })
        .0;
        let initializing = path.join(INTENT).try_exists()?;
        let store = Self::assemble(
            v2,
            root,
            native_backup_boundary,
            initializing,
            space::policy_from_env()?,
        );
        store.runtime.resume_merges();
        Ok(store)
    }
}

/// Remove the creation intent of the v2 node home that holds `run_home`.
pub(super) fn complete_initialization(run_home: &Path) -> StorageResult<()> {
    let node = run_home.parent().ok_or_else(|| {
        StorageError::InvalidOperation("a format v2 run home has no node home".into())
    })?;
    match std::fs::remove_file(node.join(INTENT)) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    std::fs::File::open(node)?.sync_all()?;
    Ok(())
}
