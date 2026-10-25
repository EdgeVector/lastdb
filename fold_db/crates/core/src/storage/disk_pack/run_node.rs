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
//! Creation is explicit in this release: [`PackNamespacedStore::create_run`],
//! or `LASTDB_DISK_FORMAT=2` when a node opens an absent or empty home. An
//! empty home that opens without either still becomes a v1 home. The same
//! setting converts an existing v1 home once (see `convert_v2`). The run home
//! is complete before the marker appears, so a crash during creation leaves a
//! home that open refuses as not empty; the owner deletes it and creates again.
//!
//! As in a v1 home, a new home holds an intent file until the node writes its
//! key-format proofs. Until then the runtime admits only that first batch;
//! a crash before it leaves the intent, and the next open retries it.
//!
//! Each run home key derives from the node encryption key with its own label
//! and is never written to a file.

use super::*;
use laststore::disk_run::home::RunHome;
use std::io::Write;

pub(super) const RUN_NODE_BYTES: &[u8] = b"format=3\nhome=disk-run\ncas=disk-cas\n";
/// The marker of a v2 home from before the CAS run home. No such home has
/// user data; it is refused with a clear message.
pub(super) const EARLY_RUN_NODE_BYTES: &[u8] = b"format=3\nhome=disk-run\n";
pub(super) const RUN_DIR: &str = "disk-run";
pub(super) const CAS_DIR: &str = "disk-cas";
const INTENT: &str = "laststore-run-initializing";
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
    Ok(marker.as_deref() == Some(RUN_NODE_BYTES))
}

/// True when the owner asked for format v2 and `path` is absent or empty.
pub(super) fn wants_new_run_node(path: &Path) -> StorageResult<bool> {
    let asked = std::env::var("LASTDB_DISK_FORMAT").as_deref() == Ok("2");
    if !asked {
        return Ok(asked);
    }
    let entries = match std::fs::read_dir(path) {
        Ok(entries) => Some(entries),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error.into()),
    };
    Ok(entries.is_none_or(|mut entries| entries.next().is_none()))
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

pub(super) fn require_key(node_key: Option<&[u8; 32]>) -> StorageResult<&[u8; 32]> {
    node_key.ok_or_else(|| {
        StorageError::InvalidOperation(
            "a format v2 node home needs the node encryption key to open".into(),
        )
    })
}

impl PackNamespacedStore {
    /// Create a new format v2 node home in `path`, which must be absent or
    /// empty, and open it.
    pub fn create_run(path: &Path, node_key: &[u8; 32]) -> StorageResult<Self> {
        crate::db_operations::AtomStore::validate_boot_key_encoding_settings()?;
        std::fs::create_dir_all(path)?;
        if std::fs::read_dir(path)?.next().is_some() {
            return Err(StorageError::InvalidOperation(
                "format v2 creation requires an empty node home".into(),
            ));
        }
        drop(std::fs::File::create(path.join(INTENT))?);
        std::fs::File::open(path)?.sync_all()?;
        drop(RunHome::create(&path.join(RUN_DIR), &home_key(node_key)).map_err(map)?);
        drop(RunHome::create(&path.join(CAS_DIR), &cas_key(node_key)).map_err(map)?);
        let staged = path.join(format!(".{NODE_MARKER}.tmp"));
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&staged)?;
        file.write_all(RUN_NODE_BYTES)?;
        file.sync_all()?;
        std::fs::rename(&staged, path.join(NODE_MARKER))?;
        std::fs::File::open(path)?.sync_all()?;
        Self::open_run(path, Some(node_key))
    }

    pub(super) fn open_run(path: &Path, node_key: Option<&[u8; 32]>) -> StorageResult<Self> {
        let node_key = require_key(node_key)?;
        let home = Arc::new(RunHome::open(&path.join(RUN_DIR), &home_key(node_key)).map_err(map)?);
        let cas = Arc::new(RunHome::open(&path.join(CAS_DIR), &cas_key(node_key)).map_err(map)?);
        let v2 = physical::V2Home::new(home, cas);
        let root = v2.current_root();
        let native_backup_boundary = tokio::sync::watch::channel(CoreNativeBackupBoundary {
            source_home: v2.home.identity(),
            frontier: root.frontier(),
        })
        .0;
        let parts = parts::PartsLedger::from_env(0, root.generation(), root.frontier());
        let initializing = path.join(INTENT).try_exists()?;
        let store = Self::assemble(
            physical::Physical::V2(v2),
            physical::PhysicalRoot::V2(root),
            native_backup_boundary,
            parts,
            false,
            None,
            space::policy_from_env()?,
        );
        store
            .runtime
            .initializing
            .store(initializing, Ordering::Release);
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
