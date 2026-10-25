//! A node home in disk format v2: the node marker and one run home.
//!
//! ```text
//! <node>/laststore-pack-node-v1   "format=3\nhome=disk-run\n"
//! <node>/disk-run/                the run home
//! ```
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
//! The run home key derives from the node encryption key and is never written
//! to a file.

use super::*;
use laststore::disk_run::home::RunHome;
use std::io::Write;

pub(super) const RUN_NODE_BYTES: &[u8] = b"format=3\nhome=disk-run\n";
pub(super) const RUN_DIR: &str = "disk-run";
const INTENT: &str = "laststore-run-initializing";
const HOME_KEY_LABEL: &[u8] = b"lastdb/run-home/v2";

/// True when `path` holds a v2 node marker.
pub(super) fn is_run_node(path: &Path) -> StorageResult<bool> {
    let marker = match std::fs::read(path.join(NODE_MARKER)) {
        Ok(bytes) => Some(bytes),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error.into()),
    };
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

/// The run home key: HKDF-SHA256 of the node encryption key. The copy is
/// cleared when it drops.
pub(super) fn home_key(node_key: &[u8; 32]) -> zeroize::Zeroizing<[u8; 32]> {
    let hkdf = hkdf::Hkdf::<sha2::Sha256>::new(None, node_key);
    let mut key = zeroize::Zeroizing::new([0u8; 32]);
    hkdf.expand(HOME_KEY_LABEL, key.as_mut())
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
        let key = home_key(require_key(node_key)?);
        let home = Arc::new(RunHome::open(&path.join(RUN_DIR), &key).map_err(map)?);
        let root = home.current_root();
        let native_backup_boundary = tokio::sync::watch::channel(CoreNativeBackupBoundary {
            source_home: home.identity(),
            frontier: root.frontier(),
        })
        .0;
        let parts = parts::PartsLedger::from_env(0, root.generation(), root.frontier());
        let initializing = path.join(INTENT).try_exists()?;
        let store = Self::assemble(
            physical::Physical::V2(physical::V2Home::new(home)),
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
