//! Retry only creation explicitly begun in an empty current-format node home.

use super::*;
use laststore::disk_pack::home::{HomeBuilder, HomeDescriptor};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::PathBuf;

const INITIALIZING: &str = "laststore-current-keys-v2-initializing";
const STAGING: &str = "physical";
pub(super) const NODE_BYTES: &[u8] = b"format=1\nhome=disk-pack\n";

/// The stable node-marker inode serializes creation and remains locked for the
/// runtime's lifetime. The directory marker authorizes only this new protocol.
pub(super) struct NodeHome {
    root: PathBuf,
    _lock: File,
    pub(super) initializing: bool,
}

impl NodeHome {
    pub(super) fn open(
        path: &Path,
        policy: &laststore::disk_pack::maintenance::SpacePolicy,
    ) -> StorageResult<Self> {
        std::fs::create_dir_all(path)?;
        let intent = path.join(INITIALIZING);
        let marker = path.join(NODE_MARKER);
        if !intent.try_exists()? && !marker.try_exists()? {
            if std::fs::read_dir(path)?.next().is_some() {
                return Err(refusal("creation requires an empty home"));
            }
            match std::fs::create_dir(&intent) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error.into()),
            }
        }
        let initializing = intent.try_exists()?;
        if initializing {
            require_directory(&intent)?;
            // The intent must survive before any physical file can appear.
            File::open(&intent)?.sync_all()?;
            File::open(path)?.sync_all()?;
        }
        require_regular_if_present(&marker)?;
        let mut lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(initializing)
            .truncate(false)
            .open(&marker)?;
        lock.try_lock().map_err(|error| {
            refusal(&format!(
                "another node owner holds the home or its lock failed: {error}"
            ))
        })?;
        // Recheck after the stable inode is locked; another creator can finish
        // between the earlier directory observation and this lock acquisition.
        let initializing = intent.try_exists()?;
        if initializing {
            prepare_creation(path, &intent, &mut lock, policy)?;
        } else {
            verify_file(&mut lock, NODE_BYTES)?;
            let mut fence = File::open(path.join("laststore-layout-v1"))?;
            verify_file(&mut fence, laststore::disk_pack::home::LEGACY_FENCE_BYTES)?;
        }
        Ok(Self {
            root: path.canonicalize()?,
            _lock: lock,
            initializing,
        })
    }

    fn complete(&self) -> StorageResult<()> {
        match std::fs::remove_dir(self.root.join(INITIALIZING)) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        File::open(&self.root)?.sync_all()?;
        Ok(())
    }
}

fn prepare_creation(
    root: &Path,
    intent: &Path,
    marker: &mut File,
    policy: &laststore::disk_pack::maintenance::SpacePolicy,
) -> StorageResult<()> {
    require_directory(intent)?;
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        let name = entry.file_name();
        match name.to_str() {
            Some(INITIALIZING | "disk-pack") => require_directory(&entry.path())?,
            Some(NODE_MARKER | "laststore-layout-v1") => {
                require_regular_if_present(&entry.path())?;
            }
            _ => return Err(refusal("unexpected data beside current creation state")),
        }
    }
    for entry in std::fs::read_dir(intent)? {
        let entry = entry?;
        if entry.file_name() != STAGING {
            return Err(refusal("unexpected current creation dependency"));
        }
        require_directory(&entry.path())?;
    }
    let _space =
        laststore::disk_pack::maintenance::reserve_space(root, policy, 1024 * 1024).map_err(map)?;
    let mut fence = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(root.join("laststore-layout-v1"))?;
    fence.write_all(laststore::disk_pack::home::LEGACY_FENCE_BYTES)?;
    fence.sync_all()?;
    marker.set_len(0)?;
    marker.write_all(NODE_BYTES)?;
    marker.sync_all()?;
    File::open(root)?.sync_all()?;

    let staging = intent.join(STAGING);
    if staging.try_exists()? {
        // This exact directory was never published to a caller. No source
        // writes are admitted before its atomic rename and proof transaction.
        std::fs::remove_dir_all(&staging)?;
        File::open(intent)?.sync_all()?;
    }
    let physical = root.join("disk-pack");
    if !physical.try_exists()? {
        HomeBuilder::create_empty_with_schema_record_refs(
            &staging,
            HomeDescriptor::new(1, 0).map_err(map)?,
        )
        .map_err(map)?;
        std::fs::rename(&staging, &physical)?;
        File::open(intent)?.sync_all()?;
        File::open(root)?.sync_all()?;
    }
    Ok(())
}

fn require_directory(path: &Path) -> StorageResult<()> {
    if !std::fs::symlink_metadata(path)?.file_type().is_dir() {
        return Err(refusal("current creation dependency is not a directory"));
    }
    Ok(())
}

fn require_regular_if_present(path: &Path) -> StorageResult<()> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if !metadata.file_type().is_file() => {
            Err(refusal("native node marker is not a regular file"))
        }
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn verify_file(file: &mut File, expected: &[u8]) -> StorageResult<()> {
    if file.metadata()?.len() != expected.len() as u64 {
        return Err(refusal("native node format fence differs"));
    }
    let mut bytes = vec![0; expected.len()];
    file.read_exact(&mut bytes)?;
    if bytes != expected {
        return Err(refusal("native node format fence differs"));
    }
    Ok(())
}

pub(super) fn refuse_in_progress_parent(path: &Path) -> StorageResult<()> {
    // A physical-home symlink must not bypass its node's creation boundary.
    let path = path.canonicalize()?;
    if let Some(parent) = path.parent() {
        if parent.join(INITIALIZING).try_exists()? {
            return Err(refusal("open the node home to finish current creation"));
        }
    }
    Ok(())
}

impl PackRuntime {
    pub(crate) fn fresh_initialization_pending(&self) -> bool {
        self.initializing.load(Ordering::Acquire)
    }

    pub(super) fn require_initialized(&self) -> StorageResult<()> {
        if self.fresh_initialization_pending() {
            return Err(refusal(
                "current creation requires its durable key proofs first",
            ));
        }
        Ok(())
    }

    /// Every native source mutation reaches this check before queue admission.
    /// Registry-only puts cannot create user data. The sole other permitted
    /// batch contains every current initialization key and no source records.
    pub(super) fn validate_initialization_write(
        &self,
        operations: &[HomeMutation],
    ) -> StorageResult<()> {
        if !self.fresh_initialization_pending() {
            return Ok(());
        }
        let keys = crate::db_operations::atom_store::CURRENT_NATIVE_INITIALIZATION_KEYS;
        let expected: Vec<_> = keys
            .iter()
            .map(|key| kv::coordinates("main", key.as_bytes())[0].clone())
            .collect();
        let mut found = vec![false; expected.len()];
        for operation in operations {
            if operation.collection == REGISTRY
                && operation.key.starts_with(NAMESPACES)
                && operation.body.as_deref() == Some(&[1])
            {
                continue;
            }
            let at = expected.iter().position(|(collection, key)| {
                collection == &operation.collection && key == &operation.key
            });
            match at {
                Some(at) if operation.body.is_some() && !found[at] => found[at] = true,
                _ => return Err(refusal("source writes are refused during current creation")),
            }
        }
        if found.iter().any(|found| *found) && !found.iter().all(|found| *found) {
            return Err(refusal(
                "current creation requires one complete key-proof batch",
            ));
        }
        Ok(())
    }

    /// The caller first validates the whole proof batch through the normal
    /// encryption seam. Cancellation retains the intent and the write guard.
    pub(crate) async fn finish_fresh_initialization(self: &Arc<Self>) -> StorageResult<()> {
        if !self.fresh_initialization_pending() {
            return Ok(());
        }
        if transaction::PendingBatch::current(self).is_some() {
            return Err(refusal(
                "current creation needs its own durable transaction",
            ));
        }
        let gate = self.enter_write().await;
        let runtime = Arc::clone(self);
        super::super::laststore::LastStoreKvStore::run_blocking(move || {
            let _gate = gate;
            if runtime.failed.load(Ordering::Acquire) {
                return Err(refusal("current creation requires a snapshot reopen"));
            }
            if runtime.fresh_initialization_pending() {
                runtime
                    .node_home
                    .as_ref()
                    .ok_or_else(|| refusal("current creation has no held node owner"))?
                    .complete()?;
                runtime.initializing.store(false, Ordering::Release);
            }
            Ok(())
        })
        .await
    }
}

fn refusal(message: &str) -> StorageError {
    StorageError::InvalidOperation(format!("native home creation refused: {message}"))
}
