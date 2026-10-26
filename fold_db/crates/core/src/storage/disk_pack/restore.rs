//! Validate an inactive native restore or finish its fresh wrapper. No source rewrite.
mod proof;

use super::{initialization::NODE_BYTES, map, StorageError, StorageResult, NODE_MARKER};
use laststore::disk_pack::backup::{prepare_snapshot_directories, validate_snapshot};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

/// Read-only validation retains the normal node-owner lock through enrollment.
/// No marker, writer, directory, or source record is created by this guard.
pub struct RestoredNodeGuard {
    root: std::path::PathBuf,
    directory: File,
    physical: File,
    marker: File,
    current: laststore::disk_pack::Current,
}

impl RestoredNodeGuard {
    /// Refuse replacement of the selected home, stable lock inode, or CURRENT.
    pub fn check_binding(&self) -> StorageResult<()> {
        same_entry(&self.directory, &self.root)?;
        same_entry(&self.physical, &self.root.join("disk-pack"))?;
        same_entry(&self.marker, &self.root.join(NODE_MARKER))?;
        let bytes = existing_bytes(&self.root.join("disk-pack/CURRENT"), 4096)?;
        let fixed: [u8; 4096] = bytes
            .try_into()
            .map_err(|_| refusal("restored CURRENT size differs"))?;
        let current = laststore::disk_pack::Current::decode(&fixed).map_err(map)?;
        if current != self.current {
            return Err(refusal("restored CURRENT changed during enrollment"));
        }
        laststore::disk_pack::home::DiskHome::open(&self.root.join("disk-pack")).map_err(map)?;
        Ok(())
    }
}

/// Validate an existing inactive node without opening a writer or changing bytes.
/// The fixed logical-format read proves that the supplied account key opens it.
pub fn validate_restored_node(
    root: &Path,
    account_key: &[u8; 32],
) -> StorageResult<RestoredNodeGuard> {
    let directory = open_existing(root, true)?;
    let physical = open_existing(&root.join("disk-pack"), true)?;
    let mut marker = open_existing(&root.join(NODE_MARKER), false)?;
    marker
        .try_lock()
        .map_err(|_| refusal("another node owner holds the restored home"))?;
    verify_existing(&mut marker, NODE_BYTES)?;
    let fence = existing_bytes(&root.join("laststore-layout-v1"), 128)?;
    if fence != laststore::disk_pack::home::LEGACY_FENCE_BYTES {
        return Err(refusal("restored node format fence differs"));
    }
    // Standalone native readers require this existing deletion lock. Requiring
    // it before the read prevents the normal missing-lock initialization path.
    let lifetime = open_existing(&root.join("disk-pack/LIFETIME_LOCK"), false)?;
    lifetime
        .try_lock_shared()
        .map_err(|_| refusal("restored file deletion is active"))?;
    let home = laststore::disk_pack::home::DiskHome::open(&root.join("disk-pack")).map_err(map)?;
    let mut reader = home
        .read(laststore::disk_run::home::ReadBudget {
            pages: 4096,
            blocks: 16384,
            bytes: 8 * 1024 * 1024,
            keys: 2,
        })
        .map_err(map)?;
    proof::verify(&mut reader, account_key)?;
    let guard = RestoredNodeGuard {
        root: root.to_path_buf(),
        directory,
        physical,
        marker,
        current: reader.current().clone(),
    };
    guard.check_binding()?;
    Ok(guard)
}

fn open_existing(path: &Path, directory: bool) -> StorageResult<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    if !directory {
        options.write(true);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let file = options.open(path)?;
    let metadata = file.metadata()?;
    if (directory && !metadata.is_dir()) || (!directory && !metadata.is_file()) {
        return Err(refusal("restored validation entry has the wrong kind"));
    }
    same_entry(&file, path)?;
    Ok(file)
}

fn same_entry(file: &File, path: &Path) -> StorageResult<()> {
    let held = file.metadata()?;
    let current = std::fs::symlink_metadata(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if held.dev() == current.dev() && held.ino() == current.ino() {
            return Ok(());
        }
    }
    Err(refusal(
        "restored validation entry changed or is unsupported",
    ))
}

pub(super) fn existing_bytes(path: &Path, maximum: u64) -> StorageResult<Vec<u8>> {
    let file = open_existing(path, false)?;
    if file.metadata()?.len() > maximum {
        return Err(refusal("restored validation file exceeds its bound"));
    }
    let mut bytes = Vec::new();
    file.take(maximum + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > maximum {
        return Err(refusal("restored validation file exceeds its bound"));
    }
    Ok(bytes)
}

fn verify_existing(file: &mut File, expected: &[u8]) -> StorageResult<()> {
    if file.metadata()?.len() != expected.len() as u64 {
        return Err(refusal("restored node marker differs"));
    }
    let mut bytes = vec![0; expected.len()];
    file.read_exact(&mut bytes)?;
    if bytes != expected {
        return Err(refusal("restored node marker differs"));
    }
    Ok(())
}

/// Validate the native root, check its logical format through the account key,
/// and create the normal node marker/fence. Retain the returned lock until publish.
/// The caller owns a fresh inactive wrapper; it separately certifies file coverage.
pub fn prepare_restored_node(root: &Path, account_key: &[u8; 32]) -> StorageResult<File> {
    if !std::fs::symlink_metadata(root)?.is_dir() {
        return Err(refusal("restore data root is not a directory"));
    }
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        if !matches!(
            entry.file_name().to_str(),
            Some(
                "disk-pack"
                    | NODE_MARKER
                    | "laststore-layout-v1"
                    | "native-file-recovery-v1"
                    | super::restore_safety::PENDING_FILE
            )
        ) {
            return Err(refusal("unexpected restore wrapper member"));
        }
        if entry.file_type()?.is_symlink() {
            return Err(refusal("restore wrapper contains a symlink"));
        }
    }
    let physical = root.join("disk-pack");
    let validated = validate_snapshot(&physical).map_err(map)?;
    let home = laststore::disk_pack::home::DiskHome::open(&physical).map_err(map)?;
    let mut reader = home
        .read(laststore::disk_run::home::ReadBudget {
            pages: 4096,
            blocks: 16384,
            bytes: 8 * 1024 * 1024,
            keys: 2,
        })
        .map_err(map)?;
    proof::verify(&mut reader, account_key)?;
    drop(reader);
    prepare_snapshot_directories(&physical, &validated.current).map_err(map)?;
    // The first logical open must check all counters before cleanup can run.
    super::restore_safety::mark_native(root)?;
    let marker = root.join(NODE_MARKER);
    regular_or_absent(&marker)?;
    let mut lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&marker)?;
    lock.try_lock()
        .map_err(|_| refusal("restore wrapper is already owned"))?;
    finish_file(&mut lock, NODE_BYTES)?;
    let fence = root.join("laststore-layout-v1");
    regular_or_absent(&fence)?;
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(fence)?;
    finish_file(&mut file, laststore::disk_pack::home::LEGACY_FENCE_BYTES)?;
    File::open(root)?.sync_all()?;
    Ok(lock)
}

fn regular_or_absent(path: &Path) -> StorageResult<()> {
    match std::fs::symlink_metadata(path) {
        Ok(meta) if !meta.is_file() => Err(refusal("restore marker is not a regular file")),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

pub(super) fn finish_file(file: &mut File, expected: &[u8]) -> StorageResult<()> {
    let length = file.metadata()?.len();
    if length > expected.len() as u64 {
        return Err(refusal("restore marker differs"));
    }
    let mut bytes = vec![0; length as usize];
    file.read_exact(&mut bytes)?;
    if !expected.starts_with(&bytes) {
        return Err(refusal("restore marker differs"));
    }
    if length != expected.len() as u64 {
        file.seek(SeekFrom::Start(0))?;
        file.write_all(expected)?;
    }
    // An earlier write can return an uncertain disk barrier with complete bytes.
    file.sync_all()?;
    Ok(())
}

fn refusal(detail: &str) -> StorageError {
    StorageError::InvalidOperation(detail.into())
}
