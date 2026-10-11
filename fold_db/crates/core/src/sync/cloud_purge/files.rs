//! Independent, exact-prefix source files and durable sidecar writes.

pub(super) use super::file_verification::verify_file;
use crate::sync::error::{SyncError, SyncResult};
use sha2::{Digest, Sha256};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

pub(super) fn owner_directory(path: &Path) -> SyncResult<()> {
    std::fs::create_dir_all(path)?;
    if !std::fs::symlink_metadata(path)?.file_type().is_dir() {
        return Err(SyncError::Storage(
            "owner directory must be a real directory".into(),
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

fn new_owner_file(path: &Path) -> SyncResult<File> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    Ok(options.open(path)?)
}

pub(super) fn atomic_json<T: serde::Serialize>(path: &Path, value: &T) -> SyncResult<()> {
    let bytes =
        super::inventory::bounded_json(value, super::inventory::INVENTORY_METADATA_MAX_BYTES)?;
    atomic_bytes(path, &bytes)
}

pub(super) fn atomic_bytes(path: &Path, bytes: &[u8]) -> SyncResult<()> {
    let parent = path
        .parent()
        .ok_or_else(|| SyncError::Storage("checkpoint parent absent".into()))?;
    owner_directory(parent)?;
    let temporary = parent.join(format!(".purge-{}.tmp", uuid::Uuid::new_v4()));
    let mut file = new_owner_file(&temporary)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    std::fs::rename(&temporary, path)?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

pub(super) fn retained_path(root: &Path, sha256: &str) -> SyncResult<PathBuf> {
    if !crate::hex::is_lower_hex_sha256(sha256) {
        return Err(SyncError::Storage("invalid source file digest".into()));
    }
    Ok(root.join("objects").join(sha256))
}

pub(super) fn copy_prefix(source: &Path, into: &Path, bytes: u64, sha: &str) -> SyncResult<()> {
    if into.exists() {
        return verify_file(into, bytes, sha);
    }
    let mut source = File::open(source)?.take(bytes);
    let temporary = into.with_file_name(format!(".source-copy-{}", uuid::Uuid::new_v4()));
    let mut destination = new_owner_file(&temporary)?;
    let mut hasher = Sha256::new();
    let mut count = 0u64;
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = source.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        destination.write_all(&buffer[..read])?;
        hasher.update(&buffer[..read]);
        count = count.saturating_add(read as u64);
    }
    if count != bytes || crate::hex::hex_lower(hasher.finalize()) != sha {
        return Err(SyncError::Crypto(
            "retained source prefix differs from its exact cut".into(),
        ));
    }
    destination.sync_all()?;
    drop(destination);
    std::fs::rename(temporary, into)?;
    File::open(
        into.parent()
            .ok_or_else(|| SyncError::Storage("source parent absent".into()))?,
    )?
    .sync_all()?;
    Ok(())
}

#[cfg(unix)]
pub(super) fn require_capacity(directory: &Path, bytes: u64) -> SyncResult<()> {
    use std::os::unix::ffi::OsStrExt;
    let path = std::ffi::CString::new(directory.as_os_str().as_bytes())
        .map_err(|_| SyncError::Storage("invalid source directory".into()))?;
    let mut stats = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // statvfs initializes stats on success and retains no pointer.
    if unsafe { libc::statvfs(path.as_ptr(), stats.as_mut_ptr()) } != 0 {
        return Err(SyncError::Io(std::io::Error::last_os_error()));
    }
    let stats = unsafe { stats.assume_init() };
    #[cfg(target_os = "macos")]
    let available = u64::from(stats.f_bavail).saturating_mul(stats.f_frsize);
    #[cfg(not(target_os = "macos"))]
    let available = stats.f_bavail.saturating_mul(stats.f_frsize);
    let required = bytes
        .saturating_add(bytes / 5)
        .saturating_add(64 * 1024 * 1024);
    if available < required {
        return Err(SyncError::Storage(
            "insufficient space for the independent source and subsequent writes".into(),
        ));
    }
    Ok(())
}

#[cfg(not(unix))]
pub(super) fn require_capacity(_: &Path, _: u64) -> SyncResult<()> {
    Err(SyncError::Storage(
        "source capacity proof unavailable on this platform".into(),
    ))
}
