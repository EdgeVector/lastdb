//! Private staged directories, descriptor-pinned exchange, and durable receipts.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

use rustix::fs::{self as rfs, FlockOperation, RenameFlags};
use serde::{Deserialize, Serialize};
use zeroize::Zeroizing;

use crate::host::{Host, IDENTITY_KEY_FILE};

use crate::log_sync::disk;

const LOCK: &str = ".native-sync-recovery.lock";
const STAGE_PREFIX: &str = ".native-sync-stage-";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct Inode(u64, u64);

impl Inode {
    pub(super) fn same_device(self, other: Self) -> bool {
        self.0 == other.0
    }
}

pub(in crate::log_sync::native_reset) struct Stage {
    pub(super) path: PathBuf,
    pub(super) inode: Inode,
    pub(super) keep: bool,
}

impl Stage {
    pub(super) fn create(host: &Host) -> Result<Self, String> {
        let path = host
            .home
            .join(format!("{STAGE_PREFIX}{}", uuid::Uuid::new_v4()));
        fs::DirBuilder::new()
            .mode(0o700)
            .create(&path)
            .map_err(io_error)?;
        let stage = Self {
            inode: inode(&path).map_err(io_error)?,
            path,
            keep: false,
        };
        write_new(
            &stage.path.join(IDENTITY_KEY_FILE),
            Zeroizing::new(host.keypair.secret_key_bytes()).as_ref(),
        )?;
        sync_dir(&stage.path)?;
        Ok(stage)
    }

    pub(in crate::log_sync::native_reset) fn path(&self) -> &Path {
        &self.path
    }

    pub(super) fn remove_identity(&self) -> Result<(), String> {
        fs::remove_file(self.path.join(IDENTITY_KEY_FILE)).map_err(io_error)
    }
}

impl Drop for Stage {
    fn drop(&mut self) {
        if !self.keep && inode(&self.path).is_ok_and(|actual| actual == self.inode) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}

pub(super) fn seed(home: &Path) -> Result<Zeroizing<[u8; 32]>, String> {
    let bytes = Zeroizing::new(
        disk::read_limited(&home.join(IDENTITY_KEY_FILE), 32)
            .map_err(io_error)?
            .ok_or("native recovery identity is missing")?,
    );
    let value = bytes
        .as_slice()
        .try_into()
        .map_err(|_| "invalid native recovery identity")?;
    Ok(Zeroizing::new(value))
}

pub(super) fn inode(path: &Path) -> io::Result<Inode> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "native recovery path is not a directory",
        ));
    }
    Ok(Inode(metadata.dev(), metadata.ino()))
}

pub(super) fn file_inode(file: &File) -> Result<Inode, String> {
    let m = file.metadata().map_err(io_error)?;
    Ok(Inode(m.dev(), m.ino()))
}

pub(super) fn checked_child(parent: &File, name: &str) -> Result<Inode, String> {
    let flags = rfs::OFlags::RDONLY | rfs::OFlags::DIRECTORY | rfs::OFlags::NOFOLLOW;
    let child = rfs::openat(parent, name, flags, rfs::Mode::empty())
        .map_err(|error| io_error(error.into()))?;
    file_inode(&File::from(child))
}

pub(super) fn valid_stage_name(name: &str) -> Result<&str, String> {
    let id = name
        .strip_prefix(STAGE_PREFIX)
        .ok_or("invalid native recovery stage name")?;
    if uuid::Uuid::parse_str(id).is_ok_and(|uuid| uuid.to_string() == id) {
        Ok(name)
    } else {
        Err("invalid native recovery stage name".into())
    }
}

pub(super) fn lock_home(home: &Path) -> Result<File, String> {
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(home.join(LOCK))
        .map_err(io_error)?;
    if !crate::cloud::is_private_file(&lock.metadata().map_err(io_error)?) {
        return Err("native recovery lock is not a private file".into());
    }
    rfs::flock(&lock, FlockOperation::NonBlockingLockExclusive)
        .map_err(|_| "native recovery file is busy")?;
    Ok(lock)
}

pub(super) fn exchange(home: &File, stage: &File) -> Result<(), String> {
    rfs::renameat_with(home, "data", stage, "data", RenameFlags::EXCHANGE)
        .map_err(|error| io_error(error.into()))
}

fn write_new(path: &Path, contents: &[u8]) -> Result<(), String> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .map_err(io_error)?;
    file.write_all(contents)
        .and_then(|()| file.sync_all())
        .map_err(io_error)
}

pub(super) fn durable_file(path: &Path, contents: &[u8]) -> Result<(), String> {
    let parent = path.parent().ok_or("native recovery file has no parent")?;
    let name = format!(".native-sync-{}.tmp", uuid::Uuid::new_v4());
    let pending = parent.join(name);
    let result = write_new(&pending, contents)
        .and_then(|()| fs::rename(&pending, path).map_err(io_error))
        .and_then(|()| sync_dir(parent));
    if result.is_err() {
        let _ = fs::remove_file(&pending);
    }
    result
}

pub(super) fn directory(path: &Path) -> Result<File, String> {
    OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_DIRECTORY)
        .open(path)
        .map_err(io_error)
}

pub(super) fn sync_dir(path: &Path) -> Result<(), String> {
    directory(path)?.sync_all().map_err(io_error)
}

pub(super) fn io_error(error: io::Error) -> String {
    format!("native recovery filesystem: {error}")
}
