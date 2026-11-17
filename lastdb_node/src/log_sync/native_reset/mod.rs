//! Stage a verified personal recovery without changing the open database.

mod install;

pub use install::install_prepared_native_recovery;

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use fold_db::crypto::E2eKeys;
use fold_db::storage::disk_backup::{accepted::UploadIdentity, stopped_sync::StoppedSync};
use rustix::fs::{self as rfs, FlockOperation, RenameFlags};
use zeroize::Zeroizing;

use crate::host::{Host, IDENTITY_KEY_FILE};
use crate::offline_home::DirInode as Inode;

use super::cloud::CloudLog;
use super::run::Ready;
use super::wire::NativeCut;
use super::{disk, owner_recovery, snapshot};

const LOCK: &str = ".native-sync-recovery.lock";
const STAGE_PREFIX: &str = ".native-sync-stage-";

pub(super) fn supports(host: &Host, ready: &Ready) -> bool {
    ready.publisher.is_none()
        && matches!(host.owned.is_private_id(&ready.database_id), Ok(true))
        && matches!(
            host.owned.registered_store(&ready.database_id),
            Ok(crate::host::owned::ResolvedStore::Owner)
        )
}

pub(super) async fn prepare(
    host: &Host,
    log: &CloudLog,
    after: u64,
    cut: &NativeCut,
) -> Result<(), String> {
    let origin = install::freeze(host, log, after).await?;
    let stage = Stage::create(host)?;
    let base = snapshot::restore_cut(stage.path(), log, cut).await?;
    let staged = Arc::new(Host::boot_for_snapshot_restore(stage.path()).await?);
    let recovery = async {
        let through = owner_recovery::recover_cut(&staged, log, base).await?;
        install::bind_stage_upload(&staged, &origin)?;
        Ok::<_, String>(through)
    }
    .await;
    let shutdown = staged.db.shutdown().await;
    drop(staged);
    let through = recovery?;
    shutdown.map_err(|error| error.to_string())?;
    install::prepare(host, log, origin, stage, cut, through).await
}

struct Stage {
    path: PathBuf,
    inode: Inode,
    keep: bool,
}

impl Stage {
    fn create(host: &Host) -> Result<Self, String> {
        let path = host
            .home
            .join(format!("{STAGE_PREFIX}{}", uuid::Uuid::new_v4()));
        fs::DirBuilder::new()
            .mode(0o700)
            .create(&path)
            .map_err(|error| io_error(&error))?;
        let stage = Self {
            inode: inode(&path)?,
            path,
            keep: false,
        };
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(stage.path.join(IDENTITY_KEY_FILE))
            .map_err(|error| io_error(&error))?;
        let secret = Zeroizing::new(host.keypair.secret_key_bytes());
        file.write_all(secret.as_ref())
            .and_then(|()| file.sync_all())
            .map_err(|error| io_error(&error))?;
        sync_dir(&stage.path)?;
        Ok(stage)
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn remove_identity(&self) -> Result<(), String> {
        fs::remove_file(self.path.join(IDENTITY_KEY_FILE)).map_err(|error| io_error(&error))
    }
}

impl Drop for Stage {
    fn drop(&mut self) {
        if !self.keep {
            self.inode.remove_dir_if_owned(&self.path);
        }
    }
}

fn seed(home: &Path) -> Result<Zeroizing<[u8; 32]>, String> {
    crate::offline_home::read_home_identity_seed(home)?
        .ok_or_else(|| "native recovery identity is missing".into())
}

fn inode(path: &Path) -> Result<Inode, String> {
    Inode::of_dir(path).map_err(|error| io_error(&error))
}

fn file_inode(file: &File) -> Result<Inode, String> {
    Inode::of_file(file).map_err(|error| io_error(&error))
}

fn checked_child(parent: &File, name: &str) -> Result<Inode, String> {
    let flags = rfs::OFlags::RDONLY | rfs::OFlags::DIRECTORY | rfs::OFlags::NOFOLLOW;
    let child =
        rfs::openat(parent, name, flags, rfs::Mode::empty()).map_err(|error| io_error(&error))?;
    file_inode(&File::from(child))
}

fn valid_stage_name(name: &str) -> Result<&str, String> {
    let id = name
        .strip_prefix(STAGE_PREFIX)
        .ok_or("invalid native recovery stage name")?;
    if uuid::Uuid::parse_str(id).is_ok_and(|uuid| uuid.to_string() == id) {
        Ok(name)
    } else {
        Err("invalid native recovery stage name".into())
    }
}

fn lock_home(home: &Path) -> Result<File, String> {
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(home.join(LOCK))
        .map_err(|error| io_error(&error))?;
    if !crate::offline_home::is_private_file(&lock.metadata().map_err(|error| io_error(&error))?) {
        return Err("native recovery lock is not a private file".into());
    }
    rfs::flock(&lock, FlockOperation::NonBlockingLockExclusive)
        .map_err(|_| "native recovery file is busy")?;
    Ok(lock)
}

fn exchange(home: &File, stage: &File) -> Result<(), String> {
    rfs::renameat_with(home, "data", stage, "data", RenameFlags::EXCHANGE)
        .map_err(|error| io_error(&error))
}

fn durable_file(path: &Path, contents: &[u8]) -> Result<(), String> {
    disk::write_private(path, contents).map_err(|error| io_error(&error))
}

fn directory(path: &Path) -> Result<File, String> {
    OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_DIRECTORY)
        .open(path)
        .map_err(|error| io_error(&error))
}

fn sync_dir(path: &Path) -> Result<(), String> {
    directory(path)?
        .sync_all()
        .map_err(|error| io_error(&error))
}

fn io_error(error: &impl std::fmt::Display) -> String {
    format!("native recovery filesystem: {error}")
}

fn open_stopped(
    data: &Path,
    seed: &[u8; 32],
    identity: &UploadIdentity,
) -> Result<StoppedSync, String> {
    let key =
        E2eKeys::from_ed25519_seed(seed).map_err(|_| "could not derive the native recovery key")?;
    StoppedSync::open(data, &key.encryption_key(), identity).map_err(|error| storage_error(&error))
}

fn storage_error(error: &fold_db::storage::error::StorageError) -> String {
    format!("native recovery cannot prove a stopped database: {error}")
}
