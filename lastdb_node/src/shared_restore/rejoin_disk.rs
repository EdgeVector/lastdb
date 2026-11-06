//! Preserve the returning device's private run and exchange the verified data roots.

use crate::byte_cloud_restore::{check_capacity, local, publish, RestoreError, Result};
use fold_db::storage::disk_pack::{PackRuntime, RUN_DIR};
use laststore::disk_run::home::{CapturedRunHome, LOCK, RUNS};
use rustix::fs::{self as unix, FlockOperation, Mode, OFlags, RenameFlags};
use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;

const PRIVATE: &str = "disk-private";

/// Copy only the descriptor, CURRENT, and runs selected by the source owner.
/// The unpublished staging home already contains an empty private run.
pub(super) async fn stage_private_run(
    runtime: &Arc<PackRuntime>,
    staged_data: &Path,
) -> Result<()> {
    local::directory(staged_data)?;
    local::directory(&staged_data.join(PRIVATE))?;
    let stage = staged_data
        .parent()
        .ok_or(RestoreError::Invalid("rejoin staged home"))?;
    local::directory(stage)?;
    let hold = stage.join(format!(".private-run-hold-{}", uuid::Uuid::new_v4()));
    let capture = runtime
        .capture_rejoin_private(&hold)
        .await
        .map_err(RestoreError::native_proof);
    let result = capture.and_then(|capture| install_private_run(&hold, &capture, staged_data));
    let release = CapturedRunHome::release_saved(&hold)
        .map_err(|_| RestoreError::Invalid("rejoin private capture release"));
    result.and(release)
}

fn install_private_run(hold: &Path, capture: &CapturedRunHome, staged_data: &Path) -> Result<()> {
    if capture.files.len() > cloud_backup_protocol::limits::NATIVE_FILES_MAX {
        return Err(RestoreError::Limit("rejoin private run files"));
    }
    let bytes = capture.files.iter().try_fold(0u64, |total, file| {
        total
            .checked_add(file.bytes)
            .filter(|total| *total <= lastdb_storage_policy::BACKUP_TOTAL_STORED_BYTES)
            .ok_or(RestoreError::Limit("rejoin private run bytes"))
    })?;
    check_capacity(staged_data, bytes)?;
    let candidate = staged_data.join(".disk-private-rejoin");
    let mut builder = fs::DirBuilder::new();
    builder.mode(0o700).create(&candidate)?;
    builder.create(candidate.join(RUNS))?;
    for member in &capture.files {
        copy_member(
            &hold.join(&member.held),
            &candidate.join(&member.path),
            member.bytes,
        )?;
    }
    local::owner_file(&candidate.join(LOCK), true)?.sync_all()?;
    File::open(candidate.join(RUNS))?.sync_all()?;
    File::open(&candidate)?.sync_all()?;

    // This private directory belongs to the unpublished restore, so an error
    // during installation leaves only disposable staging data.
    let private = staged_data.join(PRIVATE);
    fs::remove_dir_all(&private)?;
    fs::rename(candidate, private)?;
    File::open(staged_data)?.sync_all()?;
    Ok(())
}

fn copy_member(source: &Path, target: &Path, bytes: u64) -> Result<()> {
    let mut source = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(source)?;
    let info = source.metadata()?;
    if !info.is_file() || info.len() != bytes {
        return Err(RestoreError::Invalid("rejoin private run member"));
    }
    let mut output = local::owner_file(target, true)?;
    if io::copy(&mut source, &mut output)? != bytes {
        return Err(RestoreError::Invalid("rejoin private run member length"));
    }
    output.sync_all()?;
    Ok(())
}

/// Exchange data only. The caller has stopped source I/O and retains its
/// original native owner locks. Keep the old data under the private workspace.
pub(super) fn activate(
    source_data: &Path,
    staged_data: &Path,
    source_owner: &File,
) -> Result<PathBuf> {
    const DATA: &str = "data";
    let source_home = data_parent(source_data, DATA)?;
    let stage = data_parent(staged_data, DATA)?;
    if stage == source_home || !stage.starts_with(source_home) {
        return Err(RestoreError::Invalid("rejoin workspace placement"));
    }
    crate::offline_home::refuse_primary(source_home)
        .map_err(|_| RestoreError::Invalid("rejoin source home"))?;
    local::directory(source_home)?;
    local::directory(stage)?;
    let source = open_directory(unix::CWD, source_home)?;
    let staging = open_directory(unix::CWD, stage)?;
    let original = open_directory(&source, DATA)?;
    let replacement = open_directory(&staging, DATA)?;
    if original.metadata()?.dev() != replacement.metadata()?.dev() {
        return Err(RestoreError::Invalid("rejoin data filesystems differ"));
    }
    let old_lock = open_main_lock(&original)?;
    if !same_inode(&old_lock, source_owner)? {
        return Err(RestoreError::Invalid("rejoin source owner changed"));
    }
    let new_lock = open_main_lock(&replacement)?;
    match unix::flock(&old_lock, FlockOperation::NonBlockingLockExclusive) {
        Err(rustix::io::Errno::WOULDBLOCK) => {}
        Ok(()) => return Err(RestoreError::Invalid("rejoin source is not owned")),
        Err(error) => return Err(io::Error::from(error).into()),
    }
    unix::flock(&new_lock, FlockOperation::NonBlockingLockExclusive).map_err(io::Error::from)?;
    publish::sync_owned_tree(staged_data, 0)?;
    staging.sync_all()?;
    require_entry(&source, DATA, &original)?;
    require_entry(&staging, DATA, &replacement)?;
    unix::renameat_with(&source, DATA, &staging, DATA, RenameFlags::EXCHANGE)
        .map_err(io::Error::from)?;
    let durable = (|| {
        require_entry(&source, DATA, &replacement)?;
        require_entry(&staging, DATA, &original)?;
        source.sync_all()?;
        staging.sync_all()?;
        Ok(())
    })();
    durable.map_err(|cause| RestoreError::SharedWorkspace {
        path: staged_data.to_path_buf(),
        cause: Box::new(cause),
    })?;
    Ok(staged_data.to_path_buf())
}

fn data_parent<'a>(data: &'a Path, name: &str) -> Result<&'a Path> {
    let parent = data
        .parent()
        .ok_or(RestoreError::Invalid("rejoin data parent"))?;
    if data.file_name() != Some(name.as_ref())
        || !parent.is_absolute()
        || parent.canonicalize()? != parent
    {
        return Err(RestoreError::Invalid("rejoin data location"));
    }
    Ok(parent)
}

fn open_directory(parent: impl std::os::fd::AsFd, name: impl rustix::path::Arg) -> Result<File> {
    let file = File::from(
        unix::openat(
            parent,
            name,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(io::Error::from)?,
    );
    let info = file.metadata()?;
    if info.uid() != rustix::process::geteuid().as_raw() || info.mode() & 0o022 != 0 {
        return Err(RestoreError::Invalid("rejoin owned directory"));
    }
    Ok(file)
}

fn open_main_lock(data: &File) -> Result<File> {
    let main = open_directory(data, RUN_DIR)?;
    let lock = File::from(
        unix::openat(
            &main,
            LOCK,
            OFlags::RDWR | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(io::Error::from)?,
    );
    let info = lock.metadata()?;
    if !info.is_file()
        || info.uid() != rustix::process::geteuid().as_raw()
        || info.mode() & 0o077 != 0
        || info.len() != 0
        || info.nlink() != 1
    {
        return Err(RestoreError::Invalid("rejoin owner lock"));
    }
    Ok(lock)
}

fn require_entry(parent: &File, name: &str, expected: &File) -> Result<()> {
    if !same_inode(&open_directory(parent, name)?, expected)? {
        return Err(RestoreError::Invalid("rejoin data entry changed"));
    }
    Ok(())
}

fn same_inode(first: &File, second: &File) -> Result<bool> {
    let first = first.metadata()?;
    let second = second.metadata()?;
    Ok((first.dev(), first.ino()) == (second.dev(), second.ino()))
}
