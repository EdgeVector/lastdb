//! Preserve the two original native roots before a local home starts signed capture.

use super::accepted::{self, Log};
use super::snapshot::DatabaseSnapshot;
use crate::storage::disk_pack::{
    admit, cas_delete, map, physical::V2Home, run_node, CAS_DIR, RUN_DIR,
};
use crate::storage::error::{StorageError, StorageResult};
use laststore::disk_run::home::{RunHome, LOCK, RUNS};
use std::fs::{self, File, OpenOptions};
use std::io::{self, ErrorKind};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

const READY: &str = ".signed-initial-snapshot";
const STAGING: &str = ".signed-initial-snapshot-staging";
// Match the native cloud snapshot service's signed inventory bounds.
const MAX_FILES: usize = 2048;
const MAX_FILE_BYTES: u64 = 5 * 1024 * 1024 * 1024;

pub(in crate::storage) fn prepare_capture(physical: &V2Home, log: &Log, key: &[u8; 32]) {
    let status = log.status();
    if status.gap || !status.snapshot_required || status.head != 0 {
        return;
    }
    if let Err(error) = stage_and_pin(physical, log, key) {
        tracing::warn!(%error, "could not preserve the exact initial database checkpoint");
    }
}

fn stage_and_pin(physical: &V2Home, log: &Log, key: &[u8; 32]) -> StorageResult<()> {
    let data_root = data(physical)?;
    let ready = checkpoint_path(physical, READY)?;
    let frontier = physical.current_root().frontier();
    let opened = log.status().opened_frontier;
    let expected = if opened == 0 { frontier } else { opened };
    if let Some(saved) = open(physical, key)? {
        if saved.native_frontier == expected {
            File::open(ready.parent().ok_or_else(unavailable)?)?.sync_all()?;
            return log
                .pin_initial_frontier(saved.native_frontier)
                .map_err(accepted::storage);
        }
        if opened != 0 {
            return Err(unavailable());
        }
        release(physical)?;
    }
    if opened != 0 && opened != frontier {
        // An existing opened frontier cannot be reconstructed from newer files.
        return Err(unavailable());
    }
    cas_delete::require_clear(physical)?;
    if crate::storage::local_file_ref::has_stopped_references(data_root)? {
        return Err(StorageError::invalid(
            "snapshot cannot include files referenced outside the database",
        ));
    }
    let cut = DatabaseSnapshot::from_roots(physical, true, false)?;
    stage(data_root, &ready, &cut)?;
    log.pin_initial_frontier(frontier)
        .map_err(accepted::storage)
}

pub(super) fn open(physical: &V2Home, key: &[u8; 32]) -> StorageResult<Option<DatabaseSnapshot>> {
    let ready = checkpoint_path(physical, READY)?;
    if !private_directory(&ready)? {
        return Ok(None);
    }
    for root in [RUN_DIR, CAS_DIR] {
        let root = ready.join(root);
        if !private_directory(&root)? || !private_directory(&root.join(RUNS))? {
            return Err(unavailable());
        }
    }
    admit::admit_stopped_home(&ready, key)?;
    let main = RunHome::open(&ready.join(RUN_DIR), &run_node::home_key(key)).map_err(map)?;
    let files = RunHome::open(&ready.join(CAS_DIR), &run_node::cas_key(key)).map_err(map)?;
    if main.identity() != physical.home.identity() || files.identity() != physical.cas.identity() {
        return Err(unavailable());
    }
    let pinned = V2Home::new(std::sync::Arc::new(main), std::sync::Arc::new(files));
    DatabaseSnapshot::from_roots(&pinned, true, false).map(Some)
}

pub(super) fn release(physical: &V2Home) -> StorageResult<()> {
    let parent = checkpoint_parent(physical)?;
    let mut changed = false;
    for entry in [READY, STAGING] {
        let candidate = parent.join(entry);
        if private_directory(&candidate)? {
            fs::remove_dir_all(candidate)?;
            changed = true;
        }
    }
    if changed {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

fn stage(data: &Path, ready: &Path, cut: &DatabaseSnapshot) -> StorageResult<()> {
    let files = cut.files()?;
    if files.len() > MAX_FILES || files.iter().any(|(_, size)| *size > MAX_FILE_BYTES) {
        return Err(StorageError::invalid(
            "the first snapshot exceeds the signed file inventory limits",
        ));
    }
    let parent = ready.parent().ok_or_else(unavailable)?;
    let staging_bytes = files
        .iter()
        .filter(|(path, _)| !is_run_file(path))
        .map(|(_, size)| size)
        .sum();
    let _space = laststore::space::reserve_space(
        data,
        &laststore::space::SpacePolicy::default(),
        staging_bytes,
    )
    .map_err(map)?;
    let staging = parent.join(STAGING);
    if private_directory(&staging)? {
        fs::remove_dir_all(&staging)?;
    }
    mkdir(&staging)?;
    let result = stage_files(data, &staging, cut, files)
        .and_then(|()| fs::rename(&staging, ready).map_err(Into::into));
    if result.is_err() {
        let _ = fs::remove_dir_all(&staging);
    }
    result?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

fn stage_files(
    data: &Path,
    staging: &Path,
    cut: &DatabaseSnapshot,
    files: Vec<(PathBuf, u64)>,
) -> StorageResult<()> {
    for root in [RUN_DIR, CAS_DIR] {
        let root = staging.join(root);
        mkdir(&root)?;
        mkdir(&root.join(RUNS))?;
        private_file(&root.join(LOCK))?.sync_all()?;
    }
    for (relative, len) in files {
        let target = staging.join(&relative);
        let mut source = cut.open_file(&relative)?;
        let source_info = source.metadata()?;
        if !source_info.is_file() || source_info.len() != len {
            return Err(unavailable());
        }
        if is_run_file(&relative) {
            // The manifest pins these immutable runs while each hard link is made.
            fs::hard_link(data.join(&relative), &target)?;
            let target_info = fs::symlink_metadata(&target)?;
            if !target_info.is_file()
                || source_info.dev() != target_info.dev()
                || source_info.ino() != target_info.ino()
            {
                return Err(unavailable());
            }
        } else {
            let mut destination = private_file(&target)?;
            if io::copy(&mut source, &mut destination)? != len {
                return Err(unavailable());
            }
            destination.sync_all()?;
        }
    }
    for root in [RUN_DIR, CAS_DIR] {
        let root = staging.join(root);
        File::open(root.join(RUNS))?.sync_all()?;
        File::open(root)?.sync_all()?;
    }
    File::open(staging)?.sync_all()?;
    Ok(())
}

fn is_run_file(path: &Path) -> bool {
    [RUN_DIR, CAS_DIR].iter().any(|root| {
        path.strip_prefix(Path::new(*root).join(RUNS))
            .is_ok_and(|name| name.components().count() == 1)
    })
}

fn data(physical: &V2Home) -> StorageResult<&Path> {
    physical.path().parent().ok_or_else(unavailable)
}

fn checkpoint_path(physical: &V2Home, entry: &str) -> StorageResult<PathBuf> {
    Ok(checkpoint_parent(physical)?.join(entry))
}

fn checkpoint_parent(physical: &V2Home) -> StorageResult<&Path> {
    data(physical)?.parent().ok_or_else(unavailable)
}

fn private_directory(path: &Path) -> StorageResult<bool> {
    match fs::symlink_metadata(path) {
        Ok(info)
            if info.is_dir()
                && info.uid() == unsafe { libc::geteuid() }
                && info.mode() & 0o077 == 0 =>
        {
            Ok(true)
        }
        Ok(_) => Err(unavailable()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn mkdir(path: &Path) -> io::Result<()> {
    fs::DirBuilder::new().mode(0o700).create(path)
}

fn private_file(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(path)
}

fn unavailable() -> StorageError {
    StorageError::invalid("the exact initial database checkpoint is unavailable")
}
