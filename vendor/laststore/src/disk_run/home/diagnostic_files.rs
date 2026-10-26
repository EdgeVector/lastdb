//! Read-only physical file totals for one stable published home.

use super::manifest::{Manifest, RunFile, CURRENT};
use super::{RunHome, RUNS, RUN_HOME_MARKER, RUN_SUFFIX};
use crate::{Error, Result};
use serde::Serialize;
use std::fs::{self, Metadata};
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard, TryLockError};
use std::time::{Duration, Instant};
use uuid::Uuid;

const MAX_FILES: usize = 131_072;
// Only UUIDs and Arc handles are retained; names and metadata are not collected.
const MAX_METADATA_BYTES: usize = 16 * 1024 * 1024;
const RETAINED_LIMIT: u64 = lastdb_storage_policy::BACKUP_RESOURCES.working_memory_bytes;

/// File lengths and allocated bytes; directory inode blocks are excluded.
/// Allocated bytes use the filesystem's 512-byte block count and can include
/// blocks shared with hardlinks or filesystem copies outside this home.
#[derive(Debug, Default, Serialize)]
pub struct FileDiagnostics {
    /// Length of the runs selected by CURRENT.
    pub published_file_bytes: u64,
    /// Allocated bytes of the runs selected by CURRENT.
    pub published_allocated_bytes: u64,
    /// Distinct runs selected by CURRENT.
    pub published_runs: u64,
    /// Length of retired runs that another read or root still holds.
    pub retired_held_file_bytes: u64,
    /// Allocated bytes of retired runs that another read or root still holds.
    pub retired_held_allocated_bytes: u64,
    /// Distinct retired runs that another read or root still holds.
    pub retired_held_runs: u64,
    /// Length of files with a native writer's temporary-file name.
    pub temporary_file_bytes: u64,
    /// Allocated bytes of files with a native writer's temporary-file name.
    pub temporary_allocated_bytes: u64,
    /// Files with a native writer's temporary-file name.
    pub temporary_files: u64,
    /// Length of regular files with no known reference or native role.
    pub unreferenced_file_bytes: u64,
    /// Allocated bytes of regular files with no known reference or native role.
    pub unreferenced_allocated_bytes: u64,
    /// Regular files with no known reference or native role.
    pub unreferenced_files: u64,
    /// Length of the descriptor, CURRENT, and owner lock.
    pub other_home_file_bytes: u64,
    /// Allocated bytes of the descriptor, CURRENT, and owner lock.
    pub other_home_allocated_bytes: u64,
    /// Descriptor, CURRENT, and owner-lock files.
    pub other_home_files: u64,
    /// Length of every regular file in the two native home directories.
    pub total_file_bytes: u64,
    /// Allocated bytes of every regular file in the two native directories.
    pub total_allocated_bytes: u64,
    /// Regular files in the two native home directories.
    pub total_files: u64,
}

#[derive(Clone, Copy)]
enum Kind {
    Published,
    Retired,
    Temporary,
    Unreferenced,
    Other,
}

struct SelectedFiles {
    manifest: Arc<Manifest>,
    published: Vec<Uuid>,
    // Keep the upgraded handles alive until both directory reads end.
    retired: Vec<Arc<RunFile>>,
}

/// Capture and classify files while no commit or merge can create or publish
/// a file. The lock order is commit, then merge; merge never takes commit.
pub(super) fn inspect(
    home: &RunHome,
    deadline: Instant,
) -> Result<(Arc<Manifest>, FileDiagnostics)> {
    let (_commit, _merge) = pair_until(home, deadline)?;
    home.refuse_if_poisoned()?;
    let selected = capture(home, deadline)?;
    let mut totals = FileDiagnostics::default();
    let mut visited = 0;
    enumerate(
        &home.root,
        true,
        &selected,
        &mut totals,
        &mut visited,
        deadline,
    )?;
    enumerate(
        &home.runs,
        false,
        &selected,
        &mut totals,
        &mut visited,
        deadline,
    )?;
    before_deadline(deadline)?;
    if totals.published_runs != selected.published.len() as u64
        || totals.retired_held_runs != selected.retired.len() as u64
        || totals.other_home_files != 3
    {
        return Err(invalid("required file is absent"));
    }
    Ok((Arc::clone(&selected.manifest), totals))
}

fn capture(home: &RunHome, deadline: Instant) -> Result<SelectedFiles> {
    let manifest = held_until(home, deadline)?;
    if manifest.retained_bytes() > RETAINED_LIMIT {
        return Err(Error::ReadBudgetExceeded);
    }
    let mut allocated = 0;
    let mut published = reserve(manifest.newest_first().count(), &mut allocated)?;
    published.extend(manifest.newest_first().map(|run| run.meta.run_id));
    published.sort_unstable();
    published.dedup();
    let held = lock_until(&home.retired, deadline)?;
    let mut retired = reserve(held.len(), &mut allocated)?;
    for weak in held.iter() {
        before_deadline(deadline)?;
        if let Some(run) = weak.upgrade() {
            if published.binary_search(&run.meta.run_id).is_err() {
                retired.push(run);
            }
        }
    }
    retired.sort_unstable_by_key(|run| run.meta.run_id);
    retired.dedup_by_key(|run| run.meta.run_id);
    let mut retained = manifest.retained_bytes().saturating_add(allocated as u64);
    for run in &retired {
        before_deadline(deadline)?;
        // Match CURRENT's retained-metadata charge, including the Arc header.
        retained = retained
            .saturating_add(
                (std::mem::size_of::<RunFile>() + 2 * std::mem::size_of::<usize>()) as u64,
            )
            .saturating_add(run.path.capacity() as u64)
            .saturating_add(run.meta.first_key.capacity() as u64)
            .saturating_add(run.meta.last_key.capacity() as u64);
    }
    if retained > RETAINED_LIMIT {
        return Err(Error::ReadBudgetExceeded);
    }
    Ok(SelectedFiles {
        manifest,
        published,
        retired,
    })
}

fn reserve<T>(count: usize, allocated: &mut usize) -> Result<Vec<T>> {
    if count > MAX_FILES {
        return Err(Error::ReadBudgetExceeded);
    }
    let mut values = Vec::new();
    values
        .try_reserve_exact(count)
        .map_err(|_| Error::ReadBudgetExceeded)?;
    *allocated = allocated
        .checked_add(
            values
                .capacity()
                .checked_mul(std::mem::size_of::<T>())
                .ok_or(Error::ReadBudgetExceeded)?,
        )
        .ok_or(Error::ReadBudgetExceeded)?;
    if *allocated > MAX_METADATA_BYTES {
        return Err(Error::ReadBudgetExceeded);
    }
    Ok(values)
}

fn enumerate(
    directory: &Path,
    root: bool,
    selected: &SelectedFiles,
    totals: &mut FileDiagnostics,
    visited: &mut usize,
    deadline: Instant,
) -> Result<()> {
    before_deadline(deadline)?;
    let directory_metadata =
        fs::symlink_metadata(directory).map_err(|_| invalid("directory metadata unavailable"))?;
    if !directory_metadata.is_dir() {
        return Err(invalid("native directory is not a directory"));
    }
    let entries = fs::read_dir(directory).map_err(|_| invalid("directory read failed"))?;
    for entry in entries {
        before_deadline(deadline)?;
        *visited = visited.checked_add(1).ok_or(Error::ReadBudgetExceeded)?;
        if *visited > MAX_FILES {
            return Err(Error::ReadBudgetExceeded);
        }
        let entry = entry.map_err(|_| invalid("directory entry unavailable"))?;
        let name = entry.file_name();
        let metadata =
            fs::symlink_metadata(entry.path()).map_err(|_| invalid("file metadata unavailable"))?;
        if root && name == RUNS && metadata.is_dir() {
            continue;
        }
        if !metadata.is_file() {
            return Err(invalid("unexpected nonregular file"));
        }
        totals.add(classify(name.to_str(), root, selected), &metadata)?;
    }
    Ok(())
}

fn classify(name: Option<&str>, root: bool, selected: &SelectedFiles) -> Kind {
    let Some(name) = name else {
        return Kind::Unreferenced;
    };
    if root {
        if [CURRENT, RUN_HOME_MARKER, "LOCK"].contains(&name) {
            return Kind::Other;
        }
        return if name
            .strip_prefix(".CURRENT.")
            .and_then(|name| name.strip_suffix(".tmp"))
            .is_some_and(|text| {
                text.parse::<u64>()
                    .is_ok_and(|number| text == number.to_string())
            }) {
            Kind::Temporary
        } else {
            Kind::Unreferenced
        };
    }
    if let Some(id) = canonical_uuid(name, "", RUN_SUFFIX) {
        if selected.published.binary_search(&id).is_ok() {
            return Kind::Published;
        }
        if selected
            .retired
            .binary_search_by_key(&id, |run| run.meta.run_id)
            .is_ok()
        {
            return Kind::Retired;
        }
    }
    if canonical_uuid(name, ".", ".tmp").is_some() {
        Kind::Temporary
    } else {
        Kind::Unreferenced
    }
}

fn canonical_uuid(name: &str, prefix: &str, suffix: &str) -> Option<Uuid> {
    let text = name.strip_prefix(prefix)?.strip_suffix(suffix)?;
    let id = Uuid::parse_str(text).ok()?;
    (text == id.hyphenated().to_string()).then_some(id)
}

impl FileDiagnostics {
    fn add(&mut self, kind: Kind, metadata: &Metadata) -> Result<()> {
        let bytes = metadata.len();
        let allocated = metadata
            .blocks()
            .checked_mul(512)
            .ok_or(Error::ReadBudgetExceeded)?;
        add(
            &mut self.total_file_bytes,
            &mut self.total_allocated_bytes,
            &mut self.total_files,
            bytes,
            allocated,
        )?;
        let (length, blocks, count) = match kind {
            Kind::Published => (
                &mut self.published_file_bytes,
                &mut self.published_allocated_bytes,
                &mut self.published_runs,
            ),
            Kind::Retired => (
                &mut self.retired_held_file_bytes,
                &mut self.retired_held_allocated_bytes,
                &mut self.retired_held_runs,
            ),
            Kind::Temporary => (
                &mut self.temporary_file_bytes,
                &mut self.temporary_allocated_bytes,
                &mut self.temporary_files,
            ),
            Kind::Unreferenced => (
                &mut self.unreferenced_file_bytes,
                &mut self.unreferenced_allocated_bytes,
                &mut self.unreferenced_files,
            ),
            Kind::Other => (
                &mut self.other_home_file_bytes,
                &mut self.other_home_allocated_bytes,
                &mut self.other_home_files,
            ),
        };
        add(length, blocks, count, bytes, allocated)
    }
}

fn add(
    length: &mut u64,
    blocks: &mut u64,
    count: &mut u64,
    bytes: u64,
    allocated: u64,
) -> Result<()> {
    *length = length.checked_add(bytes).ok_or(Error::ReadBudgetExceeded)?;
    *blocks = blocks
        .checked_add(allocated)
        .ok_or(Error::ReadBudgetExceeded)?;
    *count = count.checked_add(1).ok_or(Error::ReadBudgetExceeded)?;
    Ok(())
}

fn invalid(reason: &str) -> Error {
    Error::Config(format!("disk run diagnostics: {reason}"))
}

fn before_deadline(deadline: Instant) -> Result<()> {
    if Instant::now() >= deadline {
        return Err(invalid("deadline exceeded"));
    }
    Ok(())
}

fn pair_until(
    home: &RunHome,
    deadline: Instant,
) -> Result<(MutexGuard<'_, ()>, MutexGuard<'_, ()>)> {
    loop {
        before_deadline(deadline)?;
        let commit = match home.commit.try_lock() {
            Ok(held) => held,
            Err(TryLockError::Poisoned(error)) => error.into_inner(),
            Err(TryLockError::WouldBlock) => {
                wait_for_lock(deadline);
                continue;
            }
        };
        match home.merge.try_lock() {
            Ok(merge) => return Ok((commit, merge)),
            Err(TryLockError::Poisoned(error)) => return Ok((commit, error.into_inner())),
            Err(TryLockError::WouldBlock) => drop(commit),
        }
        wait_for_lock(deadline);
    }
}

fn lock_until<T>(lock: &Mutex<T>, deadline: Instant) -> Result<MutexGuard<'_, T>> {
    loop {
        before_deadline(deadline)?;
        match lock.try_lock() {
            Ok(held) => return Ok(held),
            Err(TryLockError::Poisoned(error)) => return Ok(error.into_inner()),
            Err(TryLockError::WouldBlock) => wait_for_lock(deadline),
        }
    }
}

fn held_until(home: &RunHome, deadline: Instant) -> Result<Arc<Manifest>> {
    loop {
        before_deadline(deadline)?;
        match home.current.try_read() {
            Ok(held) => return Ok(Arc::clone(&held)),
            Err(TryLockError::Poisoned(error)) => return Ok(Arc::clone(&error.into_inner())),
            Err(TryLockError::WouldBlock) => wait_for_lock(deadline),
        }
    }
}

fn wait_for_lock(deadline: Instant) {
    std::thread::sleep(
        Duration::from_millis(2).min(deadline.saturating_duration_since(Instant::now())),
    );
}
