use super::{MaintenanceReceipt, SpacePolicy};
use crate::disk_pack::dependency::Coordinate;
use crate::disk_pack::private_files::DeletionEpoch;
use crate::disk_pack::write_lock::WriteLock;
use crate::disk_pack::{artifact, corrupt, generation, lifetime, Current, FEATURE_FILE_LIFETIME};
use crate::{durability, Result};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::path::{Path, PathBuf};

mod concurrent;
mod inventory;
mod retained;
mod unfinished;
use inventory::generations;
use retained::live_parts;
use unfinished::{at_generation, remove_unfinished};

const MAX_RETAINED_PARTS: usize = 262_144;

mod orphans;
mod scan;
pub(in crate::disk_pack) use orphans::{sweep_orphans_at, sweep_orphans_locked};
use scan::scan_old;

/// Directories whose entries changed. One barrier per directory after a whole
/// phase replaces one barrier per removed file. A phase ends with `flush`, and
/// the next phase starts only after it, so a receipt never leaves the live set
/// before the removals it lists are durable.
#[derive(Default)]
struct Dirty(BTreeSet<PathBuf>);

impl Dirty {
    fn mark(&mut self, dir: &Path) {
        self.0.insert(dir.to_path_buf());
    }

    fn flush(&mut self) -> Result<()> {
        for dir in std::mem::take(&mut self.0) {
            match File::open(&dir) {
                Ok(handle) => durability::sync_directory(&handle)?,
                // A directory removed in the same phase has nothing left to sync.
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }
}

/// Reclaim only exact old-receipt paths, after a complete plan and all safety checks.
/// A failure leaves CURRENT intact. Receipt files are removed after their data.
pub(in crate::disk_pack) fn reclaim_locked(
    lock: &WriteLock,
    policy: &SpacePolicy,
) -> Result<MaintenanceReceipt> {
    reclaim_at(
        lock,
        policy,
        artifact::current(lock.root())?,
        false,
        &lock.files.epoch()?,
    )
}

pub(in crate::disk_pack) fn reclaim_at(
    lock: &WriteLock,
    policy: &SpacePolicy,
    current: Current,
    concurrent: bool,
    epoch: &DeletionEpoch,
) -> Result<MaintenanceReceipt> {
    let root = lock.root();
    let mut receipt = MaintenanceReceipt {
        generation: current.generation,
        ..Default::default()
    };
    if current.required_features & FEATURE_FILE_LIFETIME == 0 || !epoch.valid()? {
        receipt.reclaim_deferred = true;
        return Ok(receipt);
    }
    // An active local read does not defer reclaim: its generation stays retained.
    let local = if concurrent {
        lock.retire_before(&current)?
    } else {
        lock.exclude_local_reads(&current)?
    };
    let Some(local) = local else {
        receipt.reclaim_deferred = true;
        return Ok(receipt);
    };
    let Some(_exclusive) = lifetime::exclusive(root)? else {
        receipt.reclaim_deferred = true;
        return Ok(receipt);
    };
    validate_roots(root)?;
    let mut dirty = Dirty::default();
    cleanup_retired(root, policy.max_files, &mut receipt, &mut dirty, epoch)?;
    dirty.flush()?;
    let references = lifetime::list_unlocked(root)?;
    let mut retained = BTreeSet::from([current.generation]);
    retained.extend(references.iter().map(|item| item.generation));
    retained.extend(local.held().iter().copied());
    let mut generations = generations(
        root,
        policy.max_files,
        &retained,
        concurrent.then_some(current.generation),
        epoch,
    )?;
    generations.sort_unstable();
    if !epoch.valid()? {
        receipt.reclaim_deferred = true;
        return Ok(receipt);
    }
    let cutoff = artifact::unix_ms().saturating_sub(policy.grace_ms);
    // Grace protects each young generation's whole closure, not only young files.
    // With no grace nothing is young, so the scan is skipped.
    for number in generations.iter().filter(|_| policy.grace_ms > 0) {
        let held = at_generation(generation::load_current(root, *number), root, *number)?;
        if held.published_ms > cutoff {
            retained.insert(*number);
        }
    }
    let live = live_parts(lock, &retained, references.len(), local.held().len())?;
    let scan = scan_old(root, generations, &retained, &live, policy, cutoff)?;
    receipt.reclaim_remaining = scan.remaining;
    let (old, candidates) = (scan.old, scan.candidates);
    let mut removals = Vec::new();
    let mut skipped = Vec::new();
    for (coordinate, path) in candidates {
        match removable(root, &current, &coordinate, &path, cutoff)? {
            Some(bytes) => removals.push((path, bytes)),
            None => skipped.push(path),
        }
    }
    // Every retained part and every candidate is validated before the first deletion.
    let plan = Obsolete {
        removals,
        skipped,
        old,
    };
    plan.apply(lock, policy, &live, &mut receipt, &mut dirty, epoch)?;
    if !epoch.valid()? {
        receipt.reclaim_deferred = true;
        receipt.reclaim_remaining = true;
    }
    Ok(receipt)
}

/// A validated plan: exact part files to remove, then the receipts that list them.
struct Obsolete {
    removals: Vec<(PathBuf, u64)>,
    /// Candidates that were not removed: already gone, or too young to remove.
    skipped: Vec<PathBuf>,
    old: Vec<Current>,
}

impl Obsolete {
    fn apply(
        mut self,
        lock: &WriteLock,
        policy: &SpacePolicy,
        live: &BTreeSet<Coordinate>,
        receipt: &mut MaintenanceReceipt,
        dirty: &mut Dirty,
        epoch: &DeletionEpoch,
    ) -> Result<()> {
        let root = lock.root();
        for (path, bytes) in self.removals {
            let Some(_deletion) = epoch.delete(&path, false)? else {
                self.skipped.push(path);
                receipt.reclaim_remaining = true;
                if !epoch.valid()? {
                    break;
                }
                continue;
            };
            std::fs::remove_file(&path)?;
            dirty.mark(path.parent().expect("part parent"));
            receipt.files_removed += 1;
            receipt.bytes_removed += bytes;
            remove_empty_parents(lock, &path, dirty, epoch)?;
        }
        // Every removal is durable before the first receipt that lists it retires.
        dirty.flush()?;
        if !epoch.valid()? {
            return Ok(());
        }
        // Each old receipt lists only live parts and candidates. When no candidate
        // remains on disk, every old receipt may retire without another read of it.
        let blocked = self.skipped.iter().any(|path| path.exists());
        for held in self.old {
            if blocked {
                let generation =
                    at_generation(generation::read_receipt(root, &held), root, held.generation)?;
                if generation.coordinates.iter().any(|coordinate| {
                    !live.contains(coordinate)
                        && coordinate.path(root).map_or(true, |path| path.exists())
                }) {
                    continue;
                }
            }
            retire_receipt(root, held.generation, dirty, epoch)?;
        }
        dirty.flush()?;
        cleanup_retired(root, policy.max_files, receipt, dirty, epoch)?;
        dirty.flush()
    }
}

fn validate_roots(root: &Path) -> Result<()> {
    for name in [
        "generations",
        "references",
        "retired-generations",
        "directory",
        "locator",
        "packs",
    ] {
        match std::fs::symlink_metadata(root.join(name)) {
            Ok(metadata) if metadata.is_dir() => {}
            Ok(_) => return Err(corrupt("reclaim root namespace is not a regular directory")),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn removable(
    root: &Path,
    current: &Current,
    coordinate: &Coordinate,
    path: &Path,
    cutoff: u64,
) -> Result<Option<u64>> {
    validate_parents(root, path)?;
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if !metadata.is_file() || !path.starts_with(root) {
        return Err(corrupt("reclaim candidate is not an exact regular part"));
    }
    let modified = metadata
        .modified()?
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| corrupt("reclaim candidate has invalid time"))?
        .as_millis();
    if modified > u128::from(cutoff) {
        return Ok(None);
    }
    // Header validation rejects a substituted file at a former exact coordinate.
    coordinate.capture(root, current)?;
    Ok(Some(metadata.len()))
}

fn validate_parents(root: &Path, path: &Path) -> Result<()> {
    let relative = path
        .strip_prefix(root)
        .map_err(|_| corrupt("reclaim path escaped root"))?;
    let mut parent = root.to_path_buf();
    for component in relative.parent().expect("part has parent").components() {
        if !matches!(component, std::path::Component::Normal(_)) {
            return Err(corrupt("unsupported reclaim path component"));
        }
        parent.push(component);
        match std::fs::symlink_metadata(&parent) {
            Ok(metadata) if metadata.is_dir() => {}
            Ok(_) => return Err(corrupt("reclaim refuses a symlink or non-directory parent")),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn retire_receipt(
    root: &Path,
    number: u64,
    dirty: &mut Dirty,
    epoch: &DeletionEpoch,
) -> Result<()> {
    let path = root.join("generations").join(number.to_string());
    let parent = root.join("retired-generations");
    if !parent.try_exists()? {
        crate::disk_pack::page_write::directory(&parent)?;
    }
    let target = parent.join(number.to_string());
    let Some(_deletion) = epoch.delete(&path, true)? else {
        return Ok(());
    };
    // Leave the complete receipt readable until it atomically leaves the live set.
    // A restart can finish deletion in this exclusively owned retirement namespace.
    {
        let _inventory = super::space::inventory_guard()?;
        std::fs::rename(&path, target)?;
    }
    dirty.mark(&root.join("generations"));
    dirty.mark(&parent);
    Ok(())
}

fn cleanup_retired(
    root: &Path,
    max_files: usize,
    receipt: &mut MaintenanceReceipt,
    dirty: &mut Dirty,
    epoch: &DeletionEpoch,
) -> Result<()> {
    let parent = root.join("retired-generations");
    if !parent.try_exists()? {
        return Ok(());
    }
    for (index, entry) in std::fs::read_dir(&parent)?.enumerate() {
        if index >= max_files {
            return Err(crate::disk_pack::resource());
        }
        let entry = entry?;
        let Some(_deletion) = epoch.delete(&entry.path(), true)? else {
            break;
        };
        if !entry.file_type()?.is_dir()
            || entry
                .file_name()
                .to_str()
                .and_then(|name| name.parse::<u64>().ok())
                .is_none()
        {
            return Err(corrupt("invalid retired generation identity"));
        }
        for (index, part) in std::fs::read_dir(entry.path())?.enumerate() {
            if index == 512 {
                return Err(crate::disk_pack::resource());
            }
            let part = part?;
            let name = part.file_name();
            let valid = name
                .to_str()
                .and_then(|name| name.strip_prefix("part-"))
                .filter(|name| name.len() == 8)
                .and_then(|name| u32::from_str_radix(name, 16).ok())
                .is_some();
            if !valid || !part.file_type()?.is_file() {
                return Err(corrupt("invalid retired receipt part"));
            }
            let bytes = part.metadata()?.len();
            std::fs::remove_file(part.path())?;
            receipt.files_removed += 1;
            receipt.bytes_removed += bytes;
        }
        std::fs::remove_dir(entry.path())?;
        dirty.mark(&parent);
    }
    Ok(())
}

fn remove_empty_parents(
    lock: &WriteLock,
    path: &Path,
    dirty: &mut Dirty,
    epoch: &DeletionEpoch,
) -> Result<()> {
    let root = lock.root();
    let mut parent = path.parent();
    for _ in 0..3 {
        let Some(directory) = parent else {
            break;
        };
        if directory == root || directory.parent() == Some(root) {
            break;
        }
        let Some(_deletion) = epoch.delete(directory, true)? else {
            break;
        };
        match std::fs::remove_dir(directory) {
            Ok(()) => {
                if let Some(up) = directory.parent() {
                    dirty.mark(up);
                }
            }
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::DirectoryNotEmpty | std::io::ErrorKind::NotFound
                ) =>
            {
                break
            }
            Err(error) => return Err(error.into()),
        }
        parent = directory.parent();
    }
    Ok(())
}
