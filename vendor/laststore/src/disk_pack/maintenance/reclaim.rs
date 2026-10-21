use super::{MaintenanceReceipt, SpacePolicy};
use crate::disk_pack::bytes::{u64_at, verify_checksum};
use crate::disk_pack::dependency::Coordinate;
use crate::disk_pack::write_lock::WriteLock;
use crate::disk_pack::{
    artifact, corrupt, generation, lifetime, Current, FEATURE_FILE_LIFETIME, PAGE_BYTES,
};
use crate::{durability, Result};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::path::Path;

const MAX_RETAINED_PARTS: usize = 262_144;

/// Reclaim only exact old-receipt paths, after a complete plan and all safety checks.
/// A failure leaves CURRENT intact. Receipt files are removed after their data.
pub(in crate::disk_pack) fn reclaim_locked(
    lock: &WriteLock,
    policy: &SpacePolicy,
) -> Result<MaintenanceReceipt> {
    let root = lock.root();
    let current = artifact::current(root)?;
    let mut receipt = MaintenanceReceipt {
        generation: current.generation,
        ..Default::default()
    };
    if current.required_features & FEATURE_FILE_LIFETIME == 0 {
        receipt.reclaim_deferred = true;
        return Ok(receipt);
    }
    let Some(_local) = lock.exclude_local_reads(&current)? else {
        receipt.reclaim_deferred = true;
        return Ok(receipt);
    };
    let Some(_exclusive) = lifetime::exclusive(root)? else {
        receipt.reclaim_deferred = true;
        return Ok(receipt);
    };
    validate_roots(root)?;
    cleanup_retired(root, policy.max_files, &mut receipt)?;
    let tail = crate::disk_pack::journal::JournalTail::read(root)?;
    if tail.stream != current.journal_stream || tail.last_batch != current.frontier {
        return Err(corrupt("reclaim requires a complete journal checkpoint"));
    }
    let references = lifetime::list_unlocked(root)?;
    let mut retained = BTreeSet::from([current.generation]);
    retained.extend(references.iter().map(|item| item.generation));
    let generations = generations(root, policy.max_files, &retained)?;
    let cutoff = artifact::unix_ms().saturating_sub(policy.grace_ms);
    // Grace protects each young generation's whole closure, not only young files.
    for number in &generations {
        if at_generation(generation::load_current(root, *number), root, *number)?.published_ms
            > cutoff
        {
            retained.insert(*number);
        }
    }
    let mut live = BTreeSet::new();
    for number in &retained {
        let held = at_generation(generation::load_current(root, *number), root, *number)?;
        let generation = at_generation(generation::verify(root, &held), root, *number)?;
        for coordinate in generation.coordinates {
            live.insert(coordinate);
            if live.len() > MAX_RETAINED_PARTS {
                return Err(crate::disk_pack::resource());
            }
        }
    }
    let mut old = Vec::new();
    let mut candidates = BTreeMap::new();
    for number in generations {
        if retained.contains(&number) {
            continue;
        }
        let held = at_generation(generation::load_current(root, number), root, number)?;
        if held.published_ms > cutoff {
            continue;
        }
        let generation = at_generation(generation::read_receipt(root, &held), root, number)?;
        for coordinate in &generation.coordinates {
            if !live.contains(coordinate) {
                candidates.insert(*coordinate, coordinate.path(root)?);
                if candidates.len() > MAX_RETAINED_PARTS {
                    return Err(crate::disk_pack::resource());
                }
            }
        }
        // Retain root identities only; old conservative receipts can overlap heavily.
        old.push(held);
    }
    let mut removals = Vec::new();
    for (coordinate, path) in candidates {
        if let Some(bytes) = removable(root, &coordinate, &path, cutoff)? {
            removals.push((path, bytes));
        }
    }
    // Every retained part and every candidate is validated before the first deletion.
    for (path, bytes) in removals {
        std::fs::remove_file(&path)?;
        durability::sync_directory(&File::open(path.parent().expect("part parent"))?)?;
        receipt.files_removed += 1;
        receipt.bytes_removed += bytes;
        remove_empty_parents(root, &path)?;
    }
    for held in old {
        let generation =
            at_generation(generation::read_receipt(root, &held), root, held.generation)?;
        if generation.coordinates.iter().any(|coordinate| {
            !live.contains(coordinate) && coordinate.path(root).map_or(true, |path| path.exists())
        }) {
            continue;
        }
        remove_receipt(root, &held, policy.max_files, &mut receipt)?;
    }
    Ok(receipt)
}

fn validate_roots(root: &Path) -> Result<()> {
    for name in [
        "generations",
        "references",
        "retired-generations",
        "directory",
        "locator",
        "packs",
        "journal",
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

fn generations(root: &Path, max_files: usize, published: &BTreeSet<u64>) -> Result<Vec<u64>> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(root.join("generations"))? {
        let entry = entry?;
        // Each generation owns a receipt file. Use the home inventory budget;
        // a lower independent limit can permanently prevent valid cleanup.
        if out.len() >= max_files || !entry.file_type()?.is_dir() {
            return Err(corrupt(
                "generation inventory exceeds bound or contains a non-directory",
            ));
        }
        let number = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<u64>().ok())
            .filter(|number| *number > 0)
            .ok_or_else(|| corrupt("invalid generation directory"))?;
        if !published.contains(&number) && remove_unfinished(&entry.path())? {
            continue;
        }
        out.push(number);
    }
    Ok(out)
}

/// Remove a generation directory whose receipt its writer never finished.
///
/// A writer creates `generations/<n>` and then appends the receipt pages, one
/// write for each page, and syncs the part files last. A publication names a
/// generation only after that. A kill in between leaves a directory with no
/// part file, an empty part file, a torn first page, or fewer pages than the
/// first page declares. None of them was ever published, so none holds data.
/// The caller passes only generations that CURRENT and the references do not
/// name. A directory with any other content, a gap in its part numbers, or a
/// first page that does not verify is left in place, and reclaim stops with an
/// error that names the directory and the reason. A complete receipt is read as
/// a published one.
fn remove_unfinished(path: &Path) -> Result<bool> {
    let mut parts = Vec::new();
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let index = entry
            .file_name()
            .to_str()
            .and_then(|name| name.strip_prefix("part-"))
            .filter(|hex| hex.len() == 8)
            .and_then(|hex| u32::from_str_radix(hex, 16).ok());
        let (Some(index), true) = (index, entry.file_type()?.is_file()) else {
            return Err(unreadable(
                path,
                &format!(
                    "holds `{}`, which is not a receipt part file; reclaim leaves it for an operator",
                    entry.file_name().to_string_lossy()
                ),
            ));
        };
        parts.push((index, entry.metadata()?.len()));
    }
    parts.sort_unstable();
    if parts
        .iter()
        .map(|(index, _)| *index)
        .ne(0..parts.len() as u32)
    {
        return Err(unreadable(
            path,
            "its receipt part files have a gap in their numbers; reclaim leaves it for an operator",
        ));
    }
    let page = PAGE_BYTES as u64;
    let present: u64 = parts.iter().map(|(_, length)| length / page).sum();
    if present > 0 {
        match declared_pages(&path.join("part-00000000"))? {
            Some(pages) if pages > present => {}
            Some(_) => return Ok(false),
            None => {
                return Err(unreadable(
                    path,
                    "its first receipt page does not verify; reclaim leaves it for an operator",
                ))
            }
        }
    }
    std::fs::remove_dir_all(path)?;
    durability::sync_directory(&File::open(path.parent().expect("generation parent"))?)?;
    Ok(true)
}

/// The error for a generation directory that reclaim cannot judge: it names the
/// directory and the reason.
fn unreadable(path: &Path, reason: &str) -> crate::Error {
    crate::Error::Corrupt(format!(
        "disk-pack v1: generation directory {}: {reason}",
        path.display()
    ))
}

/// Name the generation and its receipt directory in the error of a receipt read,
/// and keep the kind of the error.
fn at_generation<T>(result: Result<T>, root: &Path, number: u64) -> Result<T> {
    result.map_err(|error| {
        let place = root.join("generations").join(number.to_string());
        match error {
            crate::Error::Io(io) => crate::Error::Io(std::io::Error::new(
                io.kind(),
                format!("generation receipt {}: {io}", place.display()),
            )),
            crate::Error::Corrupt(reason) => {
                crate::Error::Corrupt(format!("generation receipt {}: {reason}", place.display()))
            }
            other => other,
        }
    })
}

/// The page count a receipt declares in its first page, when that page verifies.
fn declared_pages(first_part: &Path) -> Result<Option<u64>> {
    use std::io::Read;
    let mut bytes = vec![0; PAGE_BYTES];
    File::open(first_part)?.read_exact(&mut bytes)?;
    if &bytes[..8] != b"LDPAGE1\0" || verify_checksum(&bytes, 80).is_err() {
        return Ok(None);
    }
    Ok(Some(u64_at(&bytes, 72)))
}

fn removable(
    root: &Path,
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
    let current = artifact::current(root)?;
    coordinate.capture(root, &current)?;
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

fn remove_receipt(
    root: &Path,
    current: &Current,
    max_files: usize,
    receipt: &mut MaintenanceReceipt,
) -> Result<()> {
    let path = root
        .join("generations")
        .join(current.generation.to_string());
    let parent = root.join("retired-generations");
    if !parent.try_exists()? {
        crate::disk_pack::page_write::directory(&parent)?;
    }
    let target = parent.join(current.generation.to_string());
    // Leave the complete receipt readable until it atomically leaves the live set.
    // A restart can finish deletion in this exclusively owned retirement namespace.
    std::fs::rename(&path, target)?;
    durability::sync_directory(&File::open(root.join("generations"))?)?;
    durability::sync_directory(&File::open(parent)?)?;
    cleanup_retired(root, max_files, receipt)
}

fn cleanup_retired(root: &Path, max_files: usize, receipt: &mut MaintenanceReceipt) -> Result<()> {
    let parent = root.join("retired-generations");
    if !parent.try_exists()? {
        return Ok(());
    }
    for (index, entry) in std::fs::read_dir(&parent)?.enumerate() {
        if index >= max_files {
            return Err(crate::disk_pack::resource());
        }
        let entry = entry?;
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
            durability::sync_directory(&File::open(entry.path())?)?;
            receipt.files_removed += 1;
            receipt.bytes_removed += bytes;
        }
        std::fs::remove_dir(entry.path())?;
        durability::sync_directory(&File::open(&parent)?)?;
    }
    Ok(())
}

fn remove_empty_parents(root: &Path, path: &Path) -> Result<()> {
    let mut parent = path.parent();
    for _ in 0..3 {
        let Some(directory) = parent else {
            break;
        };
        if directory == root || directory.parent() == Some(root) {
            break;
        }
        match std::fs::remove_dir(directory) {
            Ok(()) => {
                if let Some(up) = directory.parent() {
                    durability::sync_directory(&File::open(up)?)?;
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
