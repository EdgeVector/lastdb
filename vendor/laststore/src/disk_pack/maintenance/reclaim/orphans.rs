//! Part files that no generation receipt names.
//!
//! A crash between the first output file of a publication or a compaction and the
//! rename of `CURRENT` leaves files that no receipt lists. Reclaim removes only
//! files that an old receipt lists, so these files stay for ever. A sweep under
//! the writer lease finds them: every part file that no receipt on disk names
//! is an orphan, because the lease excludes any other writer and the journal
//! checkpoint is complete. A minimum age guards against any unforeseen writer.

use super::*;
use crate::disk_pack::journal::JournalTail;

const NAMESPACES: [&str; 4] = ["directory", "locator", "packs", "journal"];
/// Deepest part file: `journal/<stream>/batch-<n>/map/part-<n>`.
const MAX_DEPTH: usize = 4;

/// Remove unreferenced part files that are older than `policy.grace_ms`.
pub(in crate::disk_pack) fn sweep_orphans_locked(
    lock: &WriteLock,
    policy: &SpacePolicy,
) -> Result<MaintenanceReceipt> {
    sweep_orphans_at(lock, policy, artifact::current(lock.root())?, false)
}

pub(in crate::disk_pack) fn sweep_orphans_at(
    lock: &WriteLock,
    policy: &SpacePolicy,
    current: Current,
    concurrent: bool,
) -> Result<MaintenanceReceipt> {
    let root = lock.root();
    let mut receipt = MaintenanceReceipt {
        generation: current.generation,
        ..Default::default()
    };
    if current.required_features & FEATURE_FILE_LIFETIME == 0 {
        receipt.reclaim_deferred = true;
        return Ok(receipt);
    }
    // The sweep removes only files that no receipt on disk names. A local read
    // holds a generation whose receipt stays, so no active read defers the sweep.
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
    let tail = JournalTail::read(root)?;
    if !concurrent && (tail.stream != current.journal_stream || tail.last_batch != current.frontier)
    {
        return Err(corrupt(
            "orphan sweep requires a complete journal checkpoint",
        ));
    }
    // An unreadable receipt could name parts, so it ends the sweep without a removal.
    let mut published = BTreeSet::from([current.generation]);
    published.extend(
        lifetime::list_unlocked(root)?
            .iter()
            .map(|item| item.generation),
    );
    published.extend(local.held().iter().copied());
    let named = named_paths(
        root,
        policy.max_files,
        &published,
        concurrent.then_some(current.generation),
    )?;
    let cutoff = artifact::unix_ms().saturating_sub(policy.grace_ms);
    let mut dirty = Dirty::default();
    for path in part_files(root, policy.max_files)? {
        if named.contains(&path) {
            continue;
        }
        if concurrent && !super::concurrent::old_part(&path, current.generation)? {
            continue;
        }
        validate_parents(root, &path)?;
        let metadata = std::fs::symlink_metadata(&path)?;
        let modified = metadata
            .modified()?
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(u128::MAX, |age| age.as_millis());
        if !metadata.is_file() || modified > u128::from(cutoff) {
            continue;
        }
        std::fs::remove_file(&path)?;
        dirty.mark(path.parent().expect("part parent"));
        receipt.files_removed += 1;
        receipt.bytes_removed += metadata.len();
        remove_empty_parents(root, &path, &mut dirty)?;
    }
    dirty.flush()?;
    Ok(receipt)
}

/// Every path that any generation receipt on disk lists.
fn named_paths(
    root: &Path,
    max_files: usize,
    published: &BTreeSet<u64>,
    ceiling: Option<u64>,
) -> Result<BTreeSet<PathBuf>> {
    let mut named = BTreeSet::new();
    for number in generations(root, max_files, published, ceiling)? {
        let held = at_generation(generation::load_current(root, number), root, number)?;
        let receipt = at_generation(generation::read_receipt(root, &held), root, number)?;
        for coordinate in receipt.coordinates {
            named.insert(coordinate.path(root)?);
            if named.len() > MAX_RETAINED_PARTS {
                return Err(crate::Error::Config(format!(
                    "disk-pack orphan sweep: the receipts on disk name more than \
                     {MAX_RETAINED_PARTS} parts; finish reclaim of old generations first; \
                     nothing was deleted"
                )));
            }
        }
    }
    Ok(named)
}

/// Regular files named `part-<8 hex digits>` under the four part namespaces.
fn part_files(root: &Path, max_files: usize) -> Result<Vec<PathBuf>> {
    let mut found = Vec::new();
    let mut pending: Vec<(PathBuf, usize)> =
        NAMESPACES.iter().map(|name| (root.join(name), 1)).collect();
    while let Some((directory, depth)) = pending.pop() {
        let entries = match std::fs::read_dir(&directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        for entry in entries {
            let entry = entry?;
            let kind = entry.file_type()?;
            if kind.is_dir() && depth < MAX_DEPTH {
                pending.push((entry.path(), depth + 1));
            } else if kind.is_file() && is_part_name(&entry.file_name()) {
                found.push(entry.path());
                if found.len() > max_files {
                    return Err(crate::disk_pack::resource());
                }
            }
        }
    }
    Ok(found)
}

fn is_part_name(name: &std::ffi::OsStr) -> bool {
    name.to_str()
        .and_then(|name| name.strip_prefix("part-"))
        .is_some_and(|hex| hex.len() == 8 && hex.bytes().all(|byte| byte.is_ascii_hexdigit()))
}
