use crate::disk_pack::corrupt;
use crate::Result;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

static RESERVATIONS: OnceLock<Mutex<HashMap<PathBuf, u64>>> = OnceLock::new();

/// One policy governs write headroom, replacement space, and obsolete-file grace.
#[derive(Clone, Debug)]
pub struct SpacePolicy {
    /// Whole native-root cap. Zero disables the unfrozen cap.
    pub disk_cap_bytes: u64,
    /// Filesystem free bytes retained after all active reservations.
    pub free_space_reserve_bytes: u64,
    /// Dedicated write headroom so compaction can replace files before deletion.
    pub replacement_reserve_bytes: u64,
    /// Minimum obsolete-file age in milliseconds. Active references always win.
    pub grace_ms: u64,
    /// Bounded file inventory across the six-level native layout.
    pub max_files: usize,
    /// Most obsolete generations one cleanup step can retire.
    pub reclaim_generations: usize,
}

impl Default for SpacePolicy {
    fn default() -> Self {
        Self {
            disk_cap_bytes: 0,
            free_space_reserve_bytes: 64 * 1024 * 1024,
            replacement_reserve_bytes: 64 * 1024 * 1024,
            grace_ms: 24 * 60 * 60 * 1000,
            max_files: 262_144,
            reclaim_generations: usize::MAX,
        }
    }
}

/// Current native-root bytes and filesystem capacity, without a home-root walk.
#[derive(Clone, Copy, Debug, Default)]
pub struct SpaceUsage {
    /// Regular files in the exact native root.
    pub files: u64,
    /// Their total physical file lengths.
    pub bytes: u64,
    /// Filesystem bytes available to this user.
    pub available_bytes: u64,
}

/// Capacity owned through publication, cancellation, or explicit failed-work release.
pub struct SpaceReservation {
    root: PathBuf,
    bytes: u64,
}

impl SpaceReservation {
    /// Worst-case new bytes owned by this ticket.
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Reserve only the added bytes while this complete prefix remains owned.
    /// Dropping the returned ticket leaves the previous prefix unchanged.
    pub fn reserve_extension(
        &self,
        policy: &SpacePolicy,
        total_bytes: u64,
    ) -> Result<SpaceReservation> {
        let added = total_bytes
            .checked_sub(self.bytes)
            .ok_or_else(|| corrupt("space reservation extension shrank its prefix"))?;
        reserve(&self.root, policy, added, false)
    }

    /// Transfer an already reserved prefix without reserving its bytes again.
    /// Both tickets must belong to the same root. Their combined bytes were
    /// already checked by the ledger when the extension was reserved.
    pub fn absorb(&mut self, mut previous: SpaceReservation) {
        assert_eq!(self.root, previous.root, "space reservation root changed");
        self.bytes = self
            .bytes
            .checked_add(previous.bytes)
            .expect("same-root reservations fit their checked ledger total");
        previous.bytes = 0;
    }
}

/// Reserve a complete native write, including replacement headroom.
/// Callers also hold the exclusive native writer lease across admission and output.
pub fn reserve_space(root: &Path, policy: &SpacePolicy, bytes: u64) -> Result<SpaceReservation> {
    reserve(root, policy, bytes, false)
}

pub(super) fn reserve_replacement(
    root: &Path,
    policy: &SpacePolicy,
    bytes: u64,
) -> Result<SpaceReservation> {
    reserve(root, policy, bytes, true)
}

fn reserve(
    root: &Path,
    policy: &SpacePolicy,
    bytes: u64,
    replacement: bool,
) -> Result<SpaceReservation> {
    let root = std::fs::canonicalize(root)?;
    // The ordinary unfrozen-cap path must not enumerate the home per mutation.
    let usage = if policy.disk_cap_bytes == 0 {
        SpaceUsage {
            available_bytes: available(&root)?,
            ..Default::default()
        }
    } else {
        space_usage(&root, policy.max_files)?
    };
    let mut ledger = RESERVATIONS
        .get_or_init(Mutex::default)
        .lock()
        .map_err(|_| corrupt("space reservation lock is poisoned"))?;
    let already = *ledger.get(&root).unwrap_or(&0);
    let required = already
        .checked_add(bytes)
        .ok_or_else(|| corrupt("space reservation overflow"))?;
    let headroom = policy
        .free_space_reserve_bytes
        .checked_add(if replacement {
            0
        } else {
            policy.replacement_reserve_bytes
        })
        .and_then(|n| n.checked_add(required))
        .ok_or_else(|| corrupt("space headroom overflow"))?;
    let cap_required = required
        .checked_add(if replacement {
            0
        } else {
            policy.replacement_reserve_bytes
        })
        .ok_or_else(|| corrupt("space cap headroom overflow"))?;
    let cap_exceeded = policy.disk_cap_bytes != 0
        && usage
            .bytes
            .checked_add(cap_required)
            .map_or(true, |n| n > policy.disk_cap_bytes);
    if usage.available_bytes < headroom || cap_exceeded {
        return Err(crate::Error::Io(std::io::Error::from_raw_os_error(
            rustix::io::Errno::NOSPC.raw_os_error(),
        )));
    }
    ledger.insert(root.clone(), required);
    Ok(SpaceReservation { root, bytes })
}

impl Drop for SpaceReservation {
    fn drop(&mut self) {
        if let Ok(mut ledger) = RESERVATIONS.get_or_init(Mutex::default).lock() {
            if let Some(total) = ledger.get_mut(&self.root) {
                *total = total.saturating_sub(self.bytes);
                if *total == 0 {
                    ledger.remove(&self.root);
                }
            }
        }
    }
}

/// Explicit bounded administrative inventory; ordinary uncapped writes use statvfs only.
pub fn space_usage(root: &Path, max_files: usize) -> Result<SpaceUsage> {
    let mut out = SpaceUsage::default();
    count(root, 6, max_files, &mut out)?; // walk-ok: exact native root, depth six and finite file budget
    out.available_bytes = available(root)?;
    Ok(out)
}

fn available(root: &Path) -> Result<u64> {
    let stats = rustix::fs::statvfs(root).map_err(std::io::Error::from)?;
    (stats.f_bavail as u64)
        .checked_mul(stats.f_frsize as u64)
        .ok_or_else(|| corrupt("filesystem capacity overflow"))
}

fn count(path: &Path, depth: usize, limit: usize, out: &mut SpaceUsage) -> Result<()> {
    if depth == 0 {
        return Err(corrupt("native file inventory exceeds layout depth"));
    }
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if entry.file_type()?.is_symlink() {
            return Err(corrupt("native inventory refuses symlinks"));
        }
        if metadata.is_dir() {
            count(&entry.path(), depth - 1, limit, out)?;
        } else if metadata.is_file() {
            out.files += 1;
            if out.files > limit as u64 {
                return Err(crate::disk_pack::resource());
            }
            out.bytes = out
                .bytes
                .checked_add(metadata.len())
                .ok_or_else(|| corrupt("native byte count overflow"))?;
        } else {
            return Err(corrupt("native inventory requires regular files"));
        }
    }
    Ok(())
}
