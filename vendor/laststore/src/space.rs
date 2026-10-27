//! Disk space policy, write reservations, and the bounded native-root inventory.
//!
//! One process-wide ledger admits every native write against these limits.

use crate::Result;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

/// A broken invariant of the space ledger itself.
fn corrupt(reason: &'static str) -> crate::Error {
    crate::Error::Corrupt(reason.into())
}

static RESERVATIONS: OnceLock<Mutex<HashMap<PathBuf, ReservationState>>> = OnceLock::new();

#[derive(Default)]
struct ReservationState {
    bytes: u64,
    inventories: usize,
    // None refuses admission after overflow until all current inventories end.
    released: Option<u64>,
}

/// Keep releases visible until this inventory's admission decision. No ledger
/// mutex spans filesystem I/O, and an idle root leaves no retained map entry.
struct ReservationInventory {
    root: PathBuf,
    released: u64,
}

impl ReservationInventory {
    fn start(root: &Path) -> Result<Self> {
        let mut ledger = RESERVATIONS
            .get_or_init(Mutex::default)
            .lock()
            .map_err(|_| corrupt("space reservation lock is poisoned"))?;
        let state = ledger.entry(root.to_path_buf()).or_default();
        if state.inventories == 0 {
            state.released = Some(0);
        }
        let released = state
            .released
            .ok_or_else(|| corrupt("space inventory release overflow"))?;
        state.inventories = state
            .inventories
            .checked_add(1)
            .ok_or_else(|| corrupt("space inventory count overflow"))?;
        Ok(Self {
            root: root.to_path_buf(),
            released,
        })
    }

    fn released_since(&self, state: &ReservationState) -> Result<u64> {
        let released = state
            .released
            .and_then(|n| n.checked_sub(self.released))
            .ok_or_else(|| corrupt("space inventory release overflow"))?;
        Ok(released)
    }
}

impl Drop for ReservationInventory {
    fn drop(&mut self) {
        if let Ok(mut ledger) = RESERVATIONS.get_or_init(Mutex::default).lock() {
            if let Some(state) = ledger.get_mut(&self.root) {
                state.inventories -= 1;
                if state.bytes == 0 && state.inventories == 0 {
                    ledger.remove(&self.root);
                }
            }
        }
    }
}

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
    /// Most old generations one reclaim call plans and retires, oldest first.
    /// A bounded call keeps the exclusive hold short; the receipt reports what remains.
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
        reserve(&self.root, policy, added)
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
    reserve(root, policy, bytes)
}

fn reserve(root: &Path, policy: &SpacePolicy, bytes: u64) -> Result<SpaceReservation> {
    let root = std::fs::canonicalize(root)?;
    let inventory = ReservationInventory::start(&root)?;
    // The ordinary unfrozen-cap path must not enumerate the home per mutation.
    let usage = if policy.disk_cap_bytes == 0 {
        SpaceUsage {
            available_bytes: available_space(&root)?,
            ..Default::default()
        }
    } else {
        space_usage(&root, policy.max_files)?
    };
    let mut ledger = RESERVATIONS
        .get_or_init(Mutex::default)
        .lock()
        .map_err(|_| corrupt("space reservation lock is poisoned"))?;
    let state = ledger
        .get_mut(&root)
        .ok_or_else(|| corrupt("space inventory lost its reservation ledger"))?;
    let required = state
        .bytes
        .checked_add(bytes)
        .ok_or_else(|| corrupt("space reservation overflow"))?;
    // A file omitted by the inventory either still has a live reservation or
    // belongs to a ticket released since inventory start. Count both sets.
    // Releases may include cancelled work or files already seen: overcounting
    // is conservative, temporary, and avoids retries under continuous writes.
    let accounted = required
        .checked_add(inventory.released_since(state)?)
        .ok_or_else(|| corrupt("space inventory charge overflow"))?;
    let headroom = policy
        .free_space_reserve_bytes
        .checked_add(policy.replacement_reserve_bytes)
        .and_then(|n| n.checked_add(accounted))
        .ok_or_else(|| corrupt("space headroom overflow"))?;
    let cap_required = accounted
        .checked_add(policy.replacement_reserve_bytes)
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
    state.bytes = required;
    Ok(SpaceReservation { root, bytes })
}

impl Drop for SpaceReservation {
    fn drop(&mut self) {
        if let Ok(mut ledger) = RESERVATIONS.get_or_init(Mutex::default).lock() {
            if let Some(state) = ledger.get_mut(&self.root) {
                state.bytes = state.bytes.saturating_sub(self.bytes);
                if state.inventories > 0 {
                    state.released = state.released.and_then(|n| n.checked_add(self.bytes));
                }
                if state.bytes == 0 && state.inventories == 0 {
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
    out.available_bytes = available_space(root)?;
    Ok(out)
}

/// Filesystem bytes available to this user. Does not enumerate files or create a root.
pub fn available_space(root: &Path) -> Result<u64> {
    let stats = rustix::fs::statvfs(root).map_err(std::io::Error::from)?;
    (stats.f_bavail as u64)
        .checked_mul(stats.f_frsize as u64)
        .ok_or_else(|| corrupt("filesystem capacity overflow"))
}

fn count(path: &Path, depth: usize, limit: usize, out: &mut SpaceUsage) -> Result<()> {
    if depth == 0 {
        return Err(corrupt("native file inventory exceeds layout depth"));
    }
    let entries = match std::fs::read_dir(path) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    for entry in entries {
        let entry = entry?;
        let metadata = match entry.metadata() {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        let kind = match entry.file_type() {
            Ok(kind) => kind,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        if kind.is_symlink() {
            return Err(corrupt("native inventory refuses symlinks"));
        }
        if metadata.is_dir() {
            count(&entry.path(), depth - 1, limit, out)?;
        } else if metadata.is_file() {
            out.files += 1;
            if out.files > limit as u64 {
                return Err(crate::Error::ReadBudgetExceeded);
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
