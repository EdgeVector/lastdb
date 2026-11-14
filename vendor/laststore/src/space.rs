//! Disk space policy, write reservations, and the bounded native-root inventory.

use crate::{Error, Result};
use std::collections::HashMap;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, OnceLock};

type ScopeKey = (Option<PathBuf>, u64);

#[derive(Default)]
struct ScopeState {
    reserved_bytes: u64,
    active_scans: usize,
    released_during_scan: Option<u64>,
}

static LEDGER: OnceLock<Mutex<HashMap<ScopeKey, ScopeState>>> = OnceLock::new();

fn corrupt(reason: &'static str) -> Error {
    Error::Corrupt(reason.into())
}

fn add(a: u64, b: u64) -> Result<u64> {
    a.checked_add(b).ok_or_else(|| corrupt("space overflow"))
}

fn lock_ledger() -> Result<MutexGuard<'static, HashMap<ScopeKey, ScopeState>>> {
    LEDGER
        .get_or_init(Mutex::default)
        .lock()
        .map_err(|_| corrupt("space lock poisoned"))
}

fn update_scope(
    map: &mut HashMap<ScopeKey, ScopeState>,
    key: &ScopeKey,
    f: impl FnOnce(&mut ScopeState),
) {
    if let Some(state) = map.get_mut(key) {
        f(state);
        if state.reserved_bytes == 0 && state.active_scans == 0 {
            map.remove(key);
        }
    }
}

/// One policy governs write headroom, replacement space, and the file inventory bound.
#[derive(Clone, Debug)]
pub struct SpacePolicy {
    /// Whole native-root cap. Zero disables the unfrozen cap.
    pub disk_cap_bytes: u64,
    /// Filesystem free bytes retained after all active reservations.
    pub free_space_reserve_bytes: u64,
    /// Dedicated write headroom so compaction can replace files before deletion.
    pub replacement_reserve_bytes: u64,
    /// Bounded file inventory across the native root.
    pub max_files: usize,
}

impl Default for SpacePolicy {
    fn default() -> Self {
        Self {
            disk_cap_bytes: 0,
            free_space_reserve_bytes: 64 << 20,
            replacement_reserve_bytes: 64 << 20,
            max_files: 262_144,
        }
    }
}

/// Capacity owned through publication, cancellation, or explicit failed-work release.
pub struct SpaceReservation {
    root: PathBuf,
    device: u64,
    pub(crate) reserved_bytes: u64,
}

impl SpaceReservation {
    /// Reserve only the added bytes while this complete prefix remains owned.
    pub fn reserve_extension(&self, policy: &SpacePolicy, total: u64) -> Result<Self> {
        let delta = total
            .checked_sub(self.reserved_bytes)
            .ok_or_else(|| corrupt("shrank"))?;
        reserve_space(&self.root, policy, delta)
    }

    /// Transfer an already reserved prefix without reserving its bytes again.
    pub fn absorb(&mut self, mut previous: Self) {
        assert_eq!((&self.root, self.device), (&previous.root, previous.device));
        self.reserved_bytes = self
            .reserved_bytes
            .checked_add(previous.reserved_bytes)
            .expect("ledger");
        previous.reserved_bytes = 0;
    }

    pub(crate) fn materialize(&mut self, bytes: u64) -> Result<()> {
        self.reserved_bytes = self
            .reserved_bytes
            .checked_sub(bytes)
            .ok_or_else(|| corrupt("ticket"))?;
        release(&self.root, self.device, bytes);
        Ok(())
    }
}

impl Drop for SpaceReservation {
    fn drop(&mut self) {
        release(&self.root, self.device, self.reserved_bytes);
    }
}

struct ActiveInventory([ScopeKey; 2]);

impl Drop for ActiveInventory {
    fn drop(&mut self) {
        if let Ok(mut ledger) = lock_ledger() {
            for key in &self.0 {
                update_scope(&mut ledger, key, |state| state.active_scans -= 1);
            }
        }
    }
}

/// Reserve a complete native write, including replacement headroom.
pub fn reserve_space(root: &Path, policy: &SpacePolicy, bytes: u64) -> Result<SpaceReservation> {
    let root = std::fs::canonicalize(root)?;
    let device = std::fs::metadata(&root)?.dev();
    let keys = [(Some(root.clone()), 0), (None, device)];
    let before = {
        let mut ledger = lock_ledger()?;
        let mut snapshot = [0u64; 2];
        for (idx, key) in keys.iter().enumerate() {
            let state = ledger.entry(key.clone()).or_default();
            if state.active_scans == 0 {
                state.released_during_scan = Some(0);
            }
            snapshot[idx] = state
                .released_during_scan
                .filter(|_| state.active_scans < usize::MAX)
                .ok_or_else(|| corrupt("ovf"))?;
            state.active_scans += 1;
        }
        snapshot
    };
    let _guard = ActiveInventory(keys.clone());
    let mut counted = (0u64, 0u64);
    if policy.disk_cap_bytes != 0 {
        count(&root, 6, policy.max_files, &mut counted)?; // walk-ok: exact native root, depth six and finite file budget
    }
    let vfs = rustix::fs::statvfs(&root).map_err(std::io::Error::from)?;
    let avail = (vfs.f_bavail as u64)
        .checked_mul(vfs.f_frsize as u64)
        .ok_or_else(|| corrupt("statvfs overflow"))?;
    let mut ledger = lock_ledger()?;
    let (mut reqs, mut accs) = ([0u64; 2], [0u64; 2]);
    for idx in 0..2 {
        let state = ledger
            .get(&keys[idx])
            .ok_or_else(|| corrupt("lost scope"))?;
        let released = state
            .released_during_scan
            .and_then(|n| n.checked_sub(before[idx]))
            .ok_or_else(|| corrupt("chg"))?;
        let req = add(state.reserved_bytes, bytes)?;
        (reqs[idx], accs[idx]) = (req, add(released, req)?);
    }
    let headroom = add(
        add(
            policy.free_space_reserve_bytes,
            policy.replacement_reserve_bytes,
        )?,
        accs[1],
    )?;
    let used_cap = add(add(accs[0], policy.replacement_reserve_bytes)?, counted.1)?;
    if avail < headroom || (policy.disk_cap_bytes != 0 && used_cap > policy.disk_cap_bytes) {
        return Err(
            std::io::Error::from_raw_os_error(rustix::io::Errno::NOSPC.raw_os_error()).into(),
        );
    }
    for (key, req) in keys.iter().zip(reqs) {
        ledger.get_mut(key).expect("scope").reserved_bytes = req;
    }
    Ok(SpaceReservation {
        root,
        device,
        reserved_bytes: bytes,
    })
}

fn count(path: &Path, depth: usize, limit: usize, out: &mut (u64, u64)) -> Result<()> {
    if depth == 0 {
        return Err(corrupt("native file inventory exceeds layout depth"));
    }
    let entries = match std::fs::read_dir(path) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        res => res?,
    };
    for entry in entries {
        let child = entry?.path();
        let meta = match std::fs::symlink_metadata(&child) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            res => res?,
        };
        if meta.file_type().is_symlink() {
            return Err(corrupt("native inventory refuses symlinks"));
        } else if meta.is_dir() {
            count(&child, depth - 1, limit, out)?;
        } else if meta.is_file() {
            out.0 += 1;
            if out.0 > limit as u64 {
                return Err(Error::ReadBudgetExceeded);
            }
            out.1 = add(out.1, meta.len())?;
        } else {
            return Err(corrupt("native inventory requires regular files"));
        }
    }
    Ok(())
}

fn release(root: &Path, device: u64, bytes: u64) {
    if let Ok(mut ledger) = lock_ledger() {
        for key in [(Some(root.to_path_buf()), 0), (None, device)] {
            update_scope(&mut ledger, &key, |state| {
                state.reserved_bytes = state.reserved_bytes.saturating_sub(bytes);
                if state.active_scans > 0 {
                    state.released_during_scan = state
                        .released_during_scan
                        .and_then(|n| n.checked_add(bytes));
                }
            });
        }
    }
}
