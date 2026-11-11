//! Disk space policy, write reservations, and the bounded native-root inventory.

use crate::Result;
use std::collections::HashMap;
use std::hash::Hash;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

static LEDGER: OnceLock<Mutex<Ledger>> = OnceLock::new();

fn corrupt(reason: &'static str) -> crate::Error {
    crate::Error::Corrupt(reason.into())
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
            free_space_reserve_bytes: 64 * 1024 * 1024,
            replacement_reserve_bytes: 64 * 1024 * 1024,
            max_files: 262_144,
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct SpaceUsage {
    files: u64,
    bytes: u64,
    available_bytes: u64,
}

/// Capacity owned through publication, cancellation, or explicit failed-work release.
pub struct SpaceReservation {
    root: PathBuf,
    device: u64,
    bytes: u64,
}

impl SpaceReservation {
    /// Worst-case new bytes owned by this ticket.
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Reserve only the added bytes while this complete prefix remains owned.
    pub fn reserve_extension(&self, policy: &SpacePolicy, total_bytes: u64) -> Result<Self> {
        let added = total_bytes
            .checked_sub(self.bytes)
            .ok_or_else(|| corrupt("space reservation extension shrank its prefix"))?;
        reserve(&self.root, policy, added)
    }

    /// Transfer an already reserved prefix without reserving its bytes again.
    pub fn absorb(&mut self, mut previous: Self) {
        assert_eq!(self.root, previous.root, "space reservation root changed");
        assert_eq!(self.device, previous.device, "space filesystem changed");
        self.bytes = self
            .bytes
            .checked_add(previous.bytes)
            .expect("same-root reservations fit their checked ledger total");
        previous.bytes = 0;
    }

    pub(crate) fn materialize(&mut self, bytes: u64) -> Result<()> {
        self.bytes = self
            .bytes
            .checked_sub(bytes)
            .ok_or_else(|| corrupt("space materialization exceeds its ticket"))?;
        release(&self.root, self.device, bytes);
        Ok(())
    }
}

impl Drop for SpaceReservation {
    fn drop(&mut self) {
        release(&self.root, self.device, self.bytes);
    }
}

/// Reserve a complete native write, including replacement headroom.
pub fn reserve_space(root: &Path, policy: &SpacePolicy, bytes: u64) -> Result<SpaceReservation> {
    reserve(root, policy, bytes)
}

fn reserve(root: &Path, policy: &SpacePolicy, bytes: u64) -> Result<SpaceReservation> {
    let root = std::fs::canonicalize(root)?;
    let device = std::fs::metadata(&root)?.dev();
    let inventory = Inventory::start(&root, device)?;
    let mut usage = SpaceUsage::default();
    if policy.disk_cap_bytes != 0 {
        count(&root, 6, policy.max_files, &mut usage)?; // walk-ok: exact native root, depth six and finite file budget
    }
    let stats = rustix::fs::statvfs(&root).map_err(std::io::Error::from)?;
    usage.available_bytes = (stats.f_bavail as u64)
        .checked_mul(stats.f_frsize as u64)
        .ok_or_else(|| corrupt("filesystem capacity overflow"))?;
    inventory.reserve(policy, usage, bytes)
}

fn opt_io<T>(res: std::io::Result<T>) -> Result<Option<T>> {
    match res {
        Ok(v) => Ok(Some(v)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

fn count(path: &Path, depth: usize, limit: usize, out: &mut SpaceUsage) -> Result<()> {
    if depth == 0 {
        return Err(corrupt("native file inventory exceeds layout depth"));
    }
    let Some(entries) = opt_io(std::fs::read_dir(path))? else {
        return Ok(());
    };
    for entry in entries {
        let entry = entry?;
        let (Some(metadata), Some(kind)) = (opt_io(entry.metadata())?, opt_io(entry.file_type())?)
        else {
            continue;
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

#[derive(Default)]
struct State {
    bytes: u64,
    inventories: usize,
    released: Option<u64>,
}

#[derive(Default)]
struct Ledger {
    roots: HashMap<PathBuf, State>,
    devices: HashMap<u64, State>,
}

struct Inventory {
    root: PathBuf,
    device: u64,
    root_released: u64,
    device_released: u64,
}

impl State {
    fn start(&mut self) -> Result<u64> {
        if self.inventories == 0 {
            self.released = Some(0);
        }
        let released = self
            .released
            .ok_or_else(|| corrupt("space inventory release overflow"))?;
        self.inventories = self
            .inventories
            .checked_add(1)
            .ok_or_else(|| corrupt("space inventory count overflow"))?;
        Ok(released)
    }

    fn accounted(&self, bytes: u64, before: u64) -> Result<(u64, u64)> {
        let required = self
            .bytes
            .checked_add(bytes)
            .ok_or_else(|| corrupt("space reservation overflow"))?;
        let accounted = self
            .released
            .and_then(|n| n.checked_sub(before))
            .and_then(|n| n.checked_add(required))
            .ok_or_else(|| corrupt("space inventory charge overflow"))?;
        Ok((required, accounted))
    }

    fn release(&mut self, bytes: u64) {
        self.bytes = self.bytes.saturating_sub(bytes);
        if self.inventories > 0 {
            self.released = self.released.and_then(|n| n.checked_add(bytes));
        }
    }
}

fn mutate_state<K: Eq + Hash + ?Sized, Q: Eq + Hash + std::borrow::Borrow<K>>(
    map: &mut HashMap<Q, State>,
    key: &K,
    f: impl FnOnce(&mut State),
) {
    if let Some(state) = map.get_mut(key) {
        f(state);
        if state.bytes == 0 && state.inventories == 0 {
            map.remove(key);
        }
    }
}

impl Inventory {
    fn start(root: &Path, device: u64) -> Result<Self> {
        let mut ledger = LEDGER
            .get_or_init(Mutex::default)
            .lock()
            .map_err(|_| corrupt("space reservation lock is poisoned"))?;
        let root_released = ledger
            .roots
            .entry(root.to_path_buf())
            .or_default()
            .start()?;
        let device_released = match ledger.devices.entry(device).or_default().start() {
            Ok(released) => released,
            Err(error) => {
                mutate_state(&mut ledger.roots, root, |s| s.inventories -= 1);
                return Err(error);
            }
        };
        Ok(Self {
            root: root.to_path_buf(),
            device,
            root_released,
            device_released,
        })
    }

    fn reserve(
        &self,
        policy: &SpacePolicy,
        usage: SpaceUsage,
        bytes: u64,
    ) -> Result<SpaceReservation> {
        let mut ledger = LEDGER
            .get_or_init(Mutex::default)
            .lock()
            .map_err(|_| corrupt("space reservation lock is poisoned"))?;
        let (root_required, root_accounted) = ledger
            .roots
            .get(&self.root)
            .ok_or_else(|| corrupt("space inventory lost its root"))?
            .accounted(bytes, self.root_released)?;
        let (device_required, device_accounted) = ledger
            .devices
            .get(&self.device)
            .ok_or_else(|| corrupt("space inventory lost its filesystem"))?
            .accounted(bytes, self.device_released)?;
        let headroom = policy
            .free_space_reserve_bytes
            .checked_add(policy.replacement_reserve_bytes)
            .and_then(|n| n.checked_add(device_accounted))
            .ok_or_else(|| corrupt("space headroom overflow"))?;
        let cap_required = root_accounted
            .checked_add(policy.replacement_reserve_bytes)
            .and_then(|n| n.checked_add(usage.bytes))
            .ok_or_else(|| corrupt("space cap headroom overflow"))?;
        if usage.available_bytes < headroom
            || (policy.disk_cap_bytes != 0 && cap_required > policy.disk_cap_bytes)
        {
            return Err(crate::Error::Io(std::io::Error::from_raw_os_error(
                rustix::io::Errno::NOSPC.raw_os_error(),
            )));
        }
        ledger
            .roots
            .get_mut(&self.root)
            .expect("checked root")
            .bytes = root_required;
        ledger
            .devices
            .get_mut(&self.device)
            .expect("checked filesystem")
            .bytes = device_required;
        Ok(SpaceReservation {
            root: self.root.clone(),
            device: self.device,
            bytes,
        })
    }
}

impl Drop for Inventory {
    fn drop(&mut self) {
        if let Ok(mut ledger) = LEDGER.get_or_init(Mutex::default).lock() {
            mutate_state(&mut ledger.roots, self.root.as_path(), |s| {
                s.inventories -= 1
            });
            mutate_state(&mut ledger.devices, &self.device, |s| s.inventories -= 1);
        }
    }
}

fn release(root: &Path, device: u64, bytes: u64) {
    if let Ok(mut ledger) = LEDGER.get_or_init(Mutex::default).lock() {
        mutate_state(&mut ledger.roots, root, |s| s.release(bytes));
        mutate_state(&mut ledger.devices, &device, |s| s.release(bytes));
    }
}
