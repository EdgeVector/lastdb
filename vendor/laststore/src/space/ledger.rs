//! Root caps and filesystem-wide pending write quotas share one ledger.

use super::{corrupt, SpacePolicy, SpaceReservation, SpaceUsage};
use crate::Result;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

static LEDGER: OnceLock<Mutex<Ledger>> = OnceLock::new();

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

/// Releases after the filesystem observation remain charged to admission.
pub(super) struct Inventory {
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

    fn idle(&self) -> bool {
        self.bytes == 0 && self.inventories == 0
    }
}

impl Inventory {
    pub(super) fn start(root: &Path, device: u64) -> Result<Self> {
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
                ledger
                    .roots
                    .get_mut(root)
                    .expect("started root")
                    .inventories -= 1;
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

    pub(super) fn reserve(
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
            if let Some(state) = ledger.roots.get_mut(&self.root) {
                state.inventories -= 1;
                if state.idle() {
                    ledger.roots.remove(&self.root);
                }
            }
            if let Some(state) = ledger.devices.get_mut(&self.device) {
                state.inventories -= 1;
                if state.idle() {
                    ledger.devices.remove(&self.device);
                }
            }
        }
    }
}

pub(super) fn release(root: &Path, device: u64, bytes: u64) {
    if let Ok(mut ledger) = LEDGER.get_or_init(Mutex::default).lock() {
        if let Some(state) = ledger.roots.get_mut(root) {
            state.release(bytes);
            if state.idle() {
                ledger.roots.remove(root);
            }
        }
        if let Some(state) = ledger.devices.get_mut(&device) {
            state.release(bytes);
            if state.idle() {
                ledger.devices.remove(&device);
            }
        }
    }
}
