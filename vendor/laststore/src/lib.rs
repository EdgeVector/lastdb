//! # Last Store
//!
//! Disk format v2 engine ([`disk_run`]): immutable, sorted, encrypted run files
//! governed by a sealed `CURRENT` manifest.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::fmt;

pub mod disk_run;
mod frame;

/// True when isolated storage diagnostics (`LASTDB_SNAPSHOT_TIMING=1`) are enabled.
pub fn diagnostics_enabled() -> bool {
    std::env::var("LASTDB_SNAPSHOT_TIMING").is_ok_and(|v| matches!(v.trim(), "1" | "true" | "on"))
}

/// Result alias for Last Store operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors returned by the disk format v2 engine ([`crate::disk_run`]).
#[derive(Debug)]
pub enum Error {
    /// Underlying filesystem or IO failure.
    Io(std::io::Error),
    /// On-disk format is truncated or inconsistent.
    Corrupt(String),
    /// AEAD authentication failed while decrypting a frame.
    AeadAuthFail,
    /// Invalid configuration or caller input.
    Config(String),
    /// A disk-run call exhausted its explicit work or memory budget.
    ReadBudgetExceeded,
    /// A mutation cannot reserve its bounded write resources.
    WriteCapacity(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "io: {e}"),
            Self::Corrupt(s) => write!(f, "corrupt: {s}"),
            Self::AeadAuthFail => f.write_str("aead authentication failed"),
            Self::Config(s) => write!(f, "config: {s}"),
            Self::ReadBudgetExceeded => f.write_str("read resource limit exceeded"),
            Self::WriteCapacity(s) => write!(f, "write capacity unavailable: {s}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

pub(crate) mod durability {
    use crate::Result;
    use std::fs::File;
    use std::path::Path;

    pub(crate) fn sync_dirty_file(file: &File) -> Result<()> {
        #[cfg(target_os = "macos")]
        rustix::fs::fcntl_fullfsync(file).map_err(std::io::Error::from)?;
        #[cfg(not(target_os = "macos"))]
        file.sync_data()?;
        Ok(())
    }

    pub(crate) fn sync_directory(dir: &Path) -> Result<()> {
        let dir = File::open(dir)?;
        #[cfg(target_os = "macos")]
        rustix::fs::fcntl_fullfsync(&dir).map_err(std::io::Error::from)?;
        #[cfg(not(target_os = "macos"))]
        dir.sync_all()?;
        Ok(())
    }
}

/// Disk space policy, write reservations, and the bounded native-root inventory.
pub mod space {
    use crate::{Error, Result};
    use std::collections::HashMap;
    use std::os::unix::fs::MetadataExt;
    use std::path::{Path, PathBuf};
    use std::sync::{Mutex, MutexGuard, OnceLock};

    static LEDGER: OnceLock<Mutex<Ledger>> = OnceLock::new();
    type Ledger = HashMap<ScopeKey, State>;

    #[derive(Clone, PartialEq, Eq, Hash)]
    enum ScopeKey {
        Root(PathBuf),
        Device(u64),
    }

    fn corrupt(reason: &'static str) -> Error {
        Error::Corrupt(reason.into())
    }

    fn lock_ledger() -> Result<MutexGuard<'static, Ledger>> {
        LEDGER
            .get_or_init(Mutex::default)
            .lock()
            .map_err(|_| corrupt("space lock poisoned"))
    }

    fn sum3(a: u64, b: u64, c: u64) -> Result<u64> {
        a.checked_add(b)
            .and_then(|n| n.checked_add(c))
            .ok_or_else(|| corrupt("space overflow"))
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
        bytes: u64,
    }

    impl SpaceReservation {
        /// Worst-case new bytes owned by this ticket.
        pub fn bytes(&self) -> u64 {
            self.bytes
        }

        /// Reserve only the added bytes while this complete prefix remains owned.
        pub fn reserve_extension(&self, policy: &SpacePolicy, total_bytes: u64) -> Result<Self> {
            let added = total_bytes.checked_sub(self.bytes);
            reserve_space(
                &self.root,
                policy,
                added.ok_or_else(|| corrupt("prefix shrank"))?,
            )
        }

        /// Transfer an already reserved prefix without reserving its bytes again.
        pub fn absorb(&mut self, mut prev: Self) {
            assert_eq!((&self.root, self.device), (&prev.root, prev.device));
            self.bytes = self.bytes.checked_add(prev.bytes).expect("ledger");
            prev.bytes = 0;
        }

        pub(crate) fn materialize(&mut self, bytes: u64) -> Result<()> {
            self.bytes = self
                .bytes
                .checked_sub(bytes)
                .ok_or_else(|| corrupt("ticket exceeded"))?;
            release(&self.root, self.device, bytes);
            Ok(())
        }
    }

    impl Drop for SpaceReservation {
        fn drop(&mut self) {
            release(&self.root, self.device, self.bytes);
        }
    }

    fn scopes(root: &Path, device: u64) -> [ScopeKey; 2] {
        [ScopeKey::Root(root.to_path_buf()), ScopeKey::Device(device)]
    }

    /// Reserve a complete native write, including replacement headroom.
    pub fn reserve_space(
        root: &Path,
        policy: &SpacePolicy,
        bytes: u64,
    ) -> Result<SpaceReservation> {
        let root = std::fs::canonicalize(root)?;
        let device = std::fs::metadata(&root)?.dev();
        let inv = Inventory::start(scopes(&root, device))?;
        let mut counted = (0u64, 0u64);
        if policy.disk_cap_bytes != 0 {
            count(&root, 6, policy.max_files, &mut counted)?; // walk-ok: exact native root, depth six and finite file budget
        }
        let s = rustix::fs::statvfs(&root).map_err(std::io::Error::from)?;
        let avail = (s.f_bavail as u64)
            .checked_mul(s.f_frsize as u64)
            .ok_or_else(|| corrupt("statvfs overflow"))?;
        inv.commit(policy, counted.1, avail, bytes)?;
        Ok(SpaceReservation {
            root,
            device,
            bytes,
        })
    }

    fn opt_io<T>(res: std::io::Result<T>) -> Result<Option<T>> {
        match res {
            Ok(v) => Ok(Some(v)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    fn count(path: &Path, depth: usize, limit: usize, out: &mut (u64, u64)) -> Result<()> {
        if depth == 0 {
            return Err(corrupt("native file inventory exceeds layout depth"));
        }
        let Some(entries) = opt_io(std::fs::read_dir(path))? else {
            return Ok(());
        };
        for entry in entries {
            let entry = entry?;
            let Some(m) = opt_io(std::fs::symlink_metadata(entry.path()))? else {
                continue;
            };
            if m.file_type().is_symlink() {
                return Err(corrupt("native inventory refuses symlinks"));
            } else if m.is_dir() {
                count(&entry.path(), depth - 1, limit, out)?;
            } else if m.is_file() {
                out.0 += 1;
                if out.0 > limit as u64 {
                    return Err(Error::ReadBudgetExceeded);
                }
                out.1 = out
                    .1
                    .checked_add(m.len())
                    .ok_or_else(|| corrupt("byte count overflow"))?;
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

    impl State {
        fn start(&mut self) -> Result<u64> {
            if self.inventories == 0 {
                self.released = Some(0);
            }
            let rel = self.released.ok_or_else(|| corrupt("release overflow"))?;
            self.inventories = self
                .inventories
                .checked_add(1)
                .ok_or_else(|| corrupt("inventory overflow"))?;
            Ok(rel)
        }

        fn accounted(&self, bytes: u64, before: u64) -> Result<(u64, u64)> {
            let req = self
                .bytes
                .checked_add(bytes)
                .ok_or_else(|| corrupt("reservation overflow"))?;
            let rel = self
                .released
                .and_then(|n| n.checked_sub(before)?.checked_add(req))
                .ok_or_else(|| corrupt("charge overflow"))?;
            Ok((req, rel))
        }
    }

    fn update_scope(ledger: &mut Ledger, key: ScopeKey, f: impl FnOnce(&mut State)) {
        if let Some(s) = ledger.get_mut(&key) {
            f(s);
            if s.bytes == 0 && s.inventories == 0 {
                ledger.remove(&key);
            }
        }
    }

    struct Inventory {
        keys: [ScopeKey; 2],
        before: [u64; 2],
    }

    impl Inventory {
        fn start(keys: [ScopeKey; 2]) -> Result<Self> {
            let mut l = lock_ledger()?;
            let r = l.entry(keys[0].clone()).or_default().start()?;
            let d = l
                .entry(keys[1].clone())
                .or_default()
                .start()
                .inspect_err(|_| update_scope(&mut l, keys[0].clone(), |s| s.inventories -= 1))?;
            Ok(Self {
                keys,
                before: [r, d],
            })
        }

        fn commit(&self, pol: &SpacePolicy, used: u64, avail: u64, bytes: u64) -> Result<()> {
            let mut l = lock_ledger()?;
            let acc = |i: usize| {
                l.get(&self.keys[i])
                    .ok_or_else(|| corrupt("lost scope"))?
                    .accounted(bytes, self.before[i])
            };
            let ((r_req, r_acc), (d_req, d_acc)) = (acc(0)?, acc(1)?);
            let head = sum3(
                pol.free_space_reserve_bytes,
                pol.replacement_reserve_bytes,
                d_acc,
            )?;
            let cap = sum3(r_acc, pol.replacement_reserve_bytes, used)?;
            if avail < head || (pol.disk_cap_bytes != 0 && cap > pol.disk_cap_bytes) {
                return Err(Error::Io(std::io::Error::from_raw_os_error(
                    rustix::io::Errno::NOSPC.raw_os_error(),
                )));
            }
            for (k, req) in self.keys.iter().zip([r_req, d_req]) {
                l.get_mut(k).expect("scope").bytes = req;
            }
            Ok(())
        }
    }

    impl Drop for Inventory {
        fn drop(&mut self) {
            if let Ok(mut l) = lock_ledger() {
                for k in &self.keys {
                    update_scope(&mut l, k.clone(), |s| s.inventories -= 1);
                }
            }
        }
    }

    fn release(root: &Path, device: u64, bytes: u64) {
        if let Ok(mut l) = lock_ledger() {
            for k in scopes(root, device) {
                update_scope(&mut l, k, |s| {
                    s.bytes = s.bytes.saturating_sub(bytes);
                    if s.inventories > 0 {
                        s.released = s.released.and_then(|n| n.checked_add(bytes));
                    }
                });
            }
        }
    }
}
