//! # Last Store
//!
//! Disk format v2 engine ([`disk_run`]): immutable, sorted, encrypted run files
//! governed by a sealed `CURRENT` manifest.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use serde_json as _;
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
        if let Self::Io(e) = self {
            Some(e)
        } else {
            None
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
        let f = File::open(dir)?;
        #[cfg(target_os = "macos")]
        return sync_dirty_file(&f);
        #[cfg(not(target_os = "macos"))]
        Ok(f.sync_all()?)
    }
}

/// Disk space policy, write reservations, and the bounded native-root inventory.
pub mod space {
    use crate::{Error, Result};
    use std::collections::HashMap;
    use std::os::unix::fs::MetadataExt;
    use std::path::{Path, PathBuf};
    use std::sync::{Mutex, MutexGuard, OnceLock};

    type State = (u64, usize, Option<u64>);
    static LEDGER: OnceLock<Mutex<HashMap<ScopeKey, State>>> = OnceLock::new();

    #[derive(Clone, PartialEq, Eq, Hash)]
    enum ScopeKey {
        Root(PathBuf),
        Device(u64),
    }

    fn corrupt(reason: &'static str) -> Error {
        Error::Corrupt(reason.into())
    }

    fn add(a: u64, b: u64) -> Result<u64> {
        a.checked_add(b).ok_or_else(|| corrupt("space overflow"))
    }

    fn lock_ledger() -> Result<MutexGuard<'static, HashMap<ScopeKey, State>>> {
        LEDGER
            .get_or_init(Mutex::default)
            .lock()
            .map_err(|_| corrupt("space lock poisoned"))
    }

    fn update_scope(map: &mut HashMap<ScopeKey, State>, k: &ScopeKey, f: impl FnOnce(&mut State)) {
        if let Some(s) = map.get_mut(k) {
            f(s);
            if s.0 == 0 && s.1 == 0 {
                map.remove(k);
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
        pub(crate) bytes: u64,
    }

    impl SpaceReservation {
        /// Reserve only the added bytes while this complete prefix remains owned.
        pub fn reserve_extension(&self, policy: &SpacePolicy, total_bytes: u64) -> Result<Self> {
            let added = total_bytes
                .checked_sub(self.bytes)
                .ok_or_else(|| corrupt("prefix shrank"))?;
            reserve_space(&self.root, policy, added)
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

    struct ActiveInventory([ScopeKey; 2]);

    impl Drop for ActiveInventory {
        fn drop(&mut self) {
            if let Ok(mut l) = lock_ledger() {
                for k in &self.0 {
                    update_scope(&mut l, k, |s| s.1 -= 1);
                }
            }
        }
    }

    /// Reserve a complete native write, including replacement headroom.
    pub fn reserve_space(root: &Path, pol: &SpacePolicy, bytes: u64) -> Result<SpaceReservation> {
        let root = std::fs::canonicalize(root)?;
        let device = std::fs::metadata(&root)?.dev();
        let keys = [ScopeKey::Root(root.clone()), ScopeKey::Device(device)];
        let before = {
            let mut l = lock_ledger()?;
            for k in &keys {
                let s = l.entry(k.clone()).or_default();
                if s.1 == 0 {
                    s.2 = Some(0);
                }
            }
            if keys
                .iter()
                .any(|k| l[k].2.is_none() || l[k].1 == usize::MAX)
            {
                return Err(corrupt("release overflow"));
            }
            [&keys[0], &keys[1]].map(|k| {
                let s = l.get_mut(k).unwrap();
                s.1 += 1;
                s.2.unwrap()
            })
        };
        let _guard = ActiveInventory(keys.clone());
        let mut counted = (0u64, 0u64);
        if pol.disk_cap_bytes != 0 {
            count(&root, 6, pol.max_files, &mut counted)?; // walk-ok: exact native root, depth six and finite file budget
        }
        let vfs = rustix::fs::statvfs(&root).map_err(std::io::Error::from)?;
        let avail = (vfs.f_bavail as u64)
            .checked_mul(vfs.f_frsize as u64)
            .ok_or_else(|| corrupt("statvfs overflow"))?;
        let mut l = lock_ledger()?;
        let (mut reqs, mut accs) = ([0u64; 2], [0u64; 2]);
        for i in 0..2 {
            let st = l.get(&keys[i]).ok_or_else(|| corrupt("lost scope"))?;
            reqs[i] = add(st.0, bytes)?;
            accs[i] = add(
                st.2.and_then(|n| n.checked_sub(before[i]))
                    .ok_or_else(|| corrupt("charge overflow"))?,
                reqs[i],
            )?;
        }
        let head = add(
            add(pol.free_space_reserve_bytes, pol.replacement_reserve_bytes)?,
            accs[1],
        )?;
        let cap = add(add(accs[0], pol.replacement_reserve_bytes)?, counted.1)?;
        if avail < head || (pol.disk_cap_bytes != 0 && cap > pol.disk_cap_bytes) {
            return Err(
                std::io::Error::from_raw_os_error(rustix::io::Errno::NOSPC.raw_os_error()).into(),
            );
        }
        for (k, req) in keys.iter().zip(reqs) {
            l.get_mut(k).expect("scope").0 = req;
        }
        Ok(SpaceReservation {
            root,
            device,
            bytes,
        })
    }

    fn count(path: &Path, depth: usize, limit: usize, out: &mut (u64, u64)) -> Result<()> {
        if depth == 0 {
            return Err(corrupt("native file inventory exceeds layout depth"));
        }
        let entries = match std::fs::read_dir(path) {
            Ok(v) => v,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        for entry in entries {
            let p = entry?.path();
            let m = match std::fs::symlink_metadata(&p) {
                Ok(v) => v,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e.into()),
            };
            if m.file_type().is_symlink() {
                return Err(corrupt("native inventory refuses symlinks"));
            }
            if m.is_dir() {
                count(&p, depth - 1, limit, out)?;
            } else if m.is_file() {
                out.0 += 1;
                if out.0 > limit as u64 {
                    return Err(Error::ReadBudgetExceeded);
                }
                out.1 = add(out.1, m.len())?;
            } else {
                return Err(corrupt("native inventory requires regular files"));
            }
        }
        Ok(())
    }

    fn release(root: &Path, device: u64, bytes: u64) {
        if let Ok(mut l) = lock_ledger() {
            for k in [ScopeKey::Root(root.to_path_buf()), ScopeKey::Device(device)] {
                update_scope(&mut l, &k, |s| {
                    s.0 = s.0.saturating_sub(bytes);
                    if s.1 > 0 {
                        s.2 = s.2.and_then(|n| n.checked_add(bytes));
                    }
                });
            }
        }
    }
}
