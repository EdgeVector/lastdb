//! Disk format v2 engine ([`disk_run`]): immutable, sorted, encrypted run files and `CURRENT`.

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
        let f = File::open(dir)?;
        #[cfg(target_os = "macos")]
        return sync_dirty_file(&f);
        #[cfg(not(target_os = "macos"))]
        Ok(f.sync_all()?)
    }
}

pub(crate) mod bloom {
    use crate::disk_run::codec::{corrupt, get_len, get_slice, get_varint, put_varint};
    use crate::Result;

    const PROBES: u64 = 7;
    const MAX_FILTER_BYTES: usize = 64 << 20;

    fn mix(mut z: u64) -> u64 {
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    pub(crate) fn key_hash(key: &[u8]) -> u64 {
        mix(key.iter().fold(0xcbf2_9ce4_8422_2325, |h, &b| {
            (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3)
        }))
    }

    fn probes(hash: u64, bits: u64) -> impl Iterator<Item = u64> {
        let step = mix(hash ^ 0x9e37_79b9_7f4a_7c15) | 1;
        (0..PROBES).map(move |i| hash.wrapping_add(i.wrapping_mul(step)) % bits)
    }

    pub(crate) fn put_filter(out: &mut Vec<u8>, hashes: &[u64], bpk: u32) {
        let bytes = if bpk == 0 || hashes.is_empty() {
            0
        } else {
            (hashes.len() * bpk as usize)
                .div_ceil(8)
                .clamp(8, MAX_FILTER_BYTES)
        };
        put_varint(out, bytes as u64);
        put_varint(out, PROBES);
        let start = out.len();
        out.resize(start + bytes, 0);
        if bytes > 0 {
            let (bits, filter) = ((bytes * 8) as u64, &mut out[start..]);
            for &h in hashes {
                probes(h, bits).for_each(|b| filter[(b / 8) as usize] |= 1 << (b % 8));
            }
        }
    }

    pub(crate) fn read_filter<'a>(bytes: &'a [u8], at: &mut usize) -> Result<&'a [u8]> {
        let len = get_len(bytes, at, MAX_FILTER_BYTES)?;
        (get_varint(bytes, at)? == PROBES)
            .then(|| get_slice(bytes, at, len))
            .ok_or_else(|| corrupt("bloom probes"))?
    }

    pub(crate) fn may_hold(filter: &[u8], hash: u64) -> bool {
        filter.is_empty()
            || probes(hash, (filter.len() * 8) as u64)
                .all(|b| filter[(b / 8) as usize] & (1 << (b % 8)) != 0)
    }
}

/// Disk space policy, write reservations, and the bounded native-root inventory.
pub mod space {
    use crate::{Error, Result};
    use std::collections::HashMap;
    use std::os::unix::fs::MetadataExt;
    use std::path::{Path, PathBuf};
    use std::sync::{Mutex, MutexGuard, OnceLock};

    type ScopeKey = (Option<PathBuf>, u64);
    type State = (u64, usize, Option<u64>);
    static LEDGER: OnceLock<Mutex<HashMap<ScopeKey, State>>> = OnceLock::new();

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
    pub struct SpaceReservation(PathBuf, u64, pub(crate) u64);

    impl SpaceReservation {
        /// Reserve only the added bytes while this complete prefix remains owned.
        pub fn reserve_extension(&self, policy: &SpacePolicy, total: u64) -> Result<Self> {
            let delta = total.checked_sub(self.2).ok_or_else(|| corrupt("shrank"))?;
            reserve_space(&self.0, policy, delta)
        }
        /// Transfer an already reserved prefix without reserving its bytes again.
        pub fn absorb(&mut self, mut prev: Self) {
            assert_eq!((&self.0, self.1), (&prev.0, prev.1));
            self.2 = self.2.checked_add(prev.2).expect("ledger");
            prev.2 = 0;
        }
        pub(crate) fn materialize(&mut self, bytes: u64) -> Result<()> {
            self.2 = self.2.checked_sub(bytes).ok_or_else(|| corrupt("ticket"))?;
            release(&self.0, self.1, bytes);
            Ok(())
        }
    }

    impl Drop for SpaceReservation {
        fn drop(&mut self) {
            release(&self.0, self.1, self.2);
        }
    }

    struct ActiveInventory([ScopeKey; 2]);

    impl Drop for ActiveInventory {
        fn drop(&mut self) {
            if let Ok(mut l) = lock_ledger() {
                self.0
                    .iter()
                    .for_each(|k| update_scope(&mut l, k, |s| s.1 -= 1));
            }
        }
    }

    /// Reserve a complete native write, including replacement headroom.
    pub fn reserve_space(root: &Path, pol: &SpacePolicy, bytes: u64) -> Result<SpaceReservation> {
        let root = std::fs::canonicalize(root)?;
        let dev = std::fs::metadata(&root)?.dev();
        let keys = [(Some(root.clone()), 0), (None, dev)];
        let before = {
            let mut l = lock_ledger()?;
            let mut snap = [0u64; 2];
            for (i, k) in keys.iter().enumerate() {
                let s = l.entry(k.clone()).or_default();
                if s.1 == 0 {
                    s.2 = Some(0);
                }
                snap[i] =
                    s.2.filter(|_| s.1 < usize::MAX)
                        .ok_or_else(|| corrupt("ovf"))?;
                s.1 += 1;
            }
            snap
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
            let chg =
                st.2.and_then(|n| n.checked_sub(before[i]))
                    .ok_or_else(|| corrupt("chg"))?;
            (reqs[i], accs[i]) = (add(st.0, bytes)?, add(chg, add(st.0, bytes)?)?);
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
        keys.iter()
            .zip(reqs)
            .for_each(|(k, req)| l.get_mut(k).expect("scope").0 = req);
        Ok(SpaceReservation(root, dev, bytes))
    }

    fn count(path: &Path, depth: usize, limit: usize, out: &mut (u64, u64)) -> Result<()> {
        if depth == 0 {
            return Err(corrupt("native file inventory exceeds layout depth"));
        }
        let entries = match std::fs::read_dir(path) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            r => r?,
        };
        for entry in entries {
            let p = entry?.path();
            let m = match std::fs::symlink_metadata(&p) {
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                r => r?,
            };
            if m.file_type().is_symlink() {
                return Err(corrupt("native inventory refuses symlinks"));
            } else if m.is_dir() {
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

    fn release(root: &Path, dev: u64, bytes: u64) {
        if let Ok(mut l) = lock_ledger() {
            for k in [(Some(root.to_path_buf()), 0), (None, dev)] {
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
