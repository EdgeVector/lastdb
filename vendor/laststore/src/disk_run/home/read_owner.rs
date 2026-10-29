//! Retain exclusive ownership without retaining readers, or promote one held root.

use super::{read_descriptor, RunHome, RunOptions, RunRead};
use crate::{Error, Result};
use std::fs::{File, OpenOptions};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex, RwLock};

/// Keeps the existing exclusive owner lock after all manifest/read memory drops.
/// The file is deliberately private: a caller cannot unlock a different owner.
pub struct RunReadOwner {
    _lock: Arc<File>,
}

impl RunRead {
    /// Close the read and its metadata while retaining the exclusive owner lock.
    pub fn into_owner_guard(self) -> RunReadOwner {
        RunReadOwner {
            _lock: self._owner_lock,
        }
    }

    /// Consume an exclusively held existing root without an unlock/reopen gap.
    /// No initialization, file cleanup, merge, or publication occurs here.
    pub fn into_existing_writer(self, root: &Path) -> Result<RunHome> {
        if Arc::strong_count(&self._owner_lock) != 1 {
            return Err(Error::Config(
                "read promotion requires exclusive ownership".into(),
            ));
        }
        if root.canonicalize()? != root {
            return Err(Error::Config("read promotion refuses a path alias".into()));
        }
        let lock = OpenOptions::new()
            .read(true)
            .custom_flags(libc_flags())
            .open(root.join("LOCK"))?;
        let held = self._owner_lock.metadata()?;
        let current = lock.metadata()?;
        if !current.is_file()
            || current.nlink() != 1
            || (held.dev(), held.ino(), held.uid(), held.mode(), held.len())
                != (
                    current.dev(),
                    current.ino(),
                    current.uid(),
                    current.mode(),
                    current.len(),
                )
            || read_descriptor(root)? != self.home_id()
        {
            return Err(Error::Config("read promotion home binding differs".into()));
        }
        Ok(RunHome {
            root: root.to_path_buf(),
            runs: root.join("runs"),
            key: self.key,
            identity: self.home_id(),
            options: RunOptions::default(),
            current: RwLock::new(self.manifest),
            commit: Mutex::new(()),
            publish: Mutex::new(()),
            merge: Mutex::new(()),
            retired: Mutex::new(Vec::new()),
            poisoned: AtomicBool::new(false),
            lock: self._owner_lock,
        })
    }
}

fn libc_flags() -> i32 {
    (rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::NONBLOCK).bits() as i32
}
