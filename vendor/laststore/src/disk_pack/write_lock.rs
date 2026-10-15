use super::{artifact, Current};
use crate::Result;
use std::cell::Cell;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};

/// A typed proof of exclusive artifact write ownership. No caller can forge it.
pub(super) struct WriteLock {
    root: PathBuf,
    _file: File,
    checked_current: Cell<Option<[u8; 32]>>,
}

impl WriteLock {
    pub(super) fn acquire(root: &Path) -> Result<Self> {
        let root = std::fs::canonicalize(root)?;
        let current = artifact::current(&root)?;
        if current.required_features & super::FEATURE_OBJECT_METADATA == 0 {
            return Err(crate::Error::Config(
                "disk-pack writes require complete object bounds".into(),
            ));
        }
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(root.join("journal").join("LOCK"))?;
        rustix::fs::flock(&file, rustix::fs::FlockOperation::NonBlockingLockExclusive)
            .map_err(std::io::Error::from)?;
        Ok(Self {
            root,
            _file: file,
            checked_current: Cell::new(None),
        })
    }

    pub(super) fn root(&self) -> &Path {
        &self.root
    }

    /// This lease retains only a complete-root digest, never pages or part metadata.
    pub(super) fn checked(&self, current: &Current) -> Result<bool> {
        Ok(self.checked_current.get() == Some(super::bytes::digest(&current.encode()?)))
    }

    /// Call only after complete dependency verification or successful root barriers.
    pub(super) fn remember_checked(&self, current: &Current) -> Result<()> {
        self.checked_current
            .set(Some(super::bytes::digest(&current.encode()?)));
        Ok(())
    }
}
