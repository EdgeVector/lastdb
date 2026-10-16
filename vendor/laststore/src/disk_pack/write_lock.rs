use super::{artifact, Current};
use crate::Result;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// A typed proof of exclusive artifact write ownership. No caller can forge it.
pub(super) struct WriteLock {
    root: PathBuf,
    _file: File,
    checked_current: Mutex<Option<[u8; 32]>>,
}

impl WriteLock {
    pub(super) fn acquire(root: &Path) -> Result<Self> {
        Self::acquire_inner(root, None)
    }

    pub(super) fn acquire_conversion(root: &Path, token: uuid::Uuid) -> Result<Self> {
        Self::acquire_inner(root, Some(token))
    }

    fn acquire_inner(root: &Path, conversion: Option<uuid::Uuid>) -> Result<Self> {
        let root = std::fs::canonicalize(root)?;
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(root.join("journal").join("LOCK"))?;
        rustix::fs::flock(&file, rustix::fs::FlockOperation::NonBlockingLockExclusive)
            .map_err(std::io::Error::from)?;
        if root.join(super::conversion::REVERSE_FENCE).try_exists()? {
            return Err(crate::Error::Config(
                "native source is sealed after reverse conversion".into(),
            ));
        }
        if let Some(token) = conversion {
            super::conversion_fence::validate(&root, token)?;
        } else {
            super::conversion_fence::refuse_pending(&root)?;
        }
        let current = artifact::current(&root)?;
        if current.required_features & super::FEATURE_OBJECT_METADATA == 0 {
            return Err(crate::Error::Config(
                "disk-pack writes require complete object bounds".into(),
            ));
        }
        Ok(Self {
            root,
            _file: file,
            checked_current: Mutex::new(None),
        })
    }

    pub(super) fn root(&self) -> &Path {
        &self.root
    }

    /// This lease retains only a complete-root digest, never pages or part metadata.
    pub(super) fn checked(&self, current: &Current) -> Result<bool> {
        let checked = self
            .checked_current
            .lock()
            .map_err(|_| super::corrupt("verified writer root lock is poisoned"))?;
        Ok(*checked == Some(super::bytes::digest(&current.encode()?)))
    }

    /// Call only after complete dependency verification or successful root barriers.
    pub(super) fn remember_checked(&self, current: &Current) -> Result<()> {
        let mut checked = self
            .checked_current
            .lock()
            .map_err(|_| super::corrupt("verified writer root lock is poisoned"))?;
        *checked = Some(super::bytes::digest(&current.encode()?));
        Ok(())
    }
}
