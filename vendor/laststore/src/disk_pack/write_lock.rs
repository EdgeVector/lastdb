use super::artifact;
use crate::Result;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};

/// A typed proof of exclusive artifact write ownership. No caller can forge it.
pub(super) struct WriteLock {
    root: PathBuf,
    _file: File,
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
        Ok(Self { root, _file: file })
    }

    pub(super) fn root(&self) -> &Path {
        &self.root
    }
}
