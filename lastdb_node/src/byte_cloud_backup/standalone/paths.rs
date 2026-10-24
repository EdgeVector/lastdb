//! Exact external state directories; never place checkpoints in the source home.
use super::super::{BackupError, Result};
use std::fs::{self, File};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

pub(super) struct Paths {
    pub root: PathBuf,
    pub coordinator: PathBuf,
    pub source: PathBuf,
    directory: Option<File>,
}

impl Paths {
    pub fn open(source_home: &Path, state: &Path, create: bool) -> Result<Self> {
        if !state.is_absolute() {
            return Err(BackupError::Identity);
        }
        let parent = state.parent().ok_or(BackupError::Invalid)?.canonicalize()?;
        let root = parent.join(state.file_name().ok_or(BackupError::Invalid)?);
        if root.starts_with(source_home) || source_home.starts_with(&root) {
            return Err(BackupError::Identity);
        }
        private_directory(&parent)?;
        if create && !root.try_exists()? {
            fs::DirBuilder::new().mode(0o700).create(&root)?;
            File::open(&parent)?.sync_all()?;
        }
        let directory = if root.try_exists()? {
            private_directory(&root)?;
            Some(
                fs::OpenOptions::new()
                    .read(true)
                    .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
                    .open(&root)?,
            )
        } else {
            None
        };
        let value = Self {
            coordinator: root.join("coordinator"),
            source: root.join("source"),
            root,
            directory,
        };
        value.check()?;
        Ok(value)
    }

    pub fn check(&self) -> Result<()> {
        if let Some(directory) = &self.directory {
            let held = directory.metadata()?;
            let current = fs::symlink_metadata(&self.root)?;
            if !current.is_dir() || held.dev() != current.dev() || held.ino() != current.ino() {
                return Err(BackupError::Identity);
            }
        } else if self.root.try_exists()? {
            return Err(BackupError::Identity);
        }
        Ok(())
    }
}

pub(super) fn private_directory(path: &Path) -> Result<()> {
    let info = fs::symlink_metadata(path)?;
    if !info.is_dir() || info.uid() != unsafe { libc::geteuid() } || info.mode() & 0o077 != 0 {
        return Err(BackupError::Identity);
    }
    Ok(())
}
