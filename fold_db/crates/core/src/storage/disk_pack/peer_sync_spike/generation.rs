//! ACTIVE selects one complete paired native generation, never two independent roots.

use super::control::{private_directory, read_private, write_new, RootControl};
use super::*;
use std::fs::{self, File};
use std::os::unix::fs::DirBuilderExt;
use std::path::PathBuf;
use std::sync::atomic::Ordering;

impl RootControl {
    pub(super) fn active(&self) -> StorageResult<Option<String>> {
        let path = self.root.join("ACTIVE");
        match fs::symlink_metadata(&path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
            Ok(_) => {}
        }
        let bytes = read_private(&path, 37)?;
        let value = std::str::from_utf8(&bytes[..36])
            .map_err(|_| invalid("invalid active prototype generation"))?;
        if bytes[36] != b'\n' {
            return Err(invalid("invalid prototype selector terminator"));
        }
        private_directory(&self.generation(value)?)?;
        Ok(Some(value.to_string()))
    }

    pub(super) fn generation(&self, value: &str) -> StorageResult<PathBuf> {
        let id = uuid::Uuid::parse_str(value).map_err(|_| invalid("invalid generation UUID"))?;
        if id.is_nil() || id.to_string() != value {
            return Err(invalid("prototype generation UUID is not canonical"));
        }
        let parent = self.root.join("generations");
        private_directory(&parent)?;
        let path = parent.join(value);
        if path.try_exists()? {
            private_directory(&path)?;
            if path.canonicalize()? != path {
                return Err(invalid("generation path is not canonical"));
            }
        }
        Ok(path)
    }

    pub(super) fn new_generation(&self) -> StorageResult<String> {
        if fs::read_dir(self.root.join("generations"))?
            .take(129)
            .count()
            >= 128
        {
            return Err(invalid("prototype retains at most 128 generations"));
        }
        let value = uuid::Uuid::new_v4().to_string();
        let path = self.generation(&value)?;
        if path.try_exists()? {
            return Err(invalid("prototype generation already exists"));
        }
        fs::DirBuilder::new().mode(0o700).create(&path)?;
        File::open(self.root.join("generations"))?.sync_all()?;
        Ok(value)
    }

    pub(super) fn select(&self, value: &str) -> StorageResult<()> {
        let generation = self.generation(value)?;
        private_directory(&generation)?;
        // The caller flushed the native stores; order their entry before ACTIVE.
        File::open(&generation)?.sync_all()?;
        File::open(self.root.join("generations"))?.sync_all()?;
        let temporary = self
            .root
            .join(format!(".ACTIVE.{}.tmp", uuid::Uuid::new_v4()));
        write_new(&temporary, format!("{value}\n").as_bytes())?;
        fs::rename(&temporary, self.root.join("ACTIVE"))?;
        if let Err(error) = File::open(&self.root).and_then(|directory| directory.sync_all()) {
            self.failed.store(true, Ordering::Release);
            return Err(error.into());
        }
        Ok(())
    }
}
