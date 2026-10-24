//! Metadata-only boundary reads. Neither path opens or initializes a writer.
use super::{Current, DiskHome};
use crate::disk_pack::{artifact, corrupt, lifetime::GenerationHold};
use crate::Result;
use std::fs::File;
use std::os::unix::fs::MetadataExt;

impl DiskHome {
    /// Capture the actual durable CURRENT while deletion is excluded.
    pub fn current_boundary(&self) -> Result<Current> {
        let (current, _hold) = GenerationHold::capture(self.path())?;
        self.validate_current(&current)?;
        Ok(current)
    }

    /// Observe a stopped home under its existing exclusive publication fence.
    /// No lock file, writer, cleanup, or database state is created. The caller
    /// must keep its supervisor inhibited after this observation returns.
    pub fn stopped_boundary(&self) -> Result<Current> {
        let flags = rustix::fs::OFlags::RDONLY
            | rustix::fs::OFlags::NOFOLLOW
            | rustix::fs::OFlags::NONBLOCK
            | rustix::fs::OFlags::CLOEXEC;
        let file = File::from(
            rustix::fs::open(
                self.path().join("SNAPSHOT_LOCK").as_path(),
                flags,
                rustix::fs::Mode::empty(),
            )
            .map_err(std::io::Error::from)?,
        );
        let held = file.metadata()?;
        if !held.is_file()
            || held.nlink() != 1
            || held.uid() != std::fs::metadata(self.path())?.uid()
        {
            return Err(corrupt("stopped boundary requires existing writer fence"));
        }
        rustix::fs::flock(&file, rustix::fs::FlockOperation::NonBlockingLockExclusive)
            .map_err(std::io::Error::from)?;
        let current = artifact::current(self.path())?;
        self.validate_current(&current)?;
        let selected = std::fs::symlink_metadata(self.path().join("SNAPSHOT_LOCK"))?;
        if selected.dev() != held.dev() || selected.ino() != held.ino() {
            return Err(corrupt("stopped boundary writer fence changed"));
        }
        Ok(current)
    }
}
