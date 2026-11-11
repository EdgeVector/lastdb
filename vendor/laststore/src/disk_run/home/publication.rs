//! CURRENT publication.

use super::manifest::{Manifest, CURRENT, MAX_CURRENT_BYTES, MAX_LEVELS, MAX_LEVEL_RUNS};
use super::RunHome;
use crate::disk_run::space::MergeSpace;
use crate::durability::{sync_directory, sync_dirty_file};
use crate::frame::{encode_frame, min_encoded_len, FrameHeader};
use crate::{Error, Result};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use uuid::Uuid;

impl RunHome {
    /// Publish CURRENT, then its readable root.
    pub(super) fn install(&self, next: Manifest) -> Result<Arc<Manifest>> {
        self.install_with_space(next, None)
    }

    pub(super) fn install_merge(
        &self,
        next: Manifest,
        space: &MergeSpace,
    ) -> Result<Arc<Manifest>> {
        let installed = self.install_with_space(next, Some(space));
        if installed.is_ok() || self.is_poisoned() {
            // An uncertain rename can name every new run; reopening decides.
            space.keep_outputs();
        }
        installed
    }

    fn install_with_space(
        &self,
        next: Manifest,
        space: Option<&MergeSpace>,
    ) -> Result<Arc<Manifest>> {
        let staged = next.stage_with_space(&self.root, &self.key, space)?;
        self.publish_staged(next, &staged)
    }

    pub(super) fn publish_staged(&self, next: Manifest, staged: &Path) -> Result<Arc<Manifest>> {
        if let Err(error) = Manifest::replace(&self.root, staged) {
            // The rename may or may not have happened: stop all writes.
            self.poisoned.store(true, Ordering::Release);
            return Err(error);
        }
        let next = Arc::new(next);
        self.install_current(&next)?;
        Ok(next)
    }
}

impl Manifest {
    /// Seal this manifest into a synced temporary file. A failure leaves
    /// the old CURRENT in force; the length checks match the reader.
    pub(super) fn stage(&self, root: &Path, key: &[u8; 32]) -> Result<PathBuf> {
        self.stage_with_space(root, key, None)
    }

    fn stage_with_space(
        &self,
        root: &Path,
        key: &[u8; 32],
        space: Option<&MergeSpace>,
    ) -> Result<PathBuf> {
        self.check_shape()?;
        let payload = self.encode();
        if self.levels.len() > MAX_LEVELS
            || self.levels.iter().any(|level| level.len() > MAX_LEVEL_RUNS)
            || self.merging.len() > MAX_LEVEL_RUNS
            || self.base.len() > MAX_LEVEL_RUNS
            || payload.len() > MAX_CURRENT_BYTES - min_encoded_len()
        {
            return Err(Error::WriteCapacity(
                "disk run home: CURRENT would exceed its open limits; merge first".into(),
            ));
        }
        let header = FrameHeader {
            chunk_uuid: Uuid::new_v4(),
            shard: 0,
            start_csn: 0,
            counter: 0,
        };
        let frame = encode_frame(key, header, &payload)?;
        if let Some(space) = space {
            space.admit_current(frame.len() as u64)?;
        }
        // The generation names the temporary file; a fresh frame id keeps
        // a retried generation from reusing its encryption nonce.
        let tmp = root.join(format!(".{CURRENT}.{}.tmp", self.generation));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&tmp)?;
        let written = file
            .write_all(&frame)
            .map_err(Error::from)
            .and_then(|()| sync_dirty_file(&file));
        if let Err(error) = written {
            let _ = fs::remove_file(&tmp);
            return Err(error);
        }
        Ok(tmp)
    }

    /// Rename a staged CURRENT and sync its directory. An error means the
    /// durable CURRENT is unknown and the owner must stop.
    pub(super) fn replace(root: &Path, staged: &Path) -> Result<()> {
        fs::rename(staged, root.join(CURRENT))?;
        sync_directory(&File::open(root)?)
    }
}
