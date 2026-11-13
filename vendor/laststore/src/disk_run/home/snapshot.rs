//! Hold one published CURRENT and the immutable runs it references.

use super::{open_nofollow, RunHome, RunRoot, CURRENT, RUNS, RUN_HOME_MARKER};
use crate::{Error, Result};
use std::fs::File;
use std::path::PathBuf;

/// A held run directory publication and the exact files it references.
pub struct RunSnapshot {
    root: RunRoot,
    descriptor: File,
    current: File,
}

impl RunHome {
    /// Hold the current publication across subsequent writes and compactions.
    pub fn snapshot(&self) -> Result<RunSnapshot> {
        let _publication = self.publish_lock()?;
        self.refuse_if_poisoned()?;
        Ok(RunSnapshot {
            root: self.current_root(),
            descriptor: open_nofollow(&self.path().join(RUN_HOME_MARKER))?,
            current: open_nofollow(&self.path().join(CURRENT))?,
        })
    }
}

impl RunSnapshot {
    /// List relative paths and byte lengths in the held publication.
    pub fn files(&self) -> Result<Vec<(PathBuf, u64)>> {
        let mut files = vec![
            (RUN_HOME_MARKER.into(), self.descriptor.metadata()?.len()),
            (CURRENT.into(), self.current.metadata()?.len()),
        ];
        for run in self.root.0.newest_first() {
            let name = run
                .path
                .file_name()
                .ok_or_else(|| Error::Config("snapshot run name".into()))?;
            files.push((PathBuf::from(RUNS).join(name), run.meta.file_bytes));
        }
        Ok(files)
    }

    /// Open a file belonging to the held publication.
    pub fn open_file(&self, path: &std::path::Path) -> Result<File> {
        if path == std::path::Path::new(RUN_HOME_MARKER) {
            return Ok(self.descriptor.try_clone()?);
        }
        if path == std::path::Path::new(CURRENT) {
            return Ok(self.current.try_clone()?);
        }
        let Some(name) = path.strip_prefix(RUNS).ok().and_then(|path| {
            (path.components().count() == 1)
                .then(|| path.file_name())
                .flatten()
        }) else {
            return Err(Error::Config("snapshot run name".into()));
        };
        let run = self
            .root
            .0
            .newest_first()
            .find(|run| run.path.file_name() == Some(name))
            .ok_or_else(|| Error::Config("snapshot run absent".into()))?;
        Ok(open_nofollow(&run.path)?)
    }
}
