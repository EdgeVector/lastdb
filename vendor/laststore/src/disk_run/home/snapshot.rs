//! Pinned published roots (`RunRoot`) and file-handle snapshots (`RunSnapshot`).

use super::manifest::Manifest;
use super::read::{ReadBudget, RunRead};
use super::{invalid, open_nofollow, RunHome, CURRENT, RUNS, RUN_HOME_MARKER};
use crate::Result;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// One published state of a home: a handle to the CURRENT that a commit or a merge installed.
#[derive(Debug, Clone)]
pub struct RunRoot(pub(super) Arc<Manifest>);

impl RunRoot {
    /// CURRENT generation of this state.
    pub fn generation(&self) -> u64 {
        self.0.generation
    }

    /// Complete commits in this state.
    pub fn frontier(&self) -> u64 {
        self.0.frontier
    }
}

/// A held run directory publication and the exact files it references.
pub struct RunSnapshot {
    root: RunRoot,
    descriptor: File,
    current: File,
}

impl RunHome {
    /// The state that the latest publish installed.
    pub fn current_root(&self) -> RunRoot {
        RunRoot(self.held())
    }

    /// One read call against exactly `root`, even when newer states exist.
    pub fn read_root(&self, root: &RunRoot, budget: ReadBudget) -> RunRead {
        RunRead::build(
            Arc::clone(&root.0),
            self.key,
            Arc::clone(&self.lock),
            budget,
            None,
        )
        .expect("unbounded read root")
    }

    /// Hold the current publication across subsequent writes and compactions.
    pub fn snapshot(&self) -> Result<RunSnapshot> {
        let _publish = self.publish_lock()?;
        self.refuse_if_poisoned()?;
        let descriptor = open_nofollow(&self.path().join(RUN_HOME_MARKER))?;
        let current = open_nofollow(&self.path().join(CURRENT))?;
        Ok(RunSnapshot {
            root: self.current_root(),
            descriptor,
            current,
        })
    }
}

impl RunSnapshot {
    /// List relative paths and byte lengths in the held publication.
    pub fn files(&self) -> Result<Vec<(PathBuf, u64)>> {
        let mut out = vec![
            (RUN_HOME_MARKER.into(), self.descriptor.metadata()?.len()),
            (CURRENT.into(), self.current.metadata()?.len()),
        ];
        for run in self.root.0.newest_first() {
            let name = run
                .path
                .file_name()
                .ok_or_else(|| invalid("snapshot run name"))?;
            out.push((PathBuf::from(RUNS).join(name), run.meta.file_bytes));
        }
        Ok(out)
    }

    /// Open a file belonging to the held publication.
    pub fn open_file(&self, path: &Path) -> Result<File> {
        if path == Path::new(RUN_HOME_MARKER) {
            return Ok(self.descriptor.try_clone()?);
        }
        if path == Path::new(CURRENT) {
            return Ok(self.current.try_clone()?);
        }
        let name = path
            .strip_prefix(RUNS)
            .ok()
            .filter(|p| p.components().count() == 1)
            .and_then(Path::file_name)
            .ok_or_else(|| invalid("snapshot run name"))?;
        let run = self
            .root
            .0
            .newest_first()
            .find(|r| r.path.file_name() == Some(name))
            .ok_or_else(|| invalid("snapshot run absent"))?;
        Ok(open_nofollow(&run.path)?)
    }
}
