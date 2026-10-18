//! Calls hold only their selected logical paths in the core model.
use super::super::*;
use crate::resident::{CorePath, CoreWarmSet};
use std::collections::BTreeSet;

pub(super) struct CoreCallHolds {
    core: Arc<Mutex<CoreWarmSet>>,
    paths: Mutex<BTreeSet<CorePath>>,
}

impl CoreCallHolds {
    pub(super) fn new(core: Arc<Mutex<CoreWarmSet>>) -> Self {
        Self {
            core,
            paths: Mutex::new(BTreeSet::new()),
        }
    }

    pub(super) fn hold(&self, core: &mut CoreWarmSet, paths: &[CorePath]) -> StorageResult<()> {
        let mut held = self.paths.lock().expect("core call path holds");
        let missing: Vec<_> = paths
            .iter()
            .filter(|path| !held.contains(*path))
            .cloned()
            .collect();
        let reserved =
            core.reserve_paths(&missing)
                .map_err(|error| StorageError::ReadResourceLimit {
                    detail: error.to_string(),
                })?;
        held.extend(reserved);
        Ok(())
    }
}

impl Drop for CoreCallHolds {
    fn drop(&mut self) {
        let paths = self
            .paths
            .get_mut()
            .unwrap_or_else(|error| error.into_inner());
        if let Ok(mut core) = self.core.lock() {
            core.release_paths(&paths.iter().cloned().collect::<Vec<_>>());
        }
    }
}
