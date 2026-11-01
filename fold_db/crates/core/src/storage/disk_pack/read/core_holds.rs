//! Calls hold only their selected logical paths in the core model.
use super::super::*;
use crate::resident::{CorePath, CoreWarmSet};
use std::collections::{BTreeMap, BTreeSet};

pub(super) struct CoreCallHolds {
    core: Arc<Mutex<CoreWarmSet>>,
    paths: Mutex<BTreeSet<CorePath>>,
    values: Mutex<BTreeMap<usize, Arc<dyn std::any::Any + Send + Sync>>>,
}

impl CoreCallHolds {
    pub(super) fn new(core: Arc<Mutex<CoreWarmSet>>) -> Self {
        Self {
            core,
            paths: Mutex::new(BTreeSet::new()),
            values: Mutex::new(BTreeMap::new()),
        }
    }
}

impl Drop for CoreCallHolds {
    fn drop(&mut self) {
        // Release overwritten values before the final path purge measures
        // reclaimable bytes. Other calls retain their own owners and holds.
        self.values
            .get_mut()
            .unwrap_or_else(|error| error.into_inner())
            .clear();
        let paths = self
            .paths
            .get_mut()
            .unwrap_or_else(|error| error.into_inner());
        if let Ok(mut core) = self.core.lock() {
            core.release_paths(&paths.iter().cloned().collect::<Vec<_>>());
        }
    }
}
