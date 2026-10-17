//! Physical reservation and stored-cache invalidation for one complete commit.

use super::*;
use std::collections::BTreeSet;

pub(super) struct WriteProtection {
    logical: Arc<Mutex<LogicalResidentSet>>,
    keys: BTreeSet<(String, String)>,
    space: Mutex<Option<laststore::disk_pack::maintenance::SpaceReservation>>,
}

impl WriteProtection {
    /// Stored coordinates do not consume canonical logical path capacity.
    pub(super) fn reserve(runtime: &PackRuntime, operations: &[HomeMutation]) -> Self {
        Self {
            logical: Arc::clone(&runtime.logical),
            keys: operations
                .iter()
                .map(|operation| (operation.collection.clone(), operation.key.clone()))
                .collect(),
            space: Mutex::new(None),
        }
    }

    pub(super) fn with_space(
        self,
        space: laststore::disk_pack::maintenance::SpaceReservation,
    ) -> Self {
        *self.space.lock().expect("native physical reservation") = Some(space);
        self
    }

    pub(super) fn release_space(&self) {
        self.space
            .lock()
            .expect("native physical reservation")
            .take();
    }

    /// Invalidate the old fetched bytes while this owner still protects every key.
    pub(super) fn invalidate(&self) {
        let mut logical = self
            .logical
            .lock()
            .expect("native logical write protection");
        for (collection, key) in &self.keys {
            logical.forget_record(collection, key);
        }
    }
}
