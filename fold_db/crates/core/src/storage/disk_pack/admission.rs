//! Physical space ownership for one complete commit.

use super::*;

pub(super) struct WriteProtection {
    space: Mutex<Option<laststore::disk_pack::maintenance::SpaceReservation>>,
}

impl WriteProtection {
    /// Cache invalidation belongs to atomic read-view publication, not the flush.
    pub(super) fn reserve(space: laststore::disk_pack::maintenance::SpaceReservation) -> Self {
        Self {
            space: Mutex::new(Some(space)),
        }
    }

    pub(super) fn release_space(&self) {
        self.space
            .lock()
            .expect("native physical reservation")
            .take();
    }
}
