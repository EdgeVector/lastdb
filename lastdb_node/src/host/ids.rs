//! Process id adapter for the daemon shell.
//!
//! Process entry constructs one [`SystemIds`] and passes it into [`super::Host::boot`].
//! There is no global id source.

use lastdb_host::IdGen;
use uuid::Uuid;

/// Operating-system randomness. Construct this at the daemon entrypoint.
#[derive(Clone, Copy, Debug)]
pub struct SystemIds;

impl IdGen for SystemIds {
    fn fresh_uuid_v4(&self) -> Uuid {
        Uuid::new_v4()
    }
}
