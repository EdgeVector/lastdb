//! Process id adapter for schema service shells.
//!
//! Each process entry constructs one [`SystemIds`] and passes it into
//! `SchemaServiceState`. There is no global id source.

use schema_service_core::IdGen;
use uuid::Uuid;

/// Operating-system randomness. Construct this at the shell entrypoint.
#[derive(Clone, Copy, Debug)]
pub struct SystemIds;

impl IdGen for SystemIds {
    fn fresh_uuid_v4(&self) -> Uuid {
        Uuid::new_v4()
    }
}
