//! Direct immutable snapshot commits. CURRENT is the only recovery authority.

mod admission;
mod maintenance;
mod quote;
mod reclaim;
mod writer;

pub use admission::{SnapshotPressure, SnapshotReservation};
pub use quote::{SnapshotMutationShape, SnapshotQuote};
pub use reclaim::ReclaimPlan;
pub use writer::SnapshotWriter;

/// One complete stored put or delete; body encryption remains opaque.
pub struct SnapshotMutation {
    /// Exact collection identity.
    pub collection: String,
    /// Group selected by the held placement codec.
    pub group: u16,
    /// Complete encoded storage key.
    pub key: String,
    /// Stored body, or None for a versioned tombstone.
    pub body: Option<Vec<u8>>,
}

pub(super) struct PreparedMutation {
    pub collection: String,
    pub group: u16,
    pub key: String,
    pub location: super::Location,
}

const MAX_BATCH_BYTES: u64 = 128 * 1024 * 1024;
const MAX_PENDING_BYTES: u64 = 512 * 1024 * 1024;
const MAX_PENDING_BATCHES: u64 = 4096;

fn refuse(reason: &'static str) -> crate::Error {
    crate::Error::Config(format!("disk-pack snapshot: {reason}"))
}

fn capacity(reason: &'static str) -> crate::Error {
    crate::Error::WriteCapacity(reason.into())
}
