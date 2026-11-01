//! Opt-in authored record outcomes share the data's encrypted native commit.
//! This stream has no recovery authority, reader dependency, pruning, or cloud worker.

use super::*;
use serde::de::DeserializeOwned;
use serde::Serialize;

mod adoption;
mod bounded;
mod export;
mod identity;
mod model;
mod peer;
mod reader;
mod record_outcome;
mod snapshot;
pub(crate) mod snapshot_identity;
mod store;
mod verify;

pub(crate) use adoption::plan as snapshot_adoption_plan;
pub use adoption::{
    SnapshotAdoptionCandidate, SnapshotAdoptionOrigin, SnapshotAdoptionPlan,
    SnapshotAdoptionPosition, SnapshotAdoptionPrefix, SnapshotAdoptionReplay,
    SnapshotAdoptionReplaySource, SnapshotAdoptionRequirement,
};
pub(crate) use bounded::preflight_outcome_size;
pub use model::{CaptureEvent, CaptureStatus, EventMetadata, Position, RetiredFieldOutcome};
pub use peer::{CaptureExportPage, ImportReceipt, PeerPosition};
pub use record_outcome::{AcceptedRecordOutcome, RecordFieldOutcome};
pub(crate) use snapshot::{verify_snapshot_inclusion, SNAPSHOT_INCLUSION_WORK_BYTES};
pub use snapshot::{SnapshotDevicePosition, SnapshotInclusion, SnapshotInclusionMap};
pub use snapshot_identity::{
    SnapshotIdentityPeer, SnapshotIdentityPosition, SnapshotIdentityReport,
};
pub(super) use store::{open, Capture};

pub(super) const COLLECTION: &str = "logical_sync_v1";
const HEAD: &[u8] = b"head";
const MAX_EVENT_BYTES: usize = 1024 * 1024;
const MAX_MUTATIONS: usize = 128;
const MAX_OUTCOMES: usize = 1024;
const PAGE_EVENTS: usize = 4;
const PAGE_ENVELOPE_BYTES: usize = 8192;
const BACKLOG_CAP_BYTES: u64 = 64 * 1024 * 1024;
const RECENT_ENTRIES: u64 = 16;

fn key(prefix: &str, sequence: u64) -> String {
    native_routing::encode(format!("{prefix}\0{sequence:020}").as_bytes())
}

fn head_key() -> String {
    native_routing::encode(HEAD)
}

fn encode<T: Serialize>(value: &T) -> StorageResult<Vec<u8>> {
    bounded::serialized_size(value)?;
    // Device outcomes contain nanosecond clocks and exact user integers. Keep
    // their scalar representation; the cloud RFC8785 encoder rejects >2^53-1.
    let value = serde_json::to_value(value)
        .map_err(|_| invalid("cannot encode logical capture metadata"))?;
    serde_json::to_vec(&lastdb_model::canonical::CanonicalValue(&value))
        .map_err(|_| invalid("cannot encode logical capture metadata"))
}

fn decode<T: DeserializeOwned>(bytes: &[u8]) -> StorageResult<T> {
    serde_json::from_slice(bytes).map_err(|_| invalid("invalid logical capture metadata"))
}

fn invalid(message: &str) -> StorageError {
    StorageError::InvalidOperation(message.into())
}

fn hex(bytes: &[u8]) -> String {
    lastdb_model::hex::hex_lower(bytes)
}

fn unhex<const N: usize>(text: &str) -> StorageResult<[u8; N]> {
    if text.len() != N * 2
        || !text
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(invalid("invalid logical capture hexadecimal value"));
    }
    let mut out = [0u8; N];
    for (index, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&text[index * 2..index * 2 + 2], 16)
            .map_err(|_| invalid("invalid logical capture hexadecimal value"))?;
    }
    Ok(out)
}
