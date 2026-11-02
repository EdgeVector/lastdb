//! The key format marker, the delete replay floor and the field history
//! setting of a node home.
//!
//! Home creation writes the marker. Startup, offline maintenance and the
//! restore proof read the marker and the floor with the same strict check.

use crate::storage::error::StorageResult;
use crate::storage::StorageError;
use serde::{Deserialize, Serialize};

/// This logical format includes framed field key identities and exact
/// tuple-based binding sources. Snapshots require the same marker key.
pub(crate) const CURRENT_KEY_FORMAT_KEY: &str = "layout:keys";

/// The durable delete replay floor of a home.
pub(crate) const DELETE_REPLAY_CUTOFF_KEY: &str = "layout:delete-replay-cutoff";

/// The field history setting of a home. The first field history policy
/// that is not off writes [`FIELD_HISTORY_ON`]; nothing turns it off.
pub(crate) const FIELD_HISTORY_KEY: &str = "layout:field-history";

/// The value of [`FIELD_HISTORY_KEY`] in a home that keeps field history.
pub(crate) const FIELD_HISTORY_ON: &[u8] = b"on";

/// The stored value of [`DELETE_REPLAY_CUTOFF_KEY`].
#[derive(Serialize, Deserialize)]
pub(crate) struct Cutoff {
    pub(crate) v: u8,
    pub(crate) minimum_written_at_nanos: u64,
}

fn invalid(text: impl std::fmt::Display) -> StorageError {
    StorageError::InvalidOperation(format!("delete replay cutoff: {text}"))
}

/// The same strict marker/floor check for a caller-held offline snapshot.
pub(crate) fn validate_frozen_replay_floor(
    marker: Option<&[u8]>,
    cutoff: Option<&[u8]>,
) -> StorageResult<u64> {
    if !marker.is_some_and(lastdb_model::field_key_codec::current_key_format) {
        return Err(invalid("the format marker is not the node format"));
    }
    let Some(bytes) = cutoff else {
        return Ok(0);
    };
    let floor: Cutoff = serde_json::from_slice(bytes).map_err(invalid)?;
    if floor.v != 1 || floor.minimum_written_at_nanos == 0 {
        return Err(invalid("invalid floor record"));
    }
    Ok(floor.minimum_written_at_nanos)
}
