//! The record cap (decision D3) on the records that a native request stages.
//!
//! A record above `MAX_RECORD_BYTES` is refused with the typed
//! `SchemaError::RecordTooLarge`, which the HTTP layer renders as the
//! `413 field_value_too_large` body. A request that writes one key in two
//! groups stages the earlier record without the cap
//! (`RecordChange::cap_deferred`): only the final record of the request must
//! fit, and `require_records_fit` checks it after the last group.

use super::records::RecordChange;
use super::RecordStore;
use crate::schema::SchemaError;
use lastdb_model::record_value::{RecordValueError, MAX_RECORD_BYTES};

/// The typed error of a record value that the store refuses.
fn record_value_error(error: RecordValueError) -> SchemaError {
    match error {
        RecordValueError::TooLarge { bytes } => SchemaError::RecordTooLarge {
            size: bytes,
            limit: MAX_RECORD_BYTES,
        },
        other => SchemaError::InvalidData(format!("the record write is refused: {other}")),
    }
}

/// The stored bytes of one change. A deferred change skips the cap here.
pub(super) fn record_bytes(change: &RecordChange) -> Result<Vec<u8>, SchemaError> {
    let encoded = if change.cap_deferred {
        change.after.encode_uncapped()
    } else {
        change.after.encode()
    };
    encoded.map_err(record_value_error)
}

impl RecordStore {
    /// Check the cap on the final records at these storage keys. One read by
    /// key in the native request, which sees the records that it staged.
    pub(crate) async fn require_records_fit(&self, keys: &[String]) -> Result<(), SchemaError> {
        if keys.is_empty() {
            return Ok(());
        }
        for record in self.read_records(keys).await?.into_iter().flatten() {
            record.encode().map_err(record_value_error)?;
        }
        Ok(())
    }
}
