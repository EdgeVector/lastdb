//! The record cap (decision D3) on the records that a native request stages.
//!
//! A record above `MAX_RECORD_BYTES` is refused with the typed
//! `SchemaError::RecordTooLarge`, which the HTTP layer renders as the
//! `413 field_value_too_large` body. A request that writes one key in two
//! groups stages the earlier record without the cap
//! (`RecordChange::cap_deferred`): only the final record of the request must
//! fit, and `require_records_fit` checks it after the last group.

use super::RecordStore;
use crate::schema::SchemaError;
use crate::storage::format::records::require_record_fits;

impl RecordStore {
    /// Check the cap on the final records at these storage keys. One read by
    /// key in the native request, which sees the records that it staged.
    pub(crate) async fn require_records_fit(&self, keys: &[String]) -> Result<(), SchemaError> {
        if keys.is_empty() {
            return Ok(());
        }
        for record in self.read_records(keys).await?.into_iter().flatten() {
            require_record_fits(&record)?;
        }
        Ok(())
    }
}
