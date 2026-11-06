//! On-disk shape of a legacy field-history entry.

use serde::Deserialize;

/// The family of a field history entry of a home.
pub(crate) const RECORD_HISTORY_FAMILY: &str = "rh";

pub(crate) fn record_history_suffix(key: &str) -> Option<&str> {
    key.strip_prefix(RECORD_HISTORY_FAMILY)?.strip_prefix(':')
}

/// One field history entry of a home (decision D5): its own copy of
/// one old field value and the stamp of that value. The key is
/// `rh:{S}:{sha(record key)}:{field identity}:{id}`.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RecordHistoryEntry {
    pub v: u8,
    #[serde(rename = "schema")]
    _schema: String,
    #[serde(rename = "field")]
    _field: String,
    pub field_identity: String,
    /// The full storage key of the record.
    pub record: String,
    /// The copy of the old value.
    #[serde(rename = "value")]
    _value: serde_json::Value,
    /// The stamp of the old value.
    pub stamp: lastdb_model::record_value::RecordStamp,
    #[serde(rename = "archived_at")]
    _archived_at: u64,
    pub expires_at: Option<u64>,
    /// `replace` or `delete`.
    #[serde(rename = "reason")]
    _reason: String,
    pub queue_key: Option<String>,
}

impl RecordHistoryEntry {
    pub(crate) const VERSION: u8 = 1;

    pub(crate) fn expired(&self, now: u64) -> bool {
        self.expires_at.is_some_and(|at| at <= now)
    }
}
