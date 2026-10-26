//! Identity of one mutation-log segment object.
//!
//! Pin-log replay compares the stored object key with [`MutationLogSegmentId::expected_object_key`].
//! The unused snapshot frontier, CAS payload, and log GC helpers are removed.

use serde::{Deserialize, Serialize};

/// Identity of one append-only mutation log segment object.
///
/// Legacy objects use flat or writer-scoped paths. Typed mutation objects use
/// `log/{writer_id}/{schema}/{utc_nanos}_{sequence}.enc`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MutationLogSegmentId {
    /// Optional publisher/device id for multi-writer streams.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub writer_id: Option<String>,
    /// Declared catalog schema for schema-folder mutation logs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_name: Option<String>,
    /// T0 UTC nanosecond stamp carried in the object name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub utc_nanos: Option<u64>,
    /// Sequence in this writer-schema stream.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sequence: Option<u64>,
    /// Monotonic id within the stream (seq / CSN / log object id).
    pub through_id: u64,
    /// Object key relative to the account/org prefix (informational / routing).
    pub object_key: String,
}

impl MutationLogSegmentId {
    /// Writer-scoped legacy path. Flat `log/{through_id}.enc` is also readable.
    fn default_object_key(writer_id: Option<&str>, through_id: u64) -> String {
        match writer_id {
            Some(w) if !w.is_empty() => format!("log/{w}/{through_id}.enc"),
            _ => format!("log/{through_id}.enc"),
        }
    }

    fn schema_object_key(
        writer_id: &str,
        schema_name: &str,
        utc_nanos: u64,
        sequence: u64,
    ) -> String {
        format!("log/{writer_id}/{schema_name}/{utc_nanos:019}_{sequence}.enc")
    }

    pub fn expected_object_key(&self) -> String {
        match (
            self.writer_id.as_deref(),
            self.schema_name.as_deref(),
            self.utc_nanos,
            self.sequence,
        ) {
            (Some(writer), Some(schema), Some(utc_nanos), Some(sequence)) => {
                Self::schema_object_key(writer, schema, utc_nanos, sequence)
            }
            (writer, _, _, _) => Self::default_object_key(writer, self.through_id),
        }
    }
}
