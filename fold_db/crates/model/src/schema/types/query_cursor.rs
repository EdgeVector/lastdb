//! Cursor metadata travels on the wire; it adds no stored copy of a range key.

use super::KeyValue;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct QueryCursor {
    pub hash: Option<String>,
    pub range: Option<String>,
    /// Missing only in an older client's cursor; a schema read refuses it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comparison: Option<String>,
    /// The record key position (base64url) where an unfiltered record walk
    /// page ended, in place of an API key: a page that ends in a run of
    /// deleted records has no API key to name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub record: Option<String>,
}

impl QueryCursor {
    #[must_use]
    pub fn new(key: KeyValue, comparison: Option<String>) -> Self {
        Self {
            hash: key.hash,
            range: key.range,
            comparison,
            record: None,
        }
    }

    /// A record walk cursor at one record key position: the record key
    /// suffix, sent as base64url.
    #[must_use]
    pub fn at_record(suffix: &str, comparison: Option<String>) -> Self {
        Self {
            hash: None,
            range: None,
            comparison,
            record: Some(URL_SAFE_NO_PAD.encode(suffix)),
        }
    }

    /// The record key suffix of a record walk cursor, or `None` for a key
    /// cursor.
    ///
    /// # Errors
    /// A record position that is not base64url text.
    pub fn record_position(&self) -> Result<Option<String>, String> {
        let Some(record) = self.record.as_deref() else {
            return Ok(None);
        };
        let bytes = URL_SAFE_NO_PAD
            .decode(record)
            .map_err(|_| "the record cursor is not base64url".to_string())?;
        String::from_utf8(bytes)
            .map(Some)
            .map_err(|_| "the record cursor is not a record key".to_string())
    }

    #[must_use]
    pub fn into_key(self) -> KeyValue {
        KeyValue::new(self.hash, self.range)
    }
}
