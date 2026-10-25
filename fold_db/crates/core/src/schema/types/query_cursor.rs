//! Cursor metadata travels on the wire; it adds no stored copy of a range key.

use super::KeyValue;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct QueryCursor {
    pub hash: Option<String>,
    pub range: Option<String>,
    /// Missing only for an older client's legacy UTF-8 continuation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comparison: Option<String>,
}

impl QueryCursor {
    #[must_use]
    pub fn new(key: KeyValue, comparison: Option<String>) -> Self {
        Self {
            hash: key.hash,
            range: key.range,
            comparison,
        }
    }

    #[must_use]
    pub fn into_key(self) -> KeyValue {
        KeyValue::new(self.hash, self.range)
    }
}
