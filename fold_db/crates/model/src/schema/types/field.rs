use super::key_value::KeyValue;
use serde::{Deserialize, Serialize};

/// Keyed filter selectors for Hash and HashRange queries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum HashRangeFilter {
    HashRangeKey {
        hash: String,
        range: String,
    },
    HashKey(String),
    HashRangePrefix {
        hash: String,
        prefix: String,
    },
    HashRangeRange {
        hash: String,
        start: String,
        end: String,
    },
    HashRangeKeys(Vec<(String, String)>),
}

impl HashRangeFilter {
    #[must_use]
    pub const fn is_key_restricted(&self) -> bool {
        true
    }
}

/// Page window over the key set selected by a caller's key-restricted filter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyWindow {
    Offset { offset: usize, limit: usize },
    After { after: KeyValue, limit: usize },
}

impl KeyWindow {
    #[must_use]
    pub fn offset(offset: usize, limit: usize) -> Self {
        Self::Offset { offset, limit }
    }

    #[must_use]
    pub fn after(after: KeyValue, limit: usize) -> Self {
        Self::After { after, limit }
    }
}

/// Range prefix bound helpers.
pub struct FilterUtils;

impl FilterUtils {
    /// Return the finite exclusive upper bound for a physical prefix.
    pub fn create_prefix_end(prefix: &str) -> Result<String, crate::schema::SchemaError> {
        let mut prefix_end = prefix.to_string();
        while let Some(last_char) = prefix_end.pop() {
            let next_cp = match last_char as u32 + 1 {
                0xD800 => 0xE000,
                n => n,
            };
            if let Some(next_char) = char::from_u32(next_cp) {
                prefix_end.push(next_char);
                return Ok(prefix_end);
            }
        }
        Err(crate::schema::SchemaError::InvalidData(
            "the prefix has no finite upper bound; select by prefix instead".into(),
        ))
    }
}
