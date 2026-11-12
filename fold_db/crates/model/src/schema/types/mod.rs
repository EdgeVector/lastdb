pub mod errors;
pub mod mutation;
pub mod operations;

pub use author_clock::{MutationAuthorClockState, MUTATION_AUTHOR_SIGNATURE_VERSION};
pub use cas::CasExpectation;
pub use declarative_schemas::DeclarativeSchemaDefinition;
pub use errors::SchemaError;
pub use key_value::KeyValue;
pub use mutation::{FileWrite, FileWriteIssuer, LocalFileReference, Mutation, SealedRecordFile};
pub use operations::{MutationType, Operation, Query, SortOrder};
pub use query_cursor::QueryCursor;
pub use schema::{DeclarativeSchemaType as SchemaType, Schema};
pub use schema_types::{FieldValueType, KeyConfig};

pub mod cas {
    pub use crate::record_replacement::RecordExpectation as CasExpectation;
}

pub mod schema {
    pub use super::declarative_schemas::DeclarativeSchemaDefinition as Schema;
    pub use schema_types::DeclarativeSchemaType;
}

pub mod field {
    use super::key_value::KeyValue;
    use serde::{Deserialize, Serialize};

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

    pub struct FilterUtils;

    impl FilterUtils {
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
}

pub mod author_clock {
    use super::Mutation;
    use sha2::{Digest, Sha256};

    pub const MUTATION_AUTHOR_SIGNATURE_VERSION: u8 = 3;

    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
    pub struct MutationAuthorClockState {
        pub physical_nanos: u64,
        pub logical_counter: u64,
    }

    impl MutationAuthorClockState {
        pub fn advance_local_batch(
            &mut self,
            now_nanos: u64,
            count: usize,
        ) -> Option<(std::ops::RangeInclusive<u64>, u64)> {
            let additional = u64::try_from(count.checked_sub(1)?).ok()?;
            let counter = self.logical_counter.checked_add(1)?;
            let first = self.physical_nanos.max(now_nanos);
            let last = first.checked_add(additional)?;
            self.physical_nanos = last;
            self.logical_counter = counter;
            Some((first..=last, counter))
        }

        pub fn observe_remote(&mut self, physical_nanos: u64, logical_counter: u64) {
            self.physical_nanos = self.physical_nanos.max(physical_nanos);
            self.logical_counter = self.logical_counter.max(logical_counter);
        }
    }

    #[must_use]
    pub fn mutation_author_clock_key(device_id: &str) -> String {
        format!(
            "mutation_author_clock:{:x}",
            Sha256::digest(device_id.as_bytes())
        )
    }

    #[must_use]
    pub fn mutation_author_canonical_bytes(mutation: &Mutation) -> Vec<u8> {
        let content_hash = mutation.content_hash();
        crate::canonical::CanonicalWriter::with_capacity(
            content_hash.len() + mutation.uuid.len() + mutation.author_clock_writer_id.len() + 32,
        )
        .field(b"lastdb-mutation-author-v3")
        .field(content_hash.as_bytes())
        .field(mutation.uuid.as_bytes())
        .field(mutation.author_clock_writer_id.as_bytes())
        .u64(mutation.imported_written_at.unwrap_or(0))
        .u64(mutation.logical_counter)
        .finish()
    }
}

pub mod declarative_schemas {
    use std::ops::{Deref, DerefMut};

    #[derive(Debug, Clone, PartialEq)]
    pub struct DeclarativeSchemaDefinition {
        wire: schema_types::DeclarativeSchemaDefinition,
    }

    impl Deref for DeclarativeSchemaDefinition {
        type Target = schema_types::DeclarativeSchemaDefinition;
        fn deref(&self) -> &Self::Target {
            &self.wire
        }
    }

    impl DerefMut for DeclarativeSchemaDefinition {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.wire
        }
    }

    impl DeclarativeSchemaDefinition {
        #[must_use]
        pub fn has_field(&self, name: &str) -> bool {
            self.fields.iter().flatten().any(|field| field == name)
        }

        pub fn validate(&self) -> Result<(), crate::schema::SchemaError> {
            self.validate_definition()
                .map_err(crate::schema::SchemaError::InvalidData)
        }

        pub fn require_record_keys(
            &self,
        ) -> Result<&schema_types::record_keys::RecordKeys, crate::schema::SchemaError> {
            self.record_keys.as_ref().ok_or_else(|| {
                crate::schema::SchemaError::InvalidData(
                    "this home requires explicit record_keys".into(),
                )
            })
        }

        #[must_use]
        pub fn to_wire(&self) -> schema_types::DeclarativeSchemaDefinition {
            let mut wire = self.wire.clone();
            wire.normalize_wire();
            wire
        }
    }

    impl From<schema_types::DeclarativeSchemaDefinition> for DeclarativeSchemaDefinition {
        fn from(mut wire: schema_types::DeclarativeSchemaDefinition) -> Self {
            wire.normalize_wire();
            Self { wire }
        }
    }

    impl From<&DeclarativeSchemaDefinition> for schema_types::DeclarativeSchemaDefinition {
        fn from(schema: &DeclarativeSchemaDefinition) -> Self {
            schema.to_wire()
        }
    }

    impl From<DeclarativeSchemaDefinition> for schema_types::DeclarativeSchemaDefinition {
        fn from(schema: DeclarativeSchemaDefinition) -> Self {
            schema.to_wire()
        }
    }

    impl serde::Serialize for DeclarativeSchemaDefinition {
        fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            self.wire.serialize(serializer)
        }
    }

    impl<'de> serde::Deserialize<'de> for DeclarativeSchemaDefinition {
        fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
            schema_types::DeclarativeSchemaDefinition::deserialize(deserializer).map(Self::from)
        }
    }
}

pub mod key_value {
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Default)]
    #[serde(deny_unknown_fields)]
    pub struct KeyValue {
        pub hash: Option<String>,
        pub range: Option<String>,
    }

    impl KeyValue {
        pub fn new(hash: Option<String>, range: Option<String>) -> Self {
            Self { hash, range }
        }

        #[must_use]
        pub fn to_storage_key(&self) -> String {
            serde_json::to_string(self).expect("KeyValue is always JSON-serializable")
        }

        #[must_use]
        pub fn cmp_page_order_with(
            &self,
            other: &Self,
            compare_ranges: impl FnOnce(&str, &str) -> std::cmp::Ordering,
        ) -> std::cmp::Ordering {
            compare_ranges(
                self.range.as_deref().unwrap_or(""),
                other.range.as_deref().unwrap_or(""),
            )
            .then_with(|| {
                self.hash
                    .as_deref()
                    .unwrap_or("")
                    .cmp(other.hash.as_deref().unwrap_or(""))
            })
        }

        #[must_use]
        pub fn cmp_storage_order(&self, other: &Self) -> std::cmp::Ordering {
            self.cmp_page_order_with(other, str::cmp)
        }
    }

    impl std::fmt::Display for KeyValue {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match (&self.hash, &self.range) {
                (Some(h), Some(r)) => write!(f, "{h}:{r}"),
                (Some(s), None) | (None, Some(s)) => f.write_str(s),
                (None, None) => Ok(()),
            }
        }
    }
}

pub mod query_cursor {
    use super::KeyValue;
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    pub struct QueryCursor {
        pub hash: Option<String>,
        pub range: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub comparison: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub record: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub alternate_primary: Option<KeyValue>,
    }

    impl QueryCursor {
        #[must_use]
        pub fn new(key: KeyValue, comparison: Option<String>) -> Self {
            Self {
                hash: key.hash,
                range: key.range,
                comparison,
                record: None,
                alternate_primary: None,
            }
        }

        #[must_use]
        pub fn at_record(suffix: &str, comparison: Option<String>) -> Self {
            Self {
                hash: None,
                range: None,
                comparison,
                record: Some(URL_SAFE_NO_PAD.encode(suffix)),
                alternate_primary: None,
            }
        }

        pub fn record_position(&self) -> Result<Option<String>, String> {
            self.record
                .as_deref()
                .map(|r| {
                    let bytes = URL_SAFE_NO_PAD
                        .decode(r)
                        .map_err(|_| "the record cursor is not base64url".to_string())?;
                    String::from_utf8(bytes)
                        .map_err(|_| "the record cursor is not a record key".to_string())
                })
                .transpose()
        }

        #[must_use]
        pub fn into_key(self) -> KeyValue {
            KeyValue::new(self.hash, self.range)
        }
    }
}
