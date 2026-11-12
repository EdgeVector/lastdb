//! Schema model types and pure logical layout rules.

mod mutation;
mod operations;

pub use logical_rules::{require_logical_filter, require_logical_layout};
pub use types::{Schema, SchemaError};

pub mod logical_rules {
    use super::types::{field::HashRangeFilter, schema::DeclarativeSchemaType};
    use super::SchemaError;

    pub fn require_logical_layout(schema_type: &DeclarativeSchemaType) -> Result<(), SchemaError> {
        matches!(schema_type, DeclarativeSchemaType::Hash | DeclarativeSchemaType::HashRange)
            .then_some(())
            .ok_or_else(|| {
                SchemaError::InvalidData(
                    "LastDB supports only Hash and HashRange schemas; Single and standalone Range are unsupported".into(),
                )
            })
    }

    pub fn require_logical_filter(filter: Option<&HashRangeFilter>) -> Result<(), SchemaError> {
        filter.map(|_| ()).ok_or_else(|| {
            SchemaError::InvalidData("LastDB requires a Hash or HashRange key selector".into())
        })
    }
}

pub mod types {
    pub use crate::schema::operations::author_clock;
    pub mod errors {
        pub use crate::schema::operations::SchemaError;
    }
    pub mod mutation {
        pub use crate::schema::mutation::*;
    }
    pub mod operations {
        pub use crate::schema::mutation::Mutation;
        pub use crate::schema::operations::{
            MutationConvergence, MutationDurability, MutationType, Operation, Query, SortOrder,
        };
    }

    pub use author_clock::{MutationAuthorClockState, MUTATION_AUTHOR_SIGNATURE_VERSION};
    pub use cas::CasExpectation;
    pub use declarative_schemas::DeclarativeSchemaDefinition;
    pub use errors::SchemaError;
    pub use key_value::KeyValue;
    pub use mutation::{
        FileWrite, FileWriteIssuer, LocalFileReference, Mutation, SealedRecordFile,
    };
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
            ) -> Result<&schema_types::record_keys::RecordKeys, crate::schema::SchemaError>
            {
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
}
