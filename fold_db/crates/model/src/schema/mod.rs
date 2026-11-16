//! Schema model types, queries, errors, and pure logical layout rules.

mod mutation;

use crate::record_value::RECORD_TOO_LARGE_HINT;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;

pub use crate::keys::{QueryCursor, SortOrder};
pub use logical_rules::{require_logical_filter, require_logical_layout};
pub use types::Schema;

#[derive(Debug, Clone, Serialize)]
pub enum SchemaError {
    NotFound(String),
    Blocked(String),
    ReadResourceLimit {
        detail: String,
    },
    WriteResourceLimit {
        detail: String,
    },
    StorageRead {
        detail: String,
    },
    InvalidField(String),
    InvalidData(String),
    CasConflict {
        schema: String,
        key: String,
        expected: String,
        actual: Option<String>,
    },
    RecordTooLarge {
        size: usize,
        limit: usize,
    },
    InvalidCursor(String),
    StorageFull {
        detail: String,
    },
}

impl SchemaError {
    #[must_use]
    pub fn cas_conflict_body(&self) -> Option<Value> {
        let Self::CasConflict {
            schema,
            key,
            expected,
            actual,
        } = self
        else {
            return None;
        };
        Some(serde_json::json!({
            "ok": false, "error": "cas_conflict", "schema": schema, "key": key,
            "expected": expected, "actual": actual, "message": self.to_string(),
        }))
    }

    #[must_use]
    pub fn field_value_too_large_body(&self) -> Option<Value> {
        let Self::RecordTooLarge { size, limit } = self else {
            return None;
        };
        Some(serde_json::json!({
            "ok": false, "error": "field_value_too_large", "scope": "record",
            "size": size, "limit": limit,
            "hint": RECORD_TOO_LARGE_HINT,
            "message": self.to_string(),
        }))
    }

    #[must_use]
    pub fn storage_full_body(&self) -> Option<Value> {
        let Self::StorageFull { detail } = self else {
            return None;
        };
        Some(serde_json::json!({
            "ok": false, "error": "storage_full", "detail": detail, "retryable": true,
            "hint": "the host filesystem is out of space; free disk space, then retry the same write",
            "message": self.to_string(),
        }))
    }
}

impl fmt::Display for SchemaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound(msg) => write!(f, "Schema not found: {msg}"),
            Self::Blocked(msg) => write!(f, "Schema blocked: {msg}"),
            Self::ReadResourceLimit { detail } => {
                write!(f, "read resource limit exceeded: {detail}")
            }
            Self::WriteResourceLimit { detail } => {
                write!(f, "write resource limit exceeded: {detail}")
            }
            Self::StorageRead { detail } => write!(f, "storage read failed: {detail}"),
            Self::InvalidField(msg) => write!(f, "Invalid field: {msg}"),
            Self::InvalidData(msg) => write!(f, "Invalid data: {msg}"),
            Self::InvalidCursor(msg) => write!(f, "Invalid resume cursor: {msg}"),
            Self::CasConflict {
                schema,
                key,
                expected,
                actual,
            } => write!(
                f,
                "CAS conflict on schema '{schema}' key '{key}':                  expected {expected}, found {}",
                actual.as_deref().unwrap_or("<absent>")
            ),
            Self::RecordTooLarge { size, limit } => write!(
                f,
                "record too large: {size} bytes exceeds the record cap of {limit} bytes. {RECORD_TOO_LARGE_HINT}"
            ),
            Self::StorageFull { detail } => write!(
                f,
                "no space left on device — the filesystem holding the database is full, \
                 so the write did not land. Free disk space and retry; nothing already \
                 stored was lost. Underlying error: {detail}"
            ),
        }
    }
}

impl std::error::Error for SchemaError {}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Query {
    pub schema_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_name: Option<String>,
    pub filter: Option<types::field::HashRangeFilter>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sort_order: Option<SortOrder>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_total_count: Option<usize>,
}

impl Query {
    #[must_use]
    pub fn new_with_filter(
        schema_name: String,
        filter: Option<types::field::HashRangeFilter>,
    ) -> Self {
        Self {
            schema_name,
            key_name: None,
            filter,
            sort_order: None,
            expected_total_count: None,
        }
    }
}

pub mod logical_rules {
    use super::types::{field::HashRangeFilter, schema::DeclarativeSchemaType};
    use super::SchemaError;

    pub fn require_logical_layout(schema_type: &DeclarativeSchemaType) -> Result<(), SchemaError> {
        matches!(
            schema_type,
            DeclarativeSchemaType::Hash | DeclarativeSchemaType::HashRange
        )
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
    pub use crate::keys::key_value;
    pub use crate::record_value::author_clock;

    pub mod mutation {
        pub use crate::schema::mutation::{
            ContributionAdmission, FileWrite, FileWriteIssuer, LocalFileReference, Mutation,
            SealedRecordFile,
        };
    }
    pub mod operations {
        pub use crate::keys::SortOrder;
        pub use crate::schema::mutation::{
            Mutation, MutationConvergence, MutationDurability, MutationType, Operation,
        };
        pub use crate::schema::Query;
    }
    pub mod cas {
        pub use crate::portable_mutation::record_replacement::RecordExpectation as CasExpectation;
    }
    pub mod schema {
        pub use super::declarative_schemas::DeclarativeSchemaDefinition as Schema;
        pub use schema_types::DeclarativeSchemaType;
    }

    pub use crate::keys::QueryCursor;
    pub use crate::schema::SchemaError;
    pub use author_clock::MutationAuthorClockState;
    pub use cas::CasExpectation;
    pub use declarative_schemas::DeclarativeSchemaDefinition;
    pub use key_value::KeyValue;
    pub use mutation::{
        FileWrite, FileWriteIssuer, LocalFileReference, Mutation, SealedRecordFile,
    };
    pub use operations::{MutationType, Operation, Query, SortOrder};
    pub use schema::{DeclarativeSchemaType as SchemaType, Schema};
    pub use schema_types::{FieldValueType, KeyConfig};

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
                let mut end = prefix.to_owned();
                while let Some(c) = end.pop() {
                    let next_cp = match c as u32 + 1 {
                        0xD800 => 0xE000,
                        n => n,
                    };
                    if let Some(next) = char::from_u32(next_cp) {
                        end.push(next);
                        return Ok(end);
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
            fn from(s: &DeclarativeSchemaDefinition) -> Self {
                s.to_wire()
            }
        }

        impl From<DeclarativeSchemaDefinition> for schema_types::DeclarativeSchemaDefinition {
            fn from(s: DeclarativeSchemaDefinition) -> Self {
                s.to_wire()
            }
        }

        impl serde::Serialize for DeclarativeSchemaDefinition {
            fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                self.wire.serialize(s)
            }
        }

        impl<'de> serde::Deserialize<'de> for DeclarativeSchemaDefinition {
            fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                schema_types::DeclarativeSchemaDefinition::deserialize(d).map(Self::from)
            }
        }
    }
}
