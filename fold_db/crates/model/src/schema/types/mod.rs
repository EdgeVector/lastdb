pub mod errors;
pub mod field;
pub mod mutation;
pub mod operations;

pub use author_clock::{MutationAuthorClockState, MUTATION_AUTHOR_SIGNATURE_VERSION};
pub use cas::CasExpectation;
pub use declarative_schemas::DeclarativeSchemaDefinition;
pub use errors::SchemaError;
pub use field_value_type::FieldValueType;
pub use key_config::KeyConfig;
pub use key_value::KeyValue;
pub use mutation::{FileWrite, FileWriteIssuer, LocalFileReference, Mutation, SealedRecordFile};
pub use operations::{MutationType, Operation, Query, SortOrder};
pub use query_cursor::QueryCursor;
pub use schema::{DeclarativeSchemaType as SchemaType, Schema};

pub mod cas {
    pub use crate::record_replacement::RecordExpectation as CasExpectation;
}

pub mod field_value_type {
    pub use schema_types::FieldValueType;
}

pub mod key_config {
    pub use schema_types::KeyConfig;
}

pub mod schema {
    pub use super::declarative_schemas::DeclarativeSchemaDefinition as Schema;
    pub use schema_types::DeclarativeSchemaType;
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
            let mut state = *self;
            let (first, counter) = state.advance_local(now_nanos)?;
            let last = first.checked_add(additional)?;
            state.physical_nanos = last;
            *self = state;
            Some((first..=last, counter))
        }

        #[must_use]
        pub fn advance_local(&mut self, now_nanos: u64) -> Option<(u64, u64)> {
            let counter = self.logical_counter.checked_add(1)?;
            self.physical_nanos = self.physical_nanos.max(now_nanos);
            self.logical_counter = counter;
            Some((self.physical_nanos, self.logical_counter))
        }

        pub fn observe_remote(&mut self, physical_nanos: u64, logical_counter: u64) {
            self.physical_nanos = self.physical_nanos.max(physical_nanos);
            self.logical_counter = self.logical_counter.max(logical_counter);
        }
    }

    #[must_use]
    pub fn mutation_author_clock_key(device_id: &str) -> String {
        let digest = Sha256::digest(device_id.as_bytes());
        format!("mutation_author_clock:{digest:x}")
    }

    #[must_use]
    pub fn mutation_author_canonical_bytes(mutation: &Mutation) -> Vec<u8> {
        let written_at = mutation.imported_written_at.unwrap_or(0);
        let content_hash = mutation.content_hash();
        crate::canonical::CanonicalWriter::with_capacity(
            content_hash.len() + mutation.uuid.len() + mutation.author_clock_writer_id.len() + 32,
        )
        .field(b"lastdb-mutation-author-v3")
        .field(content_hash.as_bytes())
        .field(mutation.uuid.as_bytes())
        .field(mutation.author_clock_writer_id.as_bytes())
        .u64(written_at)
        .u64(mutation.logical_counter)
        .finish()
    }
}

pub mod declarative_schemas {
    use std::ops::{Deref, DerefMut};

    #[derive(Debug, Clone)]
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
            self.fields
                .as_ref()
                .is_some_and(|fields| fields.iter().any(|field| field == name))
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

        pub fn validate_field_name(name: &str) -> Result<(), String> {
            schema_types::DeclarativeSchemaDefinition::validate_field_name(name)
        }

        #[must_use]
        pub fn new(
            name: String,
            schema_type: schema_types::DeclarativeSchemaType,
            key: Option<schema_types::KeyConfig>,
            fields: Option<Vec<String>>,
        ) -> Self {
            Self::with_wire(schema_types::DeclarativeSchemaDefinition::new(
                name,
                schema_type,
                key,
                fields,
            ))
        }

        fn with_wire(wire: schema_types::DeclarativeSchemaDefinition) -> Self {
            Self { wire }
        }

        #[must_use]
        pub fn parse_canonical_name(canonical: &str) -> (Option<String>, &str) {
            schema_types::parse_canonical_name(canonical)
        }

        #[must_use]
        pub fn to_wire(&self) -> schema_types::DeclarativeSchemaDefinition {
            let mut wire = self.wire.clone();
            wire.normalize_wire();
            wire
        }
    }

    impl From<schema_types::DeclarativeSchemaDefinition> for DeclarativeSchemaDefinition {
        fn from(mut base: schema_types::DeclarativeSchemaDefinition) -> Self {
            base.normalize_wire();
            Self::with_wire(base)
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

    impl PartialEq for DeclarativeSchemaDefinition {
        fn eq(&self, other: &Self) -> bool {
            self.wire == other.wire
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
                (Some(hash), Some(range)) => write!(f, "{hash}:{range}"),
                (Some(hash), None) => write!(f, "{hash}"),
                (None, Some(range)) => write!(f, "{range}"),
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
}
