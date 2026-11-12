use super::types::{cas::CasExpectation, field::HashRangeFilter, key_value::KeyValue};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::fmt;

pub mod author_clock {
    use crate::schema::mutation::Mutation;
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
    ReplayBeforeDeleteCutoff {
        cutoff_nanos: u64,
    },
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

    pub fn field_value_too_large_body(&self) -> Option<Value> {
        let Self::RecordTooLarge { size, limit } = self else {
            return None;
        };
        Some(serde_json::json!({
            "ok": false, "error": "field_value_too_large", "scope": "record",
            "size": size, "limit": limit, "hint": crate::content_limit::RECORD_TOO_LARGE_HINT,
            "message": self.to_string(),
        }))
    }

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
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::NotFound(msg) => write!(f, "Schema not found: {msg}"),
            Self::Blocked(msg) => write!(f, "Schema blocked: {msg}"),
            Self::ReadResourceLimit { detail } => write!(f, "read resource limit exceeded: {detail}"),
            Self::WriteResourceLimit { detail } => write!(f, "write resource limit exceeded: {detail}"),
            Self::StorageRead { detail } => write!(f, "storage read failed: {detail}"),
            Self::InvalidField(msg) => write!(f, "Invalid field: {msg}"),
            Self::InvalidData(msg) => write!(f, "Invalid data: {msg}"),
            Self::ReplayBeforeDeleteCutoff { cutoff_nanos } => write!(
                f,
                "replay_cutoff_requires_resync: original write time is missing or at/before {cutoff_nanos}; restore a current snapshot before replay"
            ),
            Self::InvalidCursor(msg) => write!(f, "Invalid resume cursor: {msg}"),
            Self::CasConflict { schema, key, expected, actual } => write!(
                f,
                "CAS conflict on schema '{schema}' key '{key}':                  expected {expected}, found {}",
                actual.as_deref().unwrap_or("<absent>")
            ),
            Self::RecordTooLarge { size, limit } => write!(
                f,
                "record too large: {size} bytes exceeds the record cap of {limit} bytes. {}",
                crate::content_limit::RECORD_TOO_LARGE_HINT
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

#[derive(Debug, Clone, PartialEq)]
pub enum SortOrder {
    Asc,
    Desc,
}

impl Serialize for SortOrder {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(match self {
            Self::Asc => "asc",
            Self::Desc => "desc",
        })
    }
}

impl<'de> Deserialize<'de> for SortOrder {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        if s.eq_ignore_ascii_case("asc") {
            Ok(Self::Asc)
        } else if s.eq_ignore_ascii_case("desc") {
            Ok(Self::Desc)
        } else {
            Err(serde::de::Error::custom(format!(
                "unknown sort order '{s}', expected 'asc' or 'desc'"
            )))
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Query {
    pub schema_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_name: Option<String>,
    pub filter: Option<HashRangeFilter>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sort_order: Option<SortOrder>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_total_count: Option<usize>,
    #[serde(default)]
    pub include_tombstones: bool,
}

impl Query {
    #[must_use]
    pub fn new_with_filter(schema_name: String, filter: Option<HashRangeFilter>) -> Self {
        Self {
            schema_name,
            key_name: None,
            filter,
            sort_order: None,
            expected_total_count: None,
            include_tombstones: false,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MutationType {
    Create,
    Replace,
    Delete,
    Purge,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MutationConvergence {
    Sync,
    #[default]
    Async,
}

impl MutationConvergence {
    pub fn waits(self) -> bool {
        matches!(self, Self::Sync)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MutationDurability {
    Durable,
    #[default]
    Queued,
}

impl MutationDurability {
    pub fn waits_for_persist(self) -> bool {
        matches!(self, Self::Durable)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum Operation {
    #[serde(rename = "mutation")]
    Mutation {
        schema: String,
        #[serde(rename = "record")]
        record_fields: HashMap<String, Value>,
        key_value: KeyValue,
        alternates: BTreeMap<String, Option<KeyValue>>,
        mutation_type: MutationType,
        source_file_name: Option<String>,
        metadata: Option<HashMap<String, String>>,
        file: Option<crate::record_value::FileMarker>,
        expected: CasExpectation,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        convergence: Option<MutationConvergence>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        durability: Option<MutationDurability>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        definition_id: Option<String>,
    },
}
