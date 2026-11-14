//! Schema domain errors and structured JSON error bodies.

use crate::limits::content_limit::RECORD_TOO_LARGE_HINT;
use serde::Serialize;
use serde_json::Value;
use std::fmt;

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
            Self::ReplayBeforeDeleteCutoff { cutoff_nanos } => write!(
                f,
                "replay_cutoff_requires_resync: original write time is missing or at/before {cutoff_nanos}; restore a current snapshot before replay"
            ),
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
