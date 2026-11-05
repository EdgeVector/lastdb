//! Publish immutable record-owned file versions.

use super::RecordAddress;
use crate::schema::types::{FileWrite, FileWriteIssuer};
use crate::schema::SchemaError;
use crate::storage::current_keys::families::record_file_path;
use crate::storage::KvMutation;
use lastdb_model::record_value::StoredRecord;
use std::sync::Arc;

pub(super) struct FileChange {
    pub(super) key: RecordAddress,
    pub(super) after: Arc<StoredRecord>,
    pub(super) write: Option<FileWrite>,
}

/// Compare against the writer's current record, including every marker field.
/// A new reference needs its bound payload; unchanged references reuse bytes.
pub(super) fn encode(
    change: FileChange,
    before: Option<&StoredRecord>,
    issuer: &FileWriteIssuer,
    content_key: Option<&[u8; 32]>,
) -> Result<Vec<KvMutation>, SchemaError> {
    let FileChange { key, after, write } = change;
    let previous = before.and_then(|record| record.file_version.as_ref());
    let next = after.file_version.as_ref();
    if previous == next {
        if before.and_then(|record| record.file.as_ref()) != after.file.as_ref() {
            return Err(SchemaError::InvalidData(
                "a retained file version has a different marker".into(),
            ));
        }
        return Ok(Vec::new());
    }
    let mut rows = Vec::new();
    if let Some(version) = next {
        let Some(FileWrite::Put(file)) = write else {
            return Err(SchemaError::InvalidData(
                "a selected file requires its sealed payload".into(),
            ));
        };
        let marker = after
            .file
            .as_ref()
            .ok_or_else(|| {
                SchemaError::InvalidData("a file row requires its record marker".into())
            })?
            .clone();
        if file.marker() != &marker {
            return Err(SchemaError::InvalidData(
                "file payload differs from its record marker".into(),
            ));
        }
        let row = issuer.admit(file).ok_or_else(|| {
            SchemaError::InvalidData("file payload belongs to another format writer".into())
        })?;
        super::file_crypto::require_row(content_key, &marker, &row)?;
        rows.push(KvMutation::put(
            record_file_path(key.as_str(), version)?.into_bytes(),
            row,
        ));
    }
    if let Some(version) = previous {
        rows.push(KvMutation::delete(
            record_file_path(key.as_str(), version)?.into_bytes(),
        ));
    }
    Ok(rows)
}
