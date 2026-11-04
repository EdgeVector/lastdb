//! Compose the existing record-owned file row with its record write.

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
) -> Result<Option<KvMutation>, SchemaError> {
    let FileChange { key, after, write } = change;
    let set = after.file.is_some()
        && before.and_then(|record| record.file.as_ref()) != after.file.as_ref();
    Ok(if set {
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
        Some(KvMutation::put(
            record_file_path(key.as_str())?.into_bytes(),
            row,
        ))
    } else if after.file.is_none() {
        // Absence is the desired file state, regardless of a provider's
        // before hint. Native deletion probes the actual owned path and is
        // a no-op when absent; no record read is needed to authorize cleanup.
        Some(KvMutation::delete(
            record_file_path(key.as_str())?.into_bytes(),
        ))
    } else {
        None
    })
}
