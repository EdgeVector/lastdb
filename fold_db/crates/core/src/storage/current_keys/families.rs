//! Record-file framing shares the format root's single prefix definition.

/// Key family of record files in `cas_blobs` (design
/// `design-lastdb-no-shared-file-blobs`).
pub(crate) use crate::storage::format::RECORD_FILE_PREFIX;
use lastdb_model::portable_mutation::Position;

/// One immutable file version owned by a single record.
pub(crate) fn record_file_path(record: &str, version: &Position) -> super::StorageResult<String> {
    use sha2::Digest as _;
    let space = record
        .strip_prefix(lastdb_model::record_space::RECORD_PREFIX)
        .and_then(|rest| rest.split(':').next())
        .filter(|space| !space.is_empty())
        .ok_or_else(|| super::unsupported("a record file path needs a record key"))?;
    let digest = lastdb_model::hex::hex_lower(sha2::Sha256::digest(record.as_bytes()));
    Ok(format!(
        "{RECORD_FILE_PREFIX}{space}:{digest}:{}:{:020}",
        version.source_stream, version.sequence
    ))
}
