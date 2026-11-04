//! Record-file framing shares the format root's single prefix definition.

/// Key family of record files in `cas_blobs` (design
/// `design-lastdb-no-shared-file-blobs`).
pub(crate) use crate::storage::format::RECORD_FILE_PREFIX;

/// The file path of the record at one storage key:
/// `fb:{S}:{sha256(record key)}`. One record has one path, so two records
/// never share a file.
pub(crate) fn record_file_path(record: &str) -> super::StorageResult<String> {
    use sha2::Digest as _;
    let space = record
        .strip_prefix(lastdb_model::record_space::RECORD_PREFIX)
        .and_then(|rest| rest.split(':').next())
        .filter(|space| !space.is_empty())
        .ok_or_else(|| super::unsupported("a record file path needs a record key"))?;
    let digest = lastdb_model::hex::hex_lower(sha2::Sha256::digest(record.as_bytes()));
    Ok(format!("{RECORD_FILE_PREFIX}{space}:{digest}"))
}
