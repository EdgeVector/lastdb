//! Record-file framing shares the format root's single prefix definition.

/// Key family of record files in `cas_blobs` (design
/// `design-lastdb-no-shared-file-blobs`).
pub(crate) use crate::storage::format::{RECORD_FILE_PREFIX, RECORD_FILE_UPLOADED_PREFIX};
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

/// The cloud location of one record file version: the same space, record key
/// digest and version as [`record_file_path`], under the database identity
/// (design `design-lastdb-blob-files-on-demand-20261107`). Every device
/// derives it from the record, so no row records it.
pub(crate) fn record_blob_path(
    database: &cloud_backup_protocol::Id,
    record: &str,
    version: &Position,
) -> super::StorageResult<cloud_backup_protocol::v2::blobs::BlobPath> {
    let path = record_file_path(record, version)?;
    let invalid = || super::unsupported("a record file path has no cloud blob path");
    let mut parts = path
        .strip_prefix(RECORD_FILE_PREFIX)
        .ok_or_else(invalid)?
        .splitn(3, ':');
    let (Some(space), Some(digest), Some(write_id)) = (parts.next(), parts.next(), parts.next())
    else {
        return Err(invalid());
    };
    let blob = cloud_backup_protocol::v2::blobs::BlobPath {
        database: database.clone(),
        space: space.to_owned().try_into().map_err(|_| invalid())?,
        record: digest.to_owned().try_into().map_err(|_| invalid())?,
        write_id: write_id.to_owned().try_into().map_err(|_| invalid())?,
    };
    blob.validate().map_err(|_| invalid())?;
    Ok(blob)
}

/// The "already in the cloud" marker of the record file row at `file_path`
/// (a [`record_file_path`] key): the same suffix under
/// [`RECORD_FILE_UPLOADED_PREFIX`]. A node writes it with a row it downloaded,
/// so backup never uploads that row again. A row without it was created
/// locally by a record write.
pub(crate) fn record_file_uploaded_path(file_path: &[u8]) -> super::StorageResult<Vec<u8>> {
    let suffix = file_path
        .strip_prefix(RECORD_FILE_PREFIX.as_bytes())
        .filter(|suffix| !suffix.is_empty())
        .ok_or_else(|| super::unsupported("an uploaded marker needs a record file path"))?;
    Ok([RECORD_FILE_UPLOADED_PREFIX.as_bytes(), suffix].concat())
}

/// The record file family of a `cas_blobs` key: its family prefix and its
/// schema space. `None` for any other key.
pub(crate) fn record_file_space(key: &str) -> Option<(&'static str, &str)> {
    [RECORD_FILE_PREFIX, RECORD_FILE_UPLOADED_PREFIX]
        .into_iter()
        .find_map(|prefix| {
            let rest = key.strip_prefix(prefix)?;
            Some((
                prefix,
                rest.split_once(':').map_or(rest, |(space, _)| space),
            ))
        })
}
