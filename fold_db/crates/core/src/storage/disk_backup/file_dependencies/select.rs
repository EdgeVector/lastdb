use super::*;
use lastdb_model::record_value::StoredRecord;

/// The file of the record at `record_key`, or `None` when the record has no
/// file. The path comes from the key and the file key from the account key
/// and the marker's hash, as the write that sealed the file made them.
pub fn expected_record(
    record_key: &str,
    record: &StoredRecord,
    account_key: &[u8; 32],
) -> StorageResult<Option<FileDependency>> {
    let Some(file) = record.file.as_ref() else {
        return Ok(None);
    };
    let marker = file.marker();
    if !marker.has_valid_hash() {
        return Err(invalid("file marker digest"));
    }
    let stored = marker
        .size_bytes
        .checked_add(cloud_backup_protocol::limits::ENCRYPTION_ENVELOPE_OVERHEAD_BYTES);
    if stored.is_none_or(|stored| stored > FILE_DEPENDENCY_MAX_STORED_BYTES) {
        return Err(invalid("file dependency exceeds total backup policy"));
    }
    let path = crate::db_operations::record_store::record_files::file_path(record_key)
        .map_err(|_| invalid("file dependency path"))?;
    let dek = crate::sharing::file_blob_seal::account_file_blob_dek_hex(account_key, &marker.sha256)
        .map_err(|_| invalid("file dependency key format"))?;
    let dek = zeroize::Zeroizing::new(dek);
    let dek = zeroize::Zeroizing::new(
        lastdb_model::hex::hex_decode_array::<32>(&dek)
            .ok_or_else(|| invalid("file dependency key format"))?,
    );
    Ok(Some(FileDependency {
        path,
        sha256: marker.sha256,
        size_bytes: marker.size_bytes,
        dek,
    }))
}
