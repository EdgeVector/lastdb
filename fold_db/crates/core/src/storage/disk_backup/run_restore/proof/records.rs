//! The records of a home and their files.
//!
//! A record has no separate body row and no file dependency row. A record
//! with a file marker has one file, at a path made from its key. The proof
//! checks that the path is a live row of the authenticated CAS run.
use super::{budget, invalid, map, read_budget, rows::Rows, Proof, StorageResult};
use crate::storage::disk_pack::native_routing;
use crate::storage::format::records::applied_deletes;
use lastdb_model::record_space::RECORD_PREFIX;
use lastdb_model::record_value::{StoredRecord, MAX_STORED_RECORD_BYTES};

/// Validate the keys and records of the `records` collection and return the
/// number of record files found.
pub(super) fn verify_records(proof: &Proof<'_>) -> StorageResult<u64> {
    let mut read = super::rows::read(proof.main, proof.key_heads)?;
    let mut rows = Rows::new(proof.main, read.as_mut(), "records")?;
    let mut files = 0u64;
    while let Some((key, body)) = rows.next()? {
        files += u64::from(verify(proof, &key, &body)?);
    }
    Ok(files)
}

/// Validate a row of the `records` collection. Returns true when the record has a file.
fn verify(proof: &Proof<'_>, key: &str, body: &[u8]) -> StorageResult<bool> {
    let raw = native_routing::decode(key)?;
    let raw = std::str::from_utf8(&raw).map_err(|_| invalid("run backup record key"))?;
    crate::storage::current_keys::validate(crate::storage::format::MAIN_NAMESPACE, raw.as_bytes())?;
    if applied_deletes::record_key(raw).is_some() {
        applied_deletes::decode(body)?;
        return Ok(false);
    }
    if applied_deletes::is_age_key(raw) {
        applied_deletes::validate_age_row(raw, body)?;
        return Ok(false);
    }
    if !raw.starts_with(RECORD_PREFIX) {
        return Ok(false);
    }
    if body.len() > MAX_STORED_RECORD_BYTES {
        return Err(invalid("run backup record exceeds its bound"));
    }
    // The charged decoder bounds the tree before the strict record decoder.
    drop(super::budget_json::decode(body)?);
    let record = StoredRecord::decode(body).map_err(|_| invalid("run backup record format"))?;
    let Some(version) = record.file_version else {
        return Ok(false);
    };
    let path = crate::storage::current_keys::families::record_file_path(raw, &version)
        .map_err(|_| invalid("run backup record file path"))?;
    super::phase(super::Stage::NativeCas, file_in_backup(proof, &path))?;
    Ok(true)
}

/// The file must be a live row of the authenticated CAS run. The read streams
/// the row through, so the run reader checks its blocks, and keeps nothing.
fn file_in_backup(proof: &Proof<'_>, path: &str) -> StorageResult<()> {
    let key = native_routing::encode(path.as_bytes());
    let mut read = super::fork_mapped(
        proof.cas,
        read_budget(budget::NATIVE_STREAM_LOOKUP_READ_BYTES, 1),
    )?;
    read.copy_body_to(
        crate::storage::disk_pack::CAS_COLLECTION,
        key.as_bytes(),
        &mut std::io::sink(),
    )
    .map_err(map)?
    .ok_or_else(|| invalid("run backup record file missing"))?;
    Ok(())
}
