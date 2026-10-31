//! The file dependencies of the records of a format 7 home.
//!
//! A record has no atom body and no `fdep` directory row. Each field value
//! that holds a file pointer names its blobs, and the proof copies and opens
//! each one from the authenticated CAS run, as it does for an atom.
use super::{files, invalid, Proof, StorageResult};
use crate::storage::disk_pack::native_routing;
use lastdb_model::record_space::RECORD_PREFIX;
use lastdb_model::record_value::{StoredRecord, MAX_RECORD_BYTES};

/// Prove the file dependencies of the record at one tips row. False for a
/// row that is not a record; the caller then classifies it.
pub(super) fn verify(proof: &Proof<'_>, key: &str, body: &[u8]) -> StorageResult<bool> {
    let raw = native_routing::decode(key)?;
    let raw = std::str::from_utf8(&raw).map_err(|_| invalid("run backup record key"))?;
    let Ok(bare) = crate::storage::current_keys::source_key(raw) else {
        return Ok(false);
    };
    if !bare.starts_with(RECORD_PREFIX) {
        return Ok(false);
    }
    crate::storage::current_keys::validate("main", raw.as_bytes())?;
    if body.len() > MAX_RECORD_BYTES {
        return Err(invalid("run backup record exceeds its bound"));
    }
    // The charged decoder bounds the tree before the strict record decoder.
    drop(files::decode_bounded_json(body)?);
    let record = StoredRecord::decode(body).map_err(|_| invalid("run backup record format"))?;
    let dependencies = files::expected_record(&record)?;
    drop(record);
    for dependency in dependencies {
        super::phase(
            super::Stage::NativeCas,
            super::native::verify(proof, &dependency),
        )?;
    }
    Ok(true)
}
