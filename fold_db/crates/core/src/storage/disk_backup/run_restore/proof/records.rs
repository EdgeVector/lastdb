//! The records of a home and their files.
//!
//! A record has no separate body row and no file dependency row. A record
//! with a file marker has one file, at a path made from its key. For a shared
//! database the proof checks that the path is a live row of the authenticated
//! CAS run. A personal home's record files are B2 objects at a path derived
//! from the record, fetched on request, so the proof checks that the marker
//! derives a valid local and cloud path and does not look for the row. A record
//! with an inline file marker has one inline file row in the same run, and
//! the proof checks its bytes against the marker.
use super::{budget, invalid, map, read_budget, rows::Rows, Proof, StorageResult};
use crate::storage::disk_pack::native_routing;
use crate::storage::format::records::{applied_deletes, inline_files};
use lastdb_model::record_space::RECORD_PREFIX;
use lastdb_model::record_value::{
    FileMarker, InlineFile, StoredRecord, MAX_INLINE_STORED_BYTES, MAX_RECORD_BYTES,
};
use laststore::disk_run::home::RunRead;

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
    if inline_files::record_key(raw).is_some() {
        InlineFile::decode(body).map_err(|_| invalid("run backup inline file row"))?;
        return Ok(false);
    }
    if !raw.starts_with(RECORD_PREFIX) {
        return Ok(false);
    }
    if body.len() > MAX_RECORD_BYTES {
        return Err(invalid("run backup record exceeds its bound"));
    }
    // The charged decoder bounds the tree before the strict record decoder.
    drop(super::budget_json::decode(body)?);
    let record = StoredRecord::decode(body).map_err(|_| invalid("run backup record format"))?;
    if let Some(marker) = &record.inline_file {
        inline_file_in_backup(proof, raw, marker)?;
    }
    let Some(version) = record.file_version else {
        return Ok(false);
    };
    let path = crate::storage::current_keys::families::record_file_path(raw, &version)
        .map_err(|_| invalid("run backup record file path"))?;
    match proof.cas {
        Some(cas) => super::phase(super::Stage::NativeCas, file_in_backup(cas, &path))?,
        None => marker_has_blob_path(proof.main, raw, &version)?,
    }
    Ok(true)
}

/// A personal record file is fetched on request from its cloud path under
/// the database identity (the main home identity). The marker must derive it.
fn marker_has_blob_path(
    main: &RunRead,
    record: &str,
    version: &lastdb_model::portable_mutation::Position,
) -> StorageResult<()> {
    let database = cloud_backup_protocol::Id::try_from(main.home_id().to_string())
        .map_err(|_| invalid("run backup record blob path"))?;
    crate::storage::current_keys::families::record_blob_path(&database, record, version)
        .map_err(|_| invalid("run backup record blob path"))?;
    Ok(())
}

/// The file must be a live row of the authenticated CAS run. The read streams
/// the row through, so the run reader checks its blocks, and keeps nothing.
fn file_in_backup(cas: &RunRead, path: &str) -> StorageResult<()> {
    let key = native_routing::encode(path.as_bytes());
    let mut read =
        super::fork_mapped(cas, read_budget(budget::NATIVE_STREAM_LOOKUP_READ_BYTES, 1))?;
    read.copy_body_to(
        crate::storage::disk_pack::CAS_COLLECTION,
        key.as_bytes(),
        &mut std::io::sink(),
    )
    .map_err(map)?
    .ok_or_else(|| invalid("run backup record file missing"))?;
    Ok(())
}

/// The inline file row of a record must be in the same run and match the
/// record's marker.
fn inline_file_in_backup(
    proof: &Proof<'_>,
    record: &str,
    marker: &FileMarker,
) -> StorageResult<()> {
    let key = native_routing::encode(inline_files::address(record).as_bytes());
    let mut read = super::fork_mapped(
        proof.main,
        read_budget(budget::NATIVE_STREAM_LOOKUP_READ_BYTES, 1),
    )?;
    let mut body = Vec::new();
    read.copy_body_to(
        crate::storage::format::RECORDS_COLLECTION,
        key.as_bytes(),
        &mut body,
    )
    .map_err(map)?
    .ok_or_else(|| invalid("run backup inline file missing"))?;
    if body.len() > MAX_INLINE_STORED_BYTES {
        return Err(invalid("run backup inline file exceeds its bound"));
    }
    inline_files::decode_for(&body, marker)
        .map_err(|_| invalid("run backup inline file differs from its record"))?;
    Ok(())
}
