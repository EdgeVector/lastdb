//! The records of a home and their files.
//!
//! A record has no separate body row and no file dependency row. A record
//! with a file marker has one file, at a path made from its key. The proof
//! checks that the path is a live row of the authenticated CAS run. A field
//! history entry (`rh:`) holds a copy of one old field value and names no
//! file.
use super::{budget, invalid, map, read_budget, rows::Rows, Proof, StorageResult};
use crate::records::field_history::{record_history_suffix, RecordHistoryEntry};
use crate::storage::disk_pack::native_routing;
use crate::storage::format::MAX_HISTORY_ENTRY_BYTES;
use lastdb_model::record_space::RECORD_PREFIX;
use lastdb_model::record_value::{StoredRecord, MAX_RECORD_BYTES};

/// Prove every record and record history entry of the `records` collection,
/// and return the number of record files found. A row that is not a record
/// must still pass the current key law. A retired prefix fails that law.
/// Other rows name no file.
pub(super) fn verify_records(proof: &Proof<'_>) -> StorageResult<u64> {
    let mut read = super::rows::read(proof.main, proof.key_heads)?;
    let mut rows = Rows::new(proof.main, read.as_mut(), "records")?;
    let mut files = 0u64;
    while let Some((key, body)) = rows.next()? {
        files += u64::from(verify(proof, &key, &body)?);
    }
    Ok(files)
}

/// Prove the record or the record history entry at one row of the `records`
/// collection, and the record's file. Returns true when the record has a file.
fn verify(proof: &Proof<'_>, key: &str, body: &[u8]) -> StorageResult<bool> {
    let raw = native_routing::decode(key)?;
    let raw = std::str::from_utf8(&raw).map_err(|_| invalid("run backup record key"))?;
    let history = record_history_suffix(raw).is_some();
    if !history && !raw.starts_with(RECORD_PREFIX) {
        crate::storage::current_keys::validate("main", raw.as_bytes())?;
        return Ok(false);
    }
    crate::storage::current_keys::validate("main", raw.as_bytes())?;
    let cap = if history {
        MAX_HISTORY_ENTRY_BYTES
    } else {
        MAX_RECORD_BYTES
    };
    if body.len() > cap {
        return Err(invalid("run backup record exceeds its bound"));
    }
    // The charged decoder bounds the tree before the strict record decoder.
    drop(super::budget_json::decode(body)?);
    if history {
        let entry: RecordHistoryEntry = serde_json::from_slice(body)
            .map_err(|_| invalid("run backup record history format"))?;
        if entry.v != RecordHistoryEntry::VERSION {
            return Err(invalid("run backup record history version"));
        }
        return Ok(false);
    }
    let record = StoredRecord::decode(body).map_err(|_| invalid("run backup record format"))?;
    if record.file.is_none() {
        return Ok(false);
    }
    let path = crate::storage::current_keys::families::record_file_path(raw)
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
