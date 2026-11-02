//! The file dependencies of the records of a format 7 home.
//!
//! A record has no separate body row and no `fdep` directory row. Each field
//! value that holds a file pointer names its blobs. The proof copies and opens
//! each blob from the authenticated CAS run. A field history entry (`rh:`)
//! holds a copy of one old value, and the proof checks the blobs of that copy
//! the same way.
use super::{files, invalid, rows::Rows, Proof, StorageResult};
use crate::records::field_history::{RecordHistoryEntry, RECORD_HISTORY_PREFIX};
use crate::storage::disk_pack::native_routing;
use lastdb_model::record_space::RECORD_PREFIX;
use lastdb_model::record_value::{StoredRecord, MAX_RECORD_BYTES};

/// A history entry holds one value of a record (at most the record cap) and
/// its stamp, schema, field and keys.
const MAX_HISTORY_ENTRY_BYTES: usize = MAX_RECORD_BYTES + 4 * 1024;

/// Prove every record and record history entry of the `records` collection.
/// A row that is not a record must still pass the current key law.
/// A retired prefix fails that law. Other rows name no file.
pub(super) fn verify_records(proof: &Proof<'_>) -> StorageResult<()> {
    let mut read = super::rows::read(proof.main, proof.key_heads)?;
    let mut rows = Rows::new(proof.main, read.as_mut(), "records")?;
    while let Some((key, body)) = rows.next()? {
        verify(proof, &key, &body)?;
    }
    Ok(())
}

/// Prove the file dependencies of the record or the record history entry at
/// one row of the `records` collection.
fn verify(proof: &Proof<'_>, key: &str, body: &[u8]) -> StorageResult<()> {
    let raw = native_routing::decode(key)?;
    let raw = std::str::from_utf8(&raw).map_err(|_| invalid("run backup record key"))?;
    let Ok(bare) = crate::storage::current_keys::source_key(raw) else {
        return Ok(());
    };
    let history = bare.starts_with(RECORD_HISTORY_PREFIX);
    if !history && !bare.starts_with(RECORD_PREFIX) {
        crate::storage::current_keys::validate("main", raw.as_bytes())?;
        return Ok(());
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
    drop(files::decode_bounded_json(body)?);
    let dependencies = if history {
        let entry: RecordHistoryEntry = serde_json::from_slice(body)
            .map_err(|_| invalid("run backup record history format"))?;
        if entry.v != RecordHistoryEntry::VERSION {
            return Err(invalid("run backup record history version"));
        }
        files::expected_values(std::iter::once(&entry.value))?
    } else {
        let record = StoredRecord::decode(body).map_err(|_| invalid("run backup record format"))?;
        files::expected_record(&record)?
    };
    for dependency in dependencies {
        super::phase(
            super::Stage::NativeCas,
            super::native::verify(proof, &dependency),
        )?;
    }
    Ok(())
}
