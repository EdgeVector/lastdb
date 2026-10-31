//! The file dependencies of the records of a format 7 home.
//!
//! A record has no atom body and no `fdep` directory row. Each field value
//! that holds a file pointer names its blobs, and the proof copies and opens
//! each one from the authenticated CAS run, as it does for an atom. A field
//! history entry (`rh:`) holds a copy of one old value, and the proof checks
//! the blobs of that copy the same way.
use super::{files, invalid, Proof, StorageResult};
use crate::records::atom_history::{RecordHistoryEntry, RECORD_HISTORY_PREFIX};
use crate::storage::disk_pack::native_routing;
use lastdb_model::record_space::RECORD_PREFIX;
use lastdb_model::record_value::{StoredRecord, MAX_RECORD_BYTES};

/// A history entry holds one value of a record (at most the record cap) and
/// its stamp, schema, field and keys.
const MAX_HISTORY_ENTRY_BYTES: usize = MAX_RECORD_BYTES + 4 * 1024;

/// Prove the file dependencies of the record or the record history entry at
/// one tips row. False for a row that is neither; the caller then classifies
/// it.
pub(super) fn verify(proof: &Proof<'_>, key: &str, body: &[u8]) -> StorageResult<bool> {
    let raw = native_routing::decode(key)?;
    let raw = std::str::from_utf8(&raw).map_err(|_| invalid("run backup record key"))?;
    let Ok(bare) = crate::storage::current_keys::source_key(raw) else {
        return Ok(false);
    };
    let history = bare.starts_with(RECORD_HISTORY_PREFIX);
    if !history && !bare.starts_with(RECORD_PREFIX) {
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
    Ok(true)
}
