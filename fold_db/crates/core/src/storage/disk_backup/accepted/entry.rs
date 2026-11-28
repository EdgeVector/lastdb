//! Stage one entry that is not a record or schema write, such as a grant, a lease or a
//! heads entry, as the next entry of this device's stream.
//!
//! The store does not build these entries. The caller signs the whole entry and the store
//! puts it in the spool, in the commit that moves the chain row. The id of the spool note
//! comes from the kind and the order value (see `body::entry_id`), so it is new.

use super::author::{LocalEntry, SignedLocal};
use super::body;
use super::capture::capture_enabled;
use super::disk::Note;
use crate::storage::disk_pack::{transaction, PackRuntime};
use crate::storage::error::{StorageError, StorageResult};

/// Sign one entry of kind `kind` as the entry after `previous`, with the order value
/// `order`, and stage it for the spool. `sign` gets `previous` and `order` and returns
/// the signed entry. Refused when the store does not capture its writes.
pub(crate) fn stage_log_entry<F>(
    runtime: &PackRuntime,
    kind: &str,
    previous: Option<&LocalEntry>,
    order: u64,
    sign: F,
) -> StorageResult<LocalEntry>
where
    F: FnOnce(Option<&LocalEntry>, u64) -> Result<SignedLocal, String>,
{
    if !capture_enabled(runtime) {
        return Err(StorageError::InvalidOperation(
            "this store does not capture its writes".into(),
        ));
    }
    let seq = previous
        .map_or(Some(1), |held| held.seq.checked_add(1))
        .ok_or_else(|| {
            StorageError::InvalidOperation("the device stream sequence is exhausted".into())
        })?;
    let signed = sign(previous, order).map_err(|detail| {
        StorageError::InvalidOperation(format!("the entry cannot be signed: {detail}"))
    })?;
    let note = Note {
        seq,
        mutation_id: body::entry_id(kind, order),
        envelope: signed.entry,
        file: None,
    };
    transaction::stage_accepted(runtime, vec![note])?;
    Ok(LocalEntry {
        seq,
        hash: signed.hash,
        order,
    })
}
