//! Small rules of the record write: the erasures that must find their record,
//! and the file rows of `put-blob`.

use crate::schema::types::operations::MutationType;
use crate::schema::types::{FileWrite, Mutation};
use lastdb_model::record_value::StoredRecord;
use std::sync::Arc;

/// The sealed file row of each mutation of a group, in mutation order.
pub(super) type FileRows = Vec<Option<Arc<Vec<u8>>>>;

/// The miss wording of a delete that must find its record: a purge, or a
/// delete with `must_exist`. `None` for every other mutation.
pub(super) fn loud_erasure(mutation: &Mutation) -> Option<(&'static str, &'static str)> {
    match mutation.mutation_type {
        MutationType::Purge => Some(("Purge", "(compliance verb)")),
        MutationType::Delete if mutation.must_exist == Some(true) => {
            Some(("Delete", "(must_exist)"))
        }
        _ => None,
    }
}

/// True for a compliance purge of a deleted record. The merge changes nothing,
/// but the purge still releases the field history of the record. The
/// precondition check refused a deleted record with no history. A TTL sweep
/// purge carries its cutoff and never takes this path.
pub(super) fn purges_deleted_record(mutation: &Mutation, before: Option<&StoredRecord>) -> bool {
    mutation.mutation_type == MutationType::Purge
        && mutation.erase_written_before_nanos.is_none()
        && before.is_some_and(|record| !record.is_live())
}

/// Move the sealed row out of each `put-blob` mutation, so the batch takes the
/// one copy and the mutation keeps only the marker.
pub(super) fn take_file_rows(mutations: &mut [Mutation]) -> FileRows {
    mutations
        .iter_mut()
        .map(|mutation| match mutation.file_write.as_mut() {
            Some(FileWrite::Put { row, .. }) => Some(std::mem::take(row)),
            _ => None,
        })
        .collect()
}
