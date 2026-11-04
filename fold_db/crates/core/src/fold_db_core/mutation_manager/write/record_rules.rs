//! Small rules of the record write: the erasures that must find their record,
//! and the file rows of `put-blob`.

use crate::schema::types::{FileWrite, Mutation};

/// The intact opaque file operation of each mutation, in mutation order.
pub(super) type FileWrites = Vec<Option<FileWrite>>;

/// Move the complete operation without separating its marker from its bytes.
pub(super) fn take_file_writes(mutations: &mut [Mutation]) -> FileWrites {
    mutations
        .iter_mut()
        .map(|mutation| mutation.file_write.take())
        .collect()
}
