//! Alternate declarations need a home format that stores them.
//!
//! Fresh homes start at format 6, or at format 7 (the record model) on request.
//! A format 4 or 5 home stays readable, but no command upgrades it in place:
//! copy its records into a fresh home instead.

use super::{run_admission, PackRuntime, StorageError, StorageResult, NODE_MARKER};

impl PackRuntime {
    pub(crate) fn require_alternate_format(&self, compound: bool) -> StorageResult<()> {
        let root = self
            .physical
            .path()
            .parent()
            .ok_or_else(|| invalid("native node parent is absent"))?;
        let bytes = std::fs::read(root.join(NODE_MARKER))?;
        let current =
            bytes == run_admission::TYPED_NODE_BYTES || bytes == run_admission::RECORD_NODE_BYTES;
        if !current && (compound || bytes != run_admission::FORMAT_FIVE_BYTES) {
            return Err(invalid(
                "this home predates these alternate declarations; copy its records into a fresh home",
            ));
        }
        Ok(())
    }
}

fn invalid(message: &str) -> StorageError {
    StorageError::InvalidOperation(message.into())
}
