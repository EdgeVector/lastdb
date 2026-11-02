//! Refuse unvalidated restore cuts without repairing the counters.
use super::{invalid, Inner, StorageResult};

pub(super) fn require_validated(inner: &Inner) -> StorageResult<()> {
    inner
        .run
        .as_ref()
        .ok_or_else(|| invalid("byte retained source unavailable"))?
        .runtime
        .require_restore_validated()
}
