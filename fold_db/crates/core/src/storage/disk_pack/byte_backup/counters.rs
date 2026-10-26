//! Refuse unvalidated restore cuts without repairing either marker or counters.
use super::{invalid, Inner, StorageResult};

pub(super) fn require_validated(inner: &Inner) -> StorageResult<()> {
    let run = inner
        .run
        .as_ref()
        .ok_or_else(|| invalid("byte retained source unavailable"))?;
    run.runtime.require_restore_validated()?;
    let (mut main, _) = run.reads(super::super::budget())?;
    let key = super::super::restore_safety::PENDING_KEY.as_bytes();
    let collection = crate::storage::disk_pack::native_routing::collection_for_key(key);
    let key = crate::storage::disk_pack::native_routing::encode(key);
    if main
        .get_bounded(collection, &key, 8192)
        .map_err(super::super::map)?
        .is_some()
    {
        return Err(invalid(
            "run backup retained restore counters are unverified",
        ));
    }
    Ok(())
}
