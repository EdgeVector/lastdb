//! Refuse unvalidated restore cuts without repairing either marker or counters.
use super::{invalid, Inner, StorageResult};

pub(super) fn require_validated(inner: &Inner) -> StorageResult<()> {
    if let Some(run) = &inner.run {
        run.runtime.require_restore_validated()?;
        let (mut main, _) = run.reads(super::super::budget())?;
        let key = super::super::restore_safety::PENDING_KEY.as_bytes();
        let collection = crate::storage::laststore::native_routing::collection_for_key(key);
        let key = crate::storage::laststore::native_routing::encode(key);
        if main
            .get_bounded(collection, &key, 8192)
            .map_err(super::super::map)?
            .is_some()
        {
            return Err(invalid(
                "run backup retained restore counters are unverified",
            ));
        }
        return Ok(());
    }
    let home = inner.home.home();
    if super::super::restore_safety::pending(home, home.path())? {
        return Err(invalid(
            "byte backup source restore counters are unverified",
        ));
    }
    let source = inner
        .source
        .as_ref()
        .ok_or_else(|| invalid("byte retained source unavailable"))?;
    // The live wrapper/latest check cannot validate a previously retained cut.
    // Read the same lifecycle marker under that cut's own generation hold.
    let mut reader = inner.home.read(source, super::super::budget())?;
    let key = super::super::restore_safety::PENDING_KEY.as_bytes();
    let collection = crate::storage::laststore::native_routing::collection_for_key(key);
    let key = crate::storage::laststore::native_routing::encode(key);
    if reader
        .get_bounded(collection, &key, 8192)
        .map_err(super::super::map)?
        .is_some()
    {
        return Err(invalid(
            "byte backup retained restore counters are unverified",
        ));
    }
    Ok(())
}
