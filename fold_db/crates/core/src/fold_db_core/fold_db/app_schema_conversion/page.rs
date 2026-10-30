use super::*;

pub(super) async fn execute(
    db: &FoldDB,
    store: &dyn KvStore,
    mut change: Change,
) -> Result<Change, SchemaError> {
    if !matches!(
        change.phase,
        Phase::Backfill | Phase::Verifying | Phase::Verified
    ) {
        return Err(invalid("conversion does not accept another backfill page"));
    }
    let Some(target) = change.targets.get_mut(change.position) else {
        return audit::verify(store, change).await;
    };
    db.db_ops
        .molecule_keys()
        .load_schema(&target.source)
        .await?;
    db.db_ops
        .molecule_keys()
        .load_schema(&target.target)
        .await?;
    let done = if target.retained_coordinates {
        opaque_hash::page(db, store, &change.id, target).await?
    } else {
        let (keys, after) = tips::keys(db, target).await?;
        if !keys.is_empty() {
            tips::copy(db, store, &change.id, target, &keys).await?;
            target.processed += keys.len() as u64;
            target.after = after;
        }
        keys.is_empty()
    };
    if target.processed > target.source_keys()? {
        return Err(invalid("conversion exceeds the frozen source key count"));
    }
    if done {
        if target.observed != target.expected
            || target.processed != target.source_keys()?
            || target.selected != membership::expected(target)?
        {
            return Err(invalid(
                "conversion key pages do not cover every frozen source field slot",
            ));
        }
        change.position += 1;
    }
    save(store, &change).await?;
    Ok(change)
}
