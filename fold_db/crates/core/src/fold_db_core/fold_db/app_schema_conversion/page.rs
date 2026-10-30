use super::*;

pub(super) async fn execute(
    db: &FoldDB,
    store: &dyn KvStore,
    mut change: Change,
) -> Result<Change, SchemaError> {
    if change.phase != Phase::Backfill {
        return Err(invalid("conversion does not accept another backfill page"));
    }
    let Some(target) = change.targets.get_mut(change.position) else {
        return Ok(change);
    };
    db.db_ops
        .molecule_keys()
        .load_schema(&target.source)
        .await?;
    db.db_ops
        .molecule_keys()
        .load_schema(&target.target)
        .await?;
    let (keys, after) = tips::keys(db, target).await?;
    if keys.is_empty() {
        if target.observed != target.expected
            || target.processed != target.expected[primary(&target.source)?]
        {
            return Err(invalid(
                "conversion key pages do not cover every frozen source field slot",
            ));
        }
        change.position += 1;
    } else {
        tips::copy(db, target, &keys).await?;
        target.processed += keys.len() as u64;
        target.after = after;
        if target.processed > target.expected[primary(&target.source)?] {
            return Err(invalid("conversion exceeds the frozen primary key count"));
        }
    }
    save(store, &change).await?;
    Ok(change)
}
