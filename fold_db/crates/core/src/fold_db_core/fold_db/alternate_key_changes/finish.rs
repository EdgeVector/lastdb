use super::*;

pub(super) async fn commit(
    db: &FoldDB,
    store: &dyn KvStore,
    mut change: Change,
) -> Result<Change, SchemaError> {
    if matches!(change.phase, Phase::Retiring | Phase::Complete) {
        return Ok(change);
    }
    if change.phase != Phase::Backfill
        || change.observed != change.expected
        || change.processed
            != change
                .expected
                .get(primary(&change.source)?)
                .copied()
                .unwrap_or_default()
    {
        return Err(invalid(
            "backfill does not cover every frozen source field slot",
        ));
    }
    let fields: Vec<_> = change.source.runtime_fields.iter().collect();
    let molecules: Vec<_> = fields
        .iter()
        .map(|(_, field)| {
            field
                .common()
                .molecule_uuid()
                .cloned()
                .ok_or_else(|| invalid("source molecule is absent"))
        })
        .collect::<Result<_, _>>()?;
    let counters = db
        .db_ops
        .atoms()
        .molecule_counter_views(&molecules, None)
        .await?;
    if counters.len() != fields.len()
        || fields.iter().zip(counters).any(|((name, _), counter)| {
            counter.suspect
                || counter.bindings != 1
                || change.expected.get(*name) != Some(&counter.slots)
        })
    {
        return Err(invalid("source fields or bindings changed during backfill"));
    }
    db.db_ops
        .store_schema(&change.target.name, &change.target)
        .await?;
    db.db_ops
        .store_schema_state(&change.target.name, &SchemaState::Available)
        .await?;
    db.schema_manager.publish_schema_cache(
        &change.target.name,
        change.target.clone_for_read_shared(),
        SchemaState::Available,
    )?;
    db.db_ops
        .atoms()
        .remove_molecule_bindings(&change.staging_edges(), None)
        .await?;
    change.phase = Phase::Retiring;
    save(store, &change).await?;
    Ok(change)
}

pub(super) async fn abort(
    db: &FoldDB,
    store: &dyn KvStore,
    mut change: Change,
) -> Result<Change, SchemaError> {
    if change.phase == Phase::Aborted {
        return Ok(change);
    }
    if !matches!(change.phase, Phase::Backfill | Phase::Aborting) {
        return Err(invalid(
            "a published alternate change cannot be undone; begin a new change",
        ));
    }
    change.phase = Phase::Aborting;
    save(store, &change).await?;
    cleanup(db, store, change).await
}

pub(super) async fn cleanup(
    db: &FoldDB,
    store: &dyn KvStore,
    mut change: Change,
) -> Result<Change, SchemaError> {
    if matches!(change.phase, Phase::Complete | Phase::Aborted) {
        return Ok(change);
    }
    if !matches!(change.phase, Phase::Retiring | Phase::Aborting) {
        return Err(invalid(
            "only a committed or aborted change can retire its temporary entries",
        ));
    }
    let retired = if change.phase == Phase::Retiring {
        &change.source
    } else {
        &change.target
    };
    let mut deletes = Vec::new();
    let atoms = db.db_ops.atoms();
    for name in retired.alternate_keys.keys() {
        let molecule = crate::alternate_keys::molecule(retired, name);
        let counters = atoms
            .molecule_counter_views(std::slice::from_ref(&molecule), None)
            .await?;
        let expected_bindings = u64::from(change.phase == Phase::Aborting);
        if counters
            .iter()
            .any(|counter| counter.suspect || counter.bindings != expected_bindings)
        {
            return Err(invalid(
                "another owner still binds a retired alternate molecule",
            ));
        }
        let prefix = format!("ak:{molecule}:");
        let rows = atoms
            .raw()
            .inner()
            .scan_prefix_paged(prefix.as_bytes(), 128)
            .await?;
        deletes.extend(rows.into_iter().map(|(key, _)| KvMutation::delete(key)));
    }
    let done_entries = deletes.is_empty();
    atoms.raw().inner().batch_mutate(deletes).await?;
    let prefix = format!("seen:{}:", change.id);
    let receipts = store.scan_prefix_paged(prefix.as_bytes(), 256).await?;
    let done_receipts = receipts.is_empty();
    store
        .batch_mutate(
            receipts
                .into_iter()
                .map(|(key, _)| KvMutation::delete(key))
                .collect(),
        )
        .await?;
    if done_entries && done_receipts {
        if change.phase == Phase::Aborting {
            db.db_ops
                .store_schema(&change.source.name, &change.source)
                .await?;
            db.db_ops
                .store_schema_state(&change.source.name, &SchemaState::Available)
                .await?;
            db.schema_manager.publish_schema_cache(
                &change.source.name,
                change.source.clone_for_read_shared(),
                SchemaState::Available,
            )?;
            atoms
                .remove_molecule_bindings(&change.staging_edges(), None)
                .await?;
            change.phase = Phase::Aborted;
        } else {
            change.phase = Phase::Complete;
        }
    }
    save(store, &change).await?;
    Ok(change)
}
