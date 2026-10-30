use super::*;

pub(super) async fn commit(
    db: &FoldDB,
    store: &dyn KvStore,
    mut change: Change,
) -> Result<Change, SchemaError> {
    if matches!(change.phase, Phase::Retiring | Phase::Ready) {
        return Ok(change);
    }
    if change.phase != Phase::Verified || change.position != change.targets.len() {
        return Err(invalid(
            "conversion must cover all frozen source keys before publication",
        ));
    }
    for target in &change.targets {
        if target.observed != target.expected
            || begin::counts(db, &target.source).await? != target.expected
        {
            return Err(invalid(
                "conversion source fields changed or coverage differs",
            ));
        }
        db.db_ops
            .store_schema(&target.target.name, &target.target)
            .await?;
        db.db_ops
            .store_schema_state(&target.target.name, &SchemaState::Blocked)
            .await?;
        db.schema_manager.publish_schema_cache(
            &target.target.name,
            target.target.clone_for_read_shared(),
            SchemaState::Blocked,
        )?;
    }
    change.phase = Phase::Retiring;
    save(store, &change).await?;
    Ok(change)
}

/// Each supported drop completes before its progress mark. Repeating a drop
/// after a crash is idempotent; no cursor can skip an unfinished schema.
pub(super) async fn retire(db: &FoldDB, id: &str) -> Result<(), SchemaError> {
    let store = db.db_ops.open_namespace(NAMESPACE).await?;
    let change = load(store.as_ref(), id).await?;
    if change.phase == Phase::Ready {
        return Ok(());
    }
    if change.phase != Phase::Retiring {
        return Err(invalid(
            "only a published conversion retires legacy schemas",
        ));
    }
    let names: Vec<_> = change
        .retire
        .iter()
        .skip(change.retired)
        .take(4)
        .cloned()
        .collect();
    for name in &names {
        db.drop_schema_and_bindings(name)
            .await
            .map_err(|error| invalid(&format!("retire legacy schema: {error}")))?;
    }
    transaction::scope(db.db_ops.namespaced_store().disk_pack_runtime(), async {
        let store = db.db_ops.open_namespace(NAMESPACE).await?;
        let mut current = load(store.as_ref(), id).await?;
        if current.phase != Phase::Retiring {
            return Err(invalid("conversion phase changed during retirement"));
        }
        current.retired = current.retired.max(change.retired + names.len());
        if current.retired == current.retire.len() && audit::cleanup(store.as_ref(), id).await? {
            db.db_ops
                .atoms()
                .remove_molecule_bindings(&current.edges(), None)
                .await?;
            current.phase = Phase::Ready;
        }
        save(store.as_ref(), &current).await
    })
    .await
}

pub(super) async fn abort(
    db: &FoldDB,
    store: &dyn KvStore,
    mut change: Change,
) -> Result<Change, SchemaError> {
    if !matches!(
        change.phase,
        Phase::Backfill | Phase::Verifying | Phase::Verified | Phase::Aborting
    ) {
        return Err(invalid(
            "a published conversion cannot be aborted; finish it",
        ));
    }
    change.phase = Phase::Aborting;
    let done = super::cleanup::shadow(db, &mut change).await?;
    if done && audit::cleanup(store, &change.id).await? {
        db.db_ops
            .atoms()
            .remove_molecule_bindings(&change.edges(), None)
            .await?;
        restore_states(db, &change, false).await?;
        store.delete(ACTIVE).await?;
        db.publish_conversion_flag(false)?;
        change.phase = Phase::Aborted;
    }
    save(store, &change).await?;
    Ok(change)
}

pub(super) async fn complete(
    db: &FoldDB,
    store: &dyn KvStore,
    mut change: Change,
) -> Result<Change, SchemaError> {
    if change.phase == Phase::Complete {
        return Ok(change);
    }
    if change.phase != Phase::Ready {
        return Err(invalid(
            "finish requires published keys and completed legacy retirement",
        ));
    }
    restore_states(db, &change, true).await?;
    store.delete(ACTIVE).await?;
    db.publish_conversion_flag(false)?;
    change.phase = Phase::Complete;
    save(store, &change).await?;
    Ok(change)
}

async fn restore_states(db: &FoldDB, change: &Change, committed: bool) -> Result<(), SchemaError> {
    for (name, state) in &change.states {
        if committed && change.retire.contains(name) {
            continue;
        }
        let schema = db
            .db_ops
            .get_schema(name)
            .await?
            .ok_or_else(|| invalid("a frozen schema disappeared before conversion finished"))?;
        db.db_ops.store_schema_state(name, state).await?;
        db.schema_manager
            .publish_schema_cache(name, schema, *state)?;
    }
    Ok(())
}
