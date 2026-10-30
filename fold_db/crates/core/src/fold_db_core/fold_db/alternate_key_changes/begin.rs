use super::*;

pub(super) async fn execute(
    db: &FoldDB,
    store: &dyn KvStore,
    name: &str,
    revision: &str,
    alternate_keys: BTreeMap<String, schema_types::AlternateKey>,
) -> Result<Change, SchemaError> {
    if let Some(bytes) = store.get(job_key(name).as_bytes()).await? {
        let existing: Change = serde_json::from_slice(&bytes)
            .map_err(|_| invalid("alternate change is unreadable"))?;
        if !matches!(existing.phase, Phase::Complete | Phase::Aborted) {
            return Err(invalid(
                "finish or abort the existing alternate change first",
            ));
        }
    }
    let mut source = db
        .db_ops
        .get_schema(name)
        .await?
        .ok_or_else(|| invalid("schema is absent"))?;
    if source.alternate_key_revision() != revision
        || db.schema_manager.get_schema_state_cached(name)? != SchemaState::Available
    {
        return Err(invalid("schema revision or availability differs"));
    }
    source.populate_runtime_fields()?;
    source.clear_runtime_molecules();
    let mut target = source.clone_for_read_shared();
    target.alternate_keys = alternate_keys;
    target.independent_fields = true;
    target
        .validate_alternate_keys()
        .map_err(SchemaError::InvalidData)?;
    if target.alternate_key_revision() == source.alternate_key_revision() {
        return Err(invalid(
            "the requested alternate declarations are already active",
        ));
    }
    let expected = source_counts(db, &source).await?;
    let observed = expected.keys().cloned().map(|name| (name, 0)).collect();
    let change = Change {
        id: uuid::Uuid::new_v4().to_string(),
        source,
        target,
        phase: Phase::Backfill,
        expected,
        observed,
        processed: 0,
    };
    db.db_ops
        .molecule_keys()
        .establish_schema_for_publication(&change.target, Some(&change.source))
        .await?;
    db.db_ops
        .atoms()
        .add_molecule_bindings(&change.staging_edges(), None)
        .await?;
    // Freeze ordinary writers before the first page. The native schema snapshot
    // and staged management cache change with the same durable root as this job.
    let mut frozen = change.source.clone_for_read_shared();
    frozen.independent_fields = true;
    db.db_ops.store_schema(name, &frozen).await?;
    db.db_ops
        .store_schema_state(name, &SchemaState::Blocked)
        .await?;
    db.schema_manager
        .publish_schema_cache(name, frozen, SchemaState::Blocked)?;
    save(store, &change).await?;
    Ok(change)
}

async fn source_counts(db: &FoldDB, source: &Schema) -> Result<BTreeMap<String, u64>, SchemaError> {
    let molecules: Vec<_> = source
        .runtime_fields
        .iter()
        .map(|(name, field)| {
            let id = field
                .common()
                .molecule_uuid()
                .cloned()
                .ok_or_else(|| invalid("source field has no molecule"))?;
            if id != crate::atom::deterministic_molecule_uuid(&source.name, name) {
                return Err(invalid("alternate changes require owned field molecules"));
            }
            Ok(id)
        })
        .collect::<Result<_, _>>()?;
    let counters = db
        .db_ops
        .atoms()
        .molecule_counter_views(&molecules, None)
        .await?;
    if counters.len() != molecules.len()
        || counters
            .iter()
            .any(|counter| counter.suspect || counter.bindings != 1)
    {
        return Err(invalid(
            "source field counters are suspect or another schema/database binds a field",
        ));
    }
    let pointers = molecules
        .iter()
        .map(|id| crate::atom::molecule_key_codec::molecule_generation_pointer_key(id).into_bytes())
        .collect();
    if db
        .db_ops
        .open_namespace("main")
        .await?
        .exists_many(pointers)
        .await?
        .into_iter()
        .any(|exists| exists)
    {
        return Err(invalid(
            "source fields with existing molecule generations require explicit retirement first",
        ));
    }
    let expected: BTreeMap<_, _> = source
        .runtime_fields
        .keys()
        .cloned()
        .zip(counters.iter().map(|counter| counter.slots))
        .collect();
    Ok(expected)
}
