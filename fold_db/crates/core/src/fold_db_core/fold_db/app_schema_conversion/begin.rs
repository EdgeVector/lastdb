use super::*;

pub(super) async fn execute(
    db: &FoldDB,
    store: &dyn KvStore,
    requests: Vec<OwnedSchemaConversion>,
    retire: Vec<String>,
    maintenance: bool,
) -> Result<Change, SchemaError> {
    if !maintenance || requests.is_empty() || requests.len() > 64 || retire.len() > 128 {
        return Err(invalid("conversion requires explicit maintenance, 1 to 64 kept schemas, and at most 128 retired schemas"));
    }
    if store.get(ACTIVE).await?.is_some() {
        return Err(invalid("finish or abort the active app conversion first"));
    }
    let runtime = db
        .db_ops
        .namespaced_store()
        .disk_pack_runtime()
        .ok_or_else(|| invalid("conversion requires native storage"))?;
    runtime.require_alternate_format(true)?;
    if !runtime.logical_capture_peer_positions().await?.is_empty() {
        return Err(invalid("app conversion requires a node without registered logical peers; coordinate the peer cutover separately"));
    }
    let all = db.db_ops.get_all_schemas().await?;
    if all.len() > 256 {
        return Err(invalid(
            "conversion maintenance supports at most 256 installed schemas",
        ));
    }
    let id = uuid::Uuid::new_v4().to_string();
    let Prepared {
        targets,
        selected,
        owners,
    } = prepare(db, &all, requests, &id).await?;
    let retired: BTreeSet<_> = retire.into_iter().collect();
    for name in &retired {
        let schema = all
            .get(name)
            .ok_or_else(|| invalid("retired schema is absent"))?;
        if selected.contains(name)
            || !schema
                .owner_app_id
                .as_ref()
                .is_some_and(|owner| owners.contains(owner))
        {
            return Err(invalid(
                "retired schemas must belong to the converted apps and cannot be kept sources",
            ));
        }
        if schema.uses_independent_fields() {
            return Err(invalid(
                "conversion does not retire an existing independent schema",
            ));
        }
        alias_retirement::validate(schema, &targets)?;
    }
    opaque_hash::validate_owners(db, &all, &targets, &retired).await?;
    if db
        .db_ops
        .db_catalog()
        .list_entries()
        .await?
        .iter()
        .any(|entry| selected.contains(&entry.schema_name) || retired.contains(&entry.schema_name))
    {
        return Err(invalid("conversion is personal-only; a kept or retired schema still belongs to a database catalog"));
    }
    let mut states = BTreeMap::new();
    for (name, schema) in all {
        states.insert(
            name.clone(),
            db.schema_manager.get_schema_state_cached(&name)?,
        );
        db.db_ops
            .store_schema_state(&name, &SchemaState::Blocked)
            .await?;
        db.schema_manager
            .publish_schema_cache(&name, schema, SchemaState::Blocked)?;
    }
    let change = Change {
        id,
        phase: Phase::Backfill,
        targets,
        states,
        retire: retired.into_iter().collect(),
        position: 0,
        retired: 0,
        cleaned: 0,
        audit_after: None,
        retirement_audit_version: 1,
    };
    db.db_ops
        .atoms()
        .add_molecule_bindings(&change.edges(), None)
        .await?;
    save(store, &change).await?;
    store.put(ACTIVE, change.id.as_bytes().to_vec()).await?;
    db.publish_conversion_flag(true)?;
    Ok(change)
}

struct Prepared {
    targets: Vec<Target>,
    selected: BTreeSet<String>,
    owners: BTreeSet<String>,
}

async fn prepare(
    db: &FoldDB,
    all: &std::collections::HashMap<String, Schema>,
    requests: Vec<OwnedSchemaConversion>,
    id: &str,
) -> Result<Prepared, SchemaError> {
    let mut targets = Vec::new();
    let selected: BTreeSet<_> = requests
        .iter()
        .map(|request| request.schema.clone())
        .collect();
    if selected.len() != requests.len() {
        return Err(invalid("conversion repeats a kept schema"));
    }
    let mut owners = BTreeSet::new();
    for request in requests {
        let source = all
            .get(&request.schema)
            .ok_or_else(|| invalid("conversion source is absent"))?;
        let owner = source
            .owner_app_id
            .as_ref()
            .filter(|owner| !owner.is_empty())
            .ok_or_else(|| invalid("conversion source is not app-owned"))?;
        owners.insert(owner.clone());
        if source.identity_hash != request.expected_identity
            || db.schema_manager.get_schema_state_cached(&source.name)? != SchemaState::Available
        {
            return Err(invalid(
                "conversion source identity or availability differs",
            ));
        }
        source_authority::validate(source, all, &selected)?;
        targets.push(target(db, source, request, id).await?);
    }
    Ok(Prepared {
        targets,
        selected,
        owners,
    })
}

async fn target(
    db: &FoldDB,
    source: &Schema,
    request: OwnedSchemaConversion,
    id: &str,
) -> Result<Target, SchemaError> {
    if (source.uses_independent_fields() && request.field_map.is_empty())
        || source
            .transform_fields
            .as_ref()
            .is_some_and(|fields| !fields.is_empty())
    {
        return Err(invalid("conversion requires ordinary fields; an independent source also requires an explicit field transition"));
    }
    let mut source = source.clone_for_read_shared();
    source.populate_runtime_fields()?;
    source.clear_runtime_molecules();
    if source.runtime_fields.len() > 128 {
        return Err(invalid("conversion source exceeds 128 fields"));
    }
    source.ensure_field_hashes();
    let retained_coordinates = opaque_hash::requested(&source, &request)?;
    membership::validate(db, &source, request.membership.as_ref()).await?;
    let expected = counts(db, &source).await?;
    let mut target = mapped_fields::definition(&source, &request)?;
    target.field_mappers = None;
    target.independent_fields = true;
    target.alternate_keys = request.alternate_keys;
    target.field_molecule_uuids = if retained_coordinates {
        source.field_molecule_uuids.clone()
    } else {
        Some(
            target
                .runtime_fields
                .keys()
                .map(|field| {
                    (
                        field.clone(),
                        crate::atom::deterministic_molecule_uuid(
                            &format!("{}:owned:{id}", source.name),
                            field,
                        ),
                    )
                })
                .collect(),
        )
    };
    target.runtime_fields.clear();
    target.populate_runtime_fields()?;
    target
        .validate_alternate_keys()
        .map_err(SchemaError::InvalidData)?;
    db.db_ops
        .molecule_keys()
        .establish_schema_for_publication(&target, None)
        .await?;
    let observed = expected.keys().cloned().map(|field| (field, 0)).collect();
    Ok(Target {
        source,
        target,
        expected,
        observed,
        processed: 0,
        after: None,
        membership: request.membership,
        selected: 0,
        target_definition_identity: request.target_definition_identity,
        field_map: request.field_map,
        retained_coordinates,
    })
}

pub(super) async fn counts(
    db: &FoldDB,
    source: &Schema,
) -> Result<BTreeMap<String, u64>, SchemaError> {
    let fields: Vec<_> = source.runtime_fields.iter().collect();
    let ids = fields
        .iter()
        .map(|(_, field)| {
            field
                .common()
                .molecule_uuid()
                .cloned()
                .ok_or_else(|| invalid("conversion source molecule is absent"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let counters = db.db_ops.atoms().molecule_counter_views(&ids, None).await?;
    if counters.len() != fields.len()
        || counters
            .iter()
            .any(|counter| counter.suspect || counter.records != 0)
    {
        return Err(invalid(
            "conversion source has suspect counters or kept history records",
        ));
    }
    let pointers: Vec<_> = ids
        .iter()
        .map(|id| crate::atom::molecule_key_codec::molecule_generation_pointer_key(id).into_bytes())
        .collect();
    if db
        .db_ops
        .atoms()
        .raw()
        .inner()
        .get_many(pointers)
        .await?
        .iter()
        .any(Option::is_some)
    {
        return Err(invalid(
            "conversion source uses a molecule generation; materialize it before conversion",
        ));
    }
    Ok(fields
        .into_iter()
        .zip(counters)
        .map(|((field, _), counter)| (field.clone(), counter.slots))
        .collect())
}
