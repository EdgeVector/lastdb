//! Retain an already-private external Hash coordinate without deblinding or rekeying.

use super::*;
use crate::db_operations::atom_store::PerKeyRecord;

pub(super) fn field(schema: &Schema) -> Result<&str, SchemaError> {
    let fields = schema.fields.as_deref().unwrap_or_default();
    if schema.schema_type != schema_types::DeclarativeSchemaType::Hash
        || schema
            .key
            .as_ref()
            .is_some_and(|key| key.hash_field.is_some() || key.range_field.is_some())
        || fields.len() != 1
        || schema.runtime_fields.len() != 1
        || !schema.alternate_keys.is_empty()
        || !schema.ref_fields.is_empty()
        || schema
            .field_mappers
            .as_ref()
            .is_some_and(|map| !map.is_empty())
        || schema
            .transform_fields
            .as_ref()
            .is_some_and(|map| !map.is_empty())
    {
        return Err(invalid("external Hash ownership requires one ordinary field, no alternate keys, references, transforms or value maps"));
    }
    let name = fields[0].as_str();
    let runtime = schema
        .runtime_fields
        .get(name)
        .ok_or_else(|| invalid("external Hash field is absent"))?;
    let ids = schema
        .field_molecule_uuids
        .as_ref()
        .ok_or_else(|| invalid("external Hash requires its stored molecule identity"))?;
    let expected = crate::atom::deterministic_molecule_uuid(&schema.name, name);
    if ids.len() != 1
        || ids.get(name) != Some(&expected)
        || runtime.common().molecule_uuid() != Some(&expected)
        || !schema.field_types.contains_key(name)
    {
        return Err(invalid(
            "external Hash ownership requires this schema's exact stored private molecule",
        ));
    }
    Ok(name)
}

pub(super) fn requested(
    source: &Schema,
    request: &OwnedSchemaConversion,
) -> Result<bool, SchemaError> {
    if source
        .key
        .as_ref()
        .and_then(|key| key.hash_field.as_ref())
        .is_some()
    {
        return Ok(false);
    }
    field(source)?;
    if !request.alternate_keys.is_empty()
        || request.membership.is_some()
        || request.target_definition_identity.is_some()
        || !request.field_map.is_empty()
    {
        return Err(invalid("external Hash ownership preserves every existing coordinate and its complete definition"));
    }
    Ok(true)
}

pub(super) fn validate_stored(target: &Target) -> Result<(), SchemaError> {
    if !target.retained_coordinates {
        return Ok(());
    }
    field(&target.source)?;
    field(&target.target)?;
    if target.membership.is_some()
        || target.target_definition_identity.is_some()
        || !target.field_map.is_empty()
    {
        return Err(invalid(
            "stored external Hash ownership changes its source scope",
        ));
    }
    let mut expected = target.source.to_wire();
    expected.independent_fields = true;
    expected.field_mappers = None;
    if serde_json::to_value(expected).map_err(|_| invalid("external Hash source metadata"))?
        != serde_json::to_value(target.target.to_wire())
            .map_err(|_| invalid("external Hash target metadata"))?
    {
        return Err(invalid(
            "external Hash ownership cannot change coordinates, fields or metadata",
        ));
    }
    Ok(())
}

pub(super) async fn validate_owners(
    db: &FoldDB,
    installed: &std::collections::HashMap<String, Schema>,
    targets: &[Target],
    retired: &BTreeSet<String>,
) -> Result<(), SchemaError> {
    for target in targets.iter().filter(|target| target.retained_coordinates) {
        validate_stored(target)?;
        let name = field(&target.source)?;
        let molecule = target
            .source
            .field_molecule_uuids
            .as_ref()
            .and_then(|ids| ids.get(name))
            .ok_or_else(|| invalid("external Hash molecule is absent"))?;
        let mut allowed = Vec::new();
        for stored in installed.values() {
            let mut schema = stored.clone_for_read_shared();
            schema.populate_runtime_fields()?;
            for (field, runtime) in &schema.runtime_fields {
                if runtime.common().molecule_uuid() != Some(molecule) {
                    continue;
                }
                if schema.name != target.source.name && !retired.contains(&schema.name) {
                    return Err(invalid(
                        "another schema still references the external Hash molecule",
                    ));
                }
                allowed.push(MoleculeRefEdge::schema_field(&schema.name, field, molecule));
            }
        }
        let counters = db
            .db_ops
            .atoms()
            .molecule_counter_views(std::slice::from_ref(molecule), None)
            .await?;
        if counters.len() != 1
            || counters[0].suspect
            || counters[0].bindings > (installed.len() * 128) as u64
        {
            return Err(invalid(
                "external Hash binding ownership exceeds the bounded schema set",
            ));
        }
        let actual = db
            .db_ops
            .atoms()
            .molecule_ref_edges_for_molecule(molecule, None)
            .await?;
        let own = MoleculeRefEdge::schema_field(&target.source.name, name, molecule);
        if !actual.complete
            || !actual.edges.contains(&own)
            || actual.edges.len() != allowed.len()
            || actual.edges.iter().any(|edge| !allowed.contains(edge))
        {
            return Err(invalid("external Hash ownership requires complete exclusive schema bindings; an unknown owner still binds its molecule"));
        }
    }
    Ok(())
}

pub(super) async fn page(
    db: &FoldDB,
    store: &dyn KvStore,
    id: &str,
    target: &mut Target,
) -> Result<bool, SchemaError> {
    validate_stored(target)?;
    let name = field(&target.source)?.to_string();
    let molecule = target
        .source
        .field_molecule_uuids
        .as_ref()
        .and_then(|ids| ids.get(&name))
        .ok_or_else(|| invalid("external Hash molecule is absent"))?;
    let prefix = format!("mk:{molecule}:");
    let (mut start, end) = crate::kind_partition::exact_prefix_bounds(&prefix);
    if let Some(after) = &target.after {
        if !after.starts_with(&prefix) {
            return Err(invalid("external Hash cursor names another molecule"));
        }
        start = format!("{after}\0");
    }
    let atoms = db.db_ops.atoms();
    let rows = atoms
        .raw()
        .inner()
        .scan_range_paged(start.as_bytes(), end.as_bytes(), 64)
        .await?;
    let codec = atoms.key_codec_for_molecule(molecule)?;
    let mut records = Vec::new();
    for (key, value) in &rows {
        let key = std::str::from_utf8(key)
            .map_err(|_| invalid("external Hash coordinate is unreadable"))?;
        let (hash, range) = crate::atom::molecule_key_codec::decode_hash_range(key, molecule)
            .ok_or_else(|| invalid("external Hash coordinate is invalid"))?;
        let range = codec
            .api_range(&range)
            .map_err(|_| invalid("external Hash range is invalid"))?;
        if hash.is_empty() || !range.is_empty() {
            return Err(invalid("external Hash source contains another key layout"));
        }
        records.push(
            serde_json::from_slice::<PerKeyRecord>(value)
                .map_err(|_| invalid("external Hash current tip is unreadable"))?,
        );
    }
    let ids: Vec<_> = records
        .iter()
        .map(|record| record.entry.atom_uuid.as_str())
        .collect();
    let found = atoms.get_atoms(&ids, None).await?;
    if found.len() != records.len() || found.iter().any(Option::is_none) {
        return Err(SchemaError::MissingSelectedAtom);
    }
    let identity = target
        .source
        .field_hashes
        .get(&name)
        .filter(|identity| !identity.is_empty())
        .ok_or_else(|| invalid("external Hash field identity is absent"))?;
    audit::observe(
        store,
        id,
        records
            .iter()
            .map(|record| (identity.clone(), record.entry.atom_uuid.clone(), true))
            .collect(),
    )
    .await?;
    target.processed += rows.len() as u64;
    target.selected += rows.len() as u64;
    *target.observed.entry(name).or_default() += rows.len() as u64;
    target.after = rows
        .last()
        .map(|(key, _)| String::from_utf8_lossy(key).into_owned())
        .or_else(|| target.after.clone());
    // No tip is rewritten: its blinded coordinate, atom, author and metadata stay exact.
    Ok(rows.is_empty())
}
