//! Owner-only metadata for a batch of exact HashKey field selections.

use std::collections::{BTreeSet, HashMap};

use fold_db::access::AccessContext;
use fold_db::db_operations::SELECTED_CONFLICT_MAX_MOLECULES;
use fold_db::schema::SchemaState;
use serde_json::Value;

use crate::host_node::HostNode;
use crate::qos::Lane;
use crate::HostError;

use super::resolve_schema_name;

mod rules;
use rules::SelectedConflictKey;
pub use rules::SelectedConflictRequest;

/// No atom body is read, and no field value or conflict payload is returned.
/// The result is conservative: any
/// unresolved conflict anywhere in a selected field molecule prevents clean.
/// This avoids parsing the lossy legacy HashRange conflict display key.
pub async fn selected_key_conflict_metadata<H: HostNode>(
    host: &H,
    request: SelectedConflictRequest,
    ctx: &AccessContext,
) -> Result<Value, HostError> {
    request.validate_access_and_count(ctx)?;
    let _permit = host.acquire_op_permit(Lane::Bulk).await?;
    if host.fold_db().record_home() {
        return selected_record_conflicts(host, &request).await;
    }
    let mut selected = Vec::with_capacity(request.keys.len());
    let mut molecules = BTreeSet::new();
    for key in &request.keys {
        key.validate()?;
        let fields = selected_field_molecules(host, key)?;
        molecules.extend(fields.iter().flatten().cloned());
        if molecules.len() > SELECTED_CONFLICT_MAX_MOLECULES {
            return Err(HostError::new(400, "conflict_molecule_limit_exceeded"));
        }
        selected.push(fields);
    }
    let molecules: Vec<_> = molecules.into_iter().collect();
    let states = host
        .fold_db()
        .selected_molecule_conflict_metadata(&molecules)
        .await;
    let states: HashMap<_, _> = molecules.into_iter().zip(states).collect();
    let results: Vec<_> = selected
        .into_iter()
        .map(|fields| rules::field_result(fields, &states))
        .collect();
    Ok(serde_json::json!({
        "version": 1,
        "scope": "selected_field_molecules",
        "count": results.len(),
        "results": results,
    }))
}

/// A format 7 home keeps its conflicts with each record: the reply names the
/// kept losing values of each selected field, read by key.
async fn selected_record_conflicts<H: HostNode>(
    host: &H,
    request: &SelectedConflictRequest,
) -> Result<Value, HostError> {
    let mut selections = Vec::with_capacity(request.keys.len());
    for key in &request.keys {
        key.validate()?;
        let schema = selected_schema(host, &key.schema_name)?;
        let primary = rules::record_key(&schema, key)?;
        selections.push((schema.name.clone(), primary, key.fields.clone()));
    }
    let found = host.fold_db().selected_record_conflicts(&selections).await;
    let results: Vec<_> = match found {
        Some(found) => found.into_iter().map(rules::record_result).collect(),
        None => request
            .keys
            .iter()
            .map(rules::unknown_record_result)
            .collect(),
    };
    Ok(serde_json::json!({
        "version": 1,
        "scope": "selected_record_fields",
        "count": results.len(),
        "results": results,
    }))
}

fn selected_field_molecules<H: HostNode>(
    host: &H,
    key: &SelectedConflictKey,
) -> Result<Vec<Option<String>>, HostError> {
    if key.range.is_some() {
        return Err(HostError::new(400, "hash_key_conflict_selection_required"));
    }
    let schema = selected_schema(host, &key.schema_name)?;
    rules::field_molecules(&schema, &key.fields)
}

fn selected_schema<H: HostNode>(
    host: &H,
    schema_name: &str,
) -> Result<fold_db::schema::types::Schema, HostError> {
    let name = resolve_schema_name(host, schema_name)
        .map_err(|_| HostError::new(409, "conflict_schema_resolution_failed"))?
        .ok_or_else(|| HostError::new(404, "conflict_schema_not_found"))?;
    let manager = host.fold_db().schema_manager();
    // Cache-only metadata lookup avoids schema refresh/self-heal writes on a
    // diagnostic request. A blocked predecessor is not silently redirected.
    let schema = manager
        .get_schema_metadata(&name)
        .map_err(|_| HostError::new(500, "conflict_schema_metadata_unavailable"))?
        .ok_or_else(|| HostError::new(404, "conflict_schema_not_found"))?;
    if manager
        .get_schema_state_cached(&name)
        .map_err(|_| HostError::new(500, "conflict_schema_state_unavailable"))?
        != SchemaState::Available
    {
        return Err(HostError::new(409, "conflict_schema_not_available"));
    }
    Ok(schema)
}
