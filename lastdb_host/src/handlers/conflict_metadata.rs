//! Owner-only metadata for a batch of exact HashKey field selections.

use super::*;
use fold_db::db_operations::{MoleculeConflictState, SELECTED_CONFLICT_MAX_MOLECULES};
use fold_db::schema::types::field::FieldKind;
use serde::Deserialize;
use std::collections::BTreeSet;

const MAX_KEYS: usize = 64;
const MAX_FIELDS_PER_KEY: usize = 32;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SelectedConflictRequest {
    keys: Vec<SelectedConflictKey>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SelectedConflictKey {
    schema_name: String,
    fields: Vec<String>,
    hash: String,
}

impl SelectedConflictKey {
    fn validate(&self) -> Result<(), HostError> {
        if self.schema_name.is_empty()
            || self.schema_name.len() > 512
            || self.hash.is_empty()
            || self.hash.len() > 4096
            || self.fields.is_empty()
            || self.fields.len() > MAX_FIELDS_PER_KEY
            || self
                .fields
                .iter()
                .any(|field| field.is_empty() || field.len() > 256)
            || self.fields.iter().collect::<BTreeSet<_>>().len() != self.fields.len()
        {
            return Err(HostError::new(400, "invalid_conflict_key_selection"));
        }
        Ok(())
    }
}

/// No atom body is read, and no field value or conflict payload is returned.
/// The result is conservative: any
/// unresolved conflict anywhere in a selected field molecule prevents clean.
/// This avoids parsing the lossy legacy HashRange conflict display key.
pub async fn selected_key_conflict_metadata<H: HostNode>(
    host: &H,
    request: SelectedConflictRequest,
    ctx: &AccessContext,
) -> Result<Value, HostError> {
    if !ctx.is_owner {
        return Err(HostError::new(403, "owner_required"));
    }
    // This first route has only a personal HashKey contract. Never fall through
    // from a named org/DB scope to the unprefixed conflict namespace.
    if ctx.storage_prefix.is_some()
        || ctx
            .db_locator
            .as_deref()
            .is_some_and(|db| db != "lastdb://personal")
    {
        return Err(HostError::new(400, "personal_conflict_scope_required"));
    }
    if request.keys.is_empty() || request.keys.len() > MAX_KEYS {
        return Err(HostError::new(400, "invalid_conflict_key_count"));
    }
    let _permit = host.acquire_op_permit(Lane::Bulk).await?;
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
        .map(|fields| field_result(fields, &states))
        .collect();
    Ok(serde_json::json!({
        "version": 1,
        "scope": "selected_field_molecules",
        "count": results.len(),
        "results": results,
    }))
}

fn selected_field_molecules<H: HostNode>(
    host: &H,
    key: &SelectedConflictKey,
) -> Result<Vec<Option<String>>, HostError> {
    let name = resolve_schema_name(host, &key.schema_name)
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
    key.fields
        .iter()
        .map(|name| {
            let field = schema
                .runtime_fields
                .get(name)
                .ok_or_else(|| HostError::new(400, "conflict_field_not_found"))?;
            if field.kind != FieldKind::Hash {
                return Err(HostError::new(400, "hash_key_conflict_selection_required"));
            }
            Ok(field.common().molecule_uuid().cloned())
        })
        .collect()
}

fn field_result(
    fields: Vec<Option<String>>,
    states: &HashMap<String, MoleculeConflictState>,
) -> Value {
    use MoleculeConflictState::{Clean, Conflict, Unknown};
    let mut combined = Clean;
    let fields: Vec<_> = fields
        .into_iter()
        .map(|molecule| {
            let state = molecule
                .as_ref()
                .and_then(|molecule| states.get(molecule))
                .copied()
                .unwrap_or(Unknown);
            combined = match (combined, state) {
                (Unknown, _) | (_, Unknown) => Unknown,
                (Conflict, _) | (_, Conflict) => Conflict,
                _ => Clean,
            };
            serde_json::json!({ "molecule_uuid": molecule, "state": state })
        })
        .collect();
    serde_json::json!({ "state": combined, "fields": fields })
}
