//! Conflict selection and reply rules. No host, admission, clocks, or store access.

use std::collections::{BTreeSet, HashMap};

use fold_db::access::AccessContext;
use fold_db::db_operations::MoleculeConflictState;
use fold_db::schema::types::field::FieldKind;
use fold_db::schema::types::Schema;
use serde::Deserialize;
use serde_json::Value;

use crate::HostError;

const MAX_KEYS: usize = 64;
const MAX_FIELDS_PER_KEY: usize = 32;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
/// Body naming the personal conflict keys to read.
pub struct SelectedConflictRequest {
    pub(super) keys: Vec<SelectedConflictKey>,
}

impl SelectedConflictRequest {
    pub(super) fn validate_access_and_count(&self, ctx: &AccessContext) -> Result<(), HostError> {
        if !ctx.is_owner {
            return Err(HostError::new(403, "owner_required"));
        }
        // This route has only a personal HashKey contract. Never fall through
        // from a named org/DB scope to the unprefixed conflict namespace.
        if ctx.storage_prefix.is_some()
            || ctx
                .db_locator
                .as_deref()
                .is_some_and(|db| db != "lastdb://personal")
        {
            return Err(HostError::new(400, "personal_conflict_scope_required"));
        }
        if self.keys.is_empty() || self.keys.len() > MAX_KEYS {
            return Err(HostError::new(400, "invalid_conflict_key_count"));
        }
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SelectedConflictKey {
    pub(super) schema_name: String,
    pub(super) fields: Vec<String>,
    hash: String,
}

impl SelectedConflictKey {
    pub(super) fn validate(&self) -> Result<(), HostError> {
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

pub(super) fn field_molecules(
    schema: &Schema,
    fields: &[String],
) -> Result<Vec<Option<String>>, HostError> {
    fields
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

pub(super) fn field_result(
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
