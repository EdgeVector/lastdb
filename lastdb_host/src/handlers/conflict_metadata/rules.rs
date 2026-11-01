//! Conflict selection and reply rules. No host, admission, clocks, or store access.

use std::collections::BTreeSet;

use fold_db::access::AccessContext;
use fold_db::db_operations::atom_store::RecordFieldConflicts;
use fold_db::db_operations::MoleculeConflictState;
use fold_db::schema::types::field::FieldKind;
use fold_db::schema::types::{KeyValue, Schema};
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
    /// The range of a HashRange record.
    #[serde(default)]
    pub(super) range: Option<String>,
}

impl SelectedConflictKey {
    pub(super) fn validate(&self) -> Result<(), HostError> {
        if self.schema_name.is_empty()
            || self.schema_name.len() > 512
            || self.hash.is_empty()
            || self.hash.len() > 4096
            || self.range.as_ref().is_some_and(|range| range.len() > 4096)
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

/// The primary key of a format 7 selection. Each field is declared, and the
/// range is present exactly for a HashRange schema.
pub(super) fn record_key(
    schema: &Schema,
    key: &SelectedConflictKey,
) -> Result<KeyValue, HostError> {
    for name in &key.fields {
        let field = schema
            .runtime_fields
            .get(name)
            .ok_or_else(|| HostError::new(400, "conflict_field_not_found"))?;
        let ranged = field.kind == FieldKind::HashRange;
        if ranged != key.range.is_some() {
            return Err(HostError::new(400, "conflict_key_range_mismatch"));
        }
    }
    Ok(KeyValue::new(Some(key.hash.clone()), key.range.clone()))
}

/// One selected record: each field with its kept losing values.
pub(super) fn record_result(fields: Vec<RecordFieldConflicts>) -> Value {
    use MoleculeConflictState::{Clean, Conflict};
    let mut combined = Clean;
    let fields: Vec<_> = fields
        .into_iter()
        .map(|field| {
            let state = if field.conflicts.is_empty() {
                Clean
            } else {
                Conflict
            };
            if state == Conflict {
                combined = Conflict;
            }
            serde_json::json!({
                "field": field.field,
                "state": state,
                "conflicts": field.conflicts,
            })
        })
        .collect();
    serde_json::json!({ "state": combined, "fields": fields })
}

/// A selected record whose read did not complete: unknown, not clean.
pub(super) fn unknown_record_result(key: &SelectedConflictKey) -> Value {
    let fields: Vec<_> = key
        .fields
        .iter()
        .map(|field| {
            serde_json::json!({
                "field": field,
                "state": MoleculeConflictState::Unknown,
                "conflicts": [],
            })
        })
        .collect();
    serde_json::json!({ "state": MoleculeConflictState::Unknown, "fields": fields })
}
