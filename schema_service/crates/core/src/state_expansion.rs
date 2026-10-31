use std::collections::{HashMap, HashSet};

use crate::lock_helpers::read_lock;
use schema_types::Schema;
use schema_types::{FoldDbError, FoldDbResult};

use super::state::SchemaServiceState;
use super::state_expansion_persist::merge_existing_metadata;
use super::types::SchemaAddOutcome;

mod renames;

/// Normalized key layout for expand / multi-key decisions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyLayoutFingerprint {
    pub hash_field: Option<String>,
    pub range_field: Option<String>,
}

/// Extract partition key layout from a schema (empty strings → None).
#[must_use]
pub fn key_layout_fingerprint(schema: &Schema) -> KeyLayoutFingerprint {
    let norm = |s: Option<&str>| {
        s.map(str::trim)
            .filter(|x| !x.is_empty())
            .map(str::to_string)
    };
    match schema.key.as_ref() {
        Some(k) => KeyLayoutFingerprint {
            hash_field: norm(k.hash_field.as_deref()),
            range_field: norm(k.range_field.as_deref()),
        },
        None => KeyLayoutFingerprint {
            hash_field: None,
            range_field: None,
        },
    }
}

/// True when incoming and existing use **different lookup keys**.
///
/// A different key layout must not supersede an existing definition or
/// rewrite its key. Canonical field reuse does not share record values.
#[must_use]
pub fn is_cross_key_layout(incoming: &Schema, existing: &Schema) -> bool {
    key_layout_fingerprint(incoming) != key_layout_fingerprint(existing)
}

/// Alias used at expand call sites.
#[must_use]
pub(super) fn is_cross_key_layout_expansion(incoming: &Schema, existing: &Schema) -> bool {
    is_cross_key_layout(incoming, existing)
}

impl SchemaServiceState {
    /// If a schema has been superseded by an expanded version, resolve to the
    /// active schema. Returns `None` if no redirection is needed.
    pub(super) fn resolve_active_schema(
        &self,
        existing_schema: &Schema,
        schema_name: &str,
        schemas: &HashMap<String, Schema>,
    ) -> Option<(Schema, String)> {
        let desc_name = existing_schema.descriptive_name.as_ref()?;
        // Index keys are namespaced by `owner_app_id` (app_identity v3.1,
        // Lane B2b), so an active-schema resolution for a same-named seed
        // and `fbrain/<X>` looks up the right entry.
        let key =
            super::state::descriptive_name_key(existing_schema.owner_app_id.as_deref(), desc_name);
        let index = match read_lock(&self.descriptive_name_index, "descriptive_name_index") {
            Ok(idx) => idx,
            Err(e) => {
                tracing::warn!(
                    target: "schema_service::schema",
                    "{} — falling back to original schema",
                    e
                );
                return None;
            }
        };
        let current_hash = index.get(&key)?;
        if *current_hash == schema_name {
            return None;
        }
        let active_schema = schemas.get(current_hash)?;
        // Namespace re-verification: `descriptive_name_key` is not injective
        // on `(Option<&str>, &str)` — `(None, "kanban/Tasks")` and
        // `(Some("kanban"), "Tasks")` both encode to `"kanban/Tasks"`. When a
        // same-keyed sibling-namespace schema overwrote the index slot, the
        // raw `index.get` answer points at an entry from a disjoint
        // namespace. Supersession is per-namespace by definition (it's the
        // tail of an `expand_schema` chain within one namespace), so a
        // cross-namespace "active" entry is never a real redirection. Drop
        // it and report no redirection. Same shape as PR #466 / #487 at a
        // sibling site.
        fn normalize(s: Option<&str>) -> Option<&str> {
            s.filter(|x| !x.is_empty())
        }
        if normalize(active_schema.owner_app_id.as_deref())
            != normalize(existing_schema.owner_app_id.as_deref())
        {
            return None;
        }
        tracing::info!(
            target: "schema_service::schema",
            "Schema '{}' was superseded by '{}' — checking active schema",
            schema_name,
            current_hash
        );
        Some((active_schema.clone(), current_hash.clone()))
    }

    /// Expand an incoming schema to be a superset of an existing schema.
    ///
    /// Merges fields, merges classifications and ref_fields, recomputes
    /// identity_hash, persists, and updates caches.
    ///
    /// Returns `SchemaAddOutcome::Expanded` on success, or `AlreadyExists` if
    /// the incoming fields are a subset of the existing.
    pub(super) async fn expand_schema(
        &self,
        schema: &mut Schema,
        existing: &Schema,
        old_name: &str,
        desc_name: &str,
        mutation_mappers: &HashMap<String, String>,
    ) -> FoldDbResult<SchemaAddOutcome> {
        // Never classic-expand across different key layouts (would supersede
        // and can rewrite hash_field on the live pin — BoardCards incident).
        if is_cross_key_layout(schema, existing) {
            return Err(FoldDbError::Config(format!(
                "refusing expand_schema across different key layouts \
                 (incoming {:?}/{:?} vs existing {:?}/{:?}); use named alternate keys \
                 on one schema, or register separate schema-owned record types",
                key_layout_fingerprint(schema).hash_field,
                key_layout_fingerprint(schema).range_field,
                key_layout_fingerprint(existing).hash_field,
                key_layout_fingerprint(existing).range_field,
            )));
        }

        let existing_fields = existing.fields.clone().unwrap_or_default();
        let Some(merged_fields) = merge_expansion_fields(schema, &existing_fields) else {
            // The new schema's fields are a subset of the existing: reuse it.
            tracing::info!(
                target: "schema_service::schema",
                "New schema is a subset of existing '{}' (descriptive_name='{}') — reusing existing",
                old_name,
                desc_name
            );
            return Ok(SchemaAddOutcome::AlreadyExists(
                existing.clone(),
                mutation_mappers.clone(),
            ));
        };

        tracing::info!(
            target: "schema_service::schema",
            "Expanding schema (descriptive_name='{}') — merging fields from old hash '{}'",
            desc_name,
            old_name
        );
        schema.fields = Some(merged_fields);

        // A definition carries no molecule address. Each node keeps the
        // molecules of the schema that it installed.
        schema.field_molecule_uuids = None;
        merge_existing_metadata(schema, existing);
        self.validate_proposal_field_names(schema)?;
        self.apply_canonical_types(schema)?;

        // Recompute identity hash with merged fields.
        // The expanded schema is a NEW schema — its name is the identity hash
        // (derived from schema name + fields). The old schema keeps its name and
        // gets blocked/superseded.
        schema.compute_identity_hash();
        let new_hash = schema
            .get_identity_hash()
            .ok_or_else(|| {
                FoldDbError::Config("Failed to compute merged identity_hash".to_string())
            })?
            .clone();
        schema.name = new_hash;

        self.persist_expansion(schema, old_name, desc_name).await?;

        // Propagate classifications and interest categories for the new fields.
        self.apply_canonical_classifications(schema);
        self.apply_canonical_interest_categories(schema);

        tracing::info!(
            target: "schema_service::schema",
            "Schema expanded: old='{}' (blocked) -> new='{}' (descriptive_name='{}')",
            old_name,
            schema.name,
            desc_name
        );

        Ok(SchemaAddOutcome::Expanded(
            old_name.to_string(),
            schema.clone(),
            mutation_mappers.clone(),
        ))
    }
}

/// Merge the incoming schema's fields onto the existing fields.
///
/// Returns `None` when the incoming fields are a subset of the existing.
fn merge_expansion_fields(schema: &Schema, existing_fields: &[String]) -> Option<Vec<String>> {
    let existing_set: HashSet<&String> = existing_fields.iter().collect();
    let new_field_set: HashSet<String> = schema
        .fields
        .as_ref()
        .map(|nf| nf.iter().cloned().collect())
        .unwrap_or_default();
    if new_field_set.iter().all(|f| existing_set.contains(f)) {
        return None;
    }
    let mut merged_fields = existing_fields.to_vec();
    merged_fields.extend(
        new_field_set
            .into_iter()
            .filter(|f| !existing_set.contains(f)),
    );
    Some(merged_fields)
}
