//! Persistence and metadata-merge helpers for schema expansion.

use std::collections::{hash_map::Entry, HashMap};

use crate::lock_helpers::write_lock;
use schema_types::FoldDbResult;
use schema_types::Schema;

use super::state::SchemaServiceState;

impl SchemaServiceState {
    /// Preserve every predecessor field through its canonical molecule mapper.
    pub(super) fn stamp_predecessor_mappers(
        schema: &mut Schema,
        existing_fields: &[String],
        old_name: &str,
    ) {
        use schema_types::FieldMapper;
        let mut mappers: HashMap<String, FieldMapper> =
            schema.field_mappers().cloned().unwrap_or_default();
        for field in existing_fields {
            mappers
                .entry(field.clone())
                .or_insert_with(|| FieldMapper::new(old_name.to_string(), field.clone()));
        }
        schema.field_mappers = Some(mappers);
        schema.field_molecule_uuids = None;
    }

    /// Persist the expanded schema, mark the predecessor as superseded in
    /// memory and on disk, and point the owner-namespaced descriptive_name
    /// index at the expanded schema.
    pub(super) async fn persist_expansion(
        &self,
        schema: &Schema,
        old_name: &str,
        desc_name: &str,
    ) -> FoldDbResult<()> {
        let expanded_name = schema.name.clone();
        self.persist_schema(schema).await?;

        // Mark the old schema as superseded in memory and grab a clone for persistence
        let old_clone = {
            let mut schemas = write_lock(&self.schemas, "schemas")?;
            let clone = if let Some(old_schema) = schemas.get_mut(old_name) {
                old_schema.superseded_by = Some(expanded_name.clone());
                Some(old_schema.clone())
            } else {
                None
            };
            schemas.insert(expanded_name.clone(), schema.clone());
            clone
        };

        // Persist the superseded marker (outside the lock)
        if let Some(old_schema) = old_clone {
            self.persist_schema(&old_schema).await?;
        }

        // Update descriptive_name index to point to expanded schema. The
        // key is namespaced by `owner_app_id` (app_identity v3.1, Lane B2b)
        // so the expansion target replaces only the matching same-owner
        // entry, leaving any same-named seed/legacy entry intact.
        let key = super::state::descriptive_name_key(schema.owner_app_id.as_deref(), desc_name);
        let mut index = write_lock(&self.descriptive_name_index, "descriptive_name_index")?;
        index.insert(key, expanded_name);
        Ok(())
    }
}

/// Merge classifications, ref_fields, descriptions and types from the
/// predecessor into the expanded schema (keep incoming entries, add
/// existing-only ones).
/// Inherited declared hashes retain their declaration provenance.
///
/// Without the description/type merge the existing-only fields (which get
/// added to `schema.fields`) carry no entry in these maps after expansion
/// and the same class of silent breakage that #439 / #457 fixed for
/// `apply_field_renames` shows up here too:
///
///  - `field_descriptions` missing breaks the
///    `add_schema`-time invariant that every entry in `fields` has a
///    description, and downstream `build_field_description` falls
///    back to a low-signal stub that lowers semantic-match quality.
///  - `field_types` missing makes `Schema::get_field_type` fall back
///    to `Any` whenever the canonical registry doesn't already
///    carry the field (e.g. legacy entries with `field_type == Any`),
///    losing the type the existing schema's caller had declared.
pub(super) fn merge_existing_metadata(schema: &mut Schema, existing: &Schema) {
    for (field, classifications) in &existing.field_classifications {
        schema
            .field_classifications
            .entry(field.clone())
            .or_insert_with(|| classifications.clone());
    }
    for (field, classification) in &existing.field_data_classifications {
        schema
            .field_data_classifications
            .entry(field.clone())
            .or_insert_with(|| classification.clone());
    }
    for (field, category) in &existing.field_interest_categories {
        schema
            .field_interest_categories
            .entry(field.clone())
            .or_insert_with(|| category.clone());
    }
    for (field, target) in &existing.ref_fields {
        schema
            .ref_fields
            .entry(field.clone())
            .or_insert_with(|| target.clone());
    }
    for (field, description) in &existing.field_descriptions {
        schema
            .field_descriptions
            .entry(field.clone())
            .or_insert_with(|| description.clone());
    }
    for (field, ty) in &existing.field_types {
        schema
            .field_types
            .entry(field.clone())
            .or_insert_with(|| ty.clone());
    }
    for (field, version) in &existing.field_versions {
        schema
            .field_versions
            .entry(field.clone())
            .or_insert(*version);
    }
    for (field, identity) in &existing.field_hashes {
        if let Entry::Vacant(entry) = schema.field_hashes.entry(field.clone()) {
            entry.insert(identity.clone());
            // Provenance travels with an inherited identity. An incoming
            // verified hash already carries its own canonical/declared claim.
            if let Some(declaration) = existing.field_declarations.get(field) {
                schema
                    .field_declarations
                    .entry(field.clone())
                    .or_insert_with(|| declaration.clone());
            }
        }
    }
}
