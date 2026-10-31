//! Persistence and metadata-merge helpers for schema expansion.

use std::collections::hash_map::Entry;

use crate::lock_helpers::{read_lock, write_lock};
use schema_types::Schema;
use schema_types::{FoldDbError, FoldDbResult};

use super::state::SchemaServiceState;

impl SchemaServiceState {
    /// Commit the predecessor marker and successor in one definition image.
    /// Publish both schema caches and the name index only after that commit.
    pub(super) async fn persist_expansion(
        &self,
        schema: &Schema,
        old_name: &str,
        desc_name: &str,
    ) -> FoldDbResult<()> {
        let expanded_name = schema.name.clone();
        let mut predecessor = {
            let schemas = read_lock(&self.schemas, "schemas")?;
            schemas.get(old_name).cloned().ok_or_else(|| {
                FoldDbError::Config(format!(
                    "schema_expansion_write_conflict: predecessor '{old_name}' is absent"
                ))
            })?
        };
        predecessor.superseded_by = Some(expanded_name.clone());
        self.persist_schemas(&[predecessor.clone(), schema.clone()])
            .await?;

        // Match the schemas-then-index lock order used by index rebuilds.
        let key = super::state::descriptive_name_key(schema.owner_app_id.as_deref(), desc_name);
        let mut schemas = write_lock(&self.schemas, "schemas")?;
        let mut index = write_lock(&self.descriptive_name_index, "descriptive_name_index")?;
        schemas.insert(old_name.to_owned(), predecessor);
        schemas.insert(expanded_name.clone(), schema.clone());
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
