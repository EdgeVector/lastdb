//! Caller-supplied hashes are claims, not an alternate field identity authority.

use std::collections::HashMap;

use crate::declared_fields::{DeclaredFieldRecord, DeclaredFieldRegistry};
use crate::lock_helpers::{read_lock, write_lock};
use crate::state::SchemaServiceState;
use crate::types::CanonicalField;
use schema_types::{
    compute_declared_field_identity, FoldDbError, FoldDbResult, Schema,
    DECLARED_FIELD_IDENTITY_ALGO_VERSION,
};

pub(crate) fn validate_canonical_schema_hashes(
    schema: &Schema,
    vocabulary: &HashMap<String, CanonicalField>,
) -> FoldDbResult<()> {
    for (name, supplied) in &schema.field_hashes {
        if schema.field_declarations.contains_key(name) {
            continue;
        }
        let canonical = vocabulary.get(name).ok_or_else(|| invalid_hash(name))?;
        if supplied != &canonical.field_hash(name) {
            return Err(invalid_hash(name));
        }
    }
    for name in schema.field_declarations.keys() {
        if !schema.field_hashes.contains_key(name) {
            return Err(invalid_hash(name));
        }
    }
    Ok(())
}

pub(crate) fn validate_declaration_identities(record: &DeclaredFieldRecord) -> FoldDbResult<()> {
    if record.algo_version != DECLARED_FIELD_IDENTITY_ALGO_VERSION {
        return Err(FoldDbError::Config(
            "invalid_field_hash: unsupported declared field identity algorithm".into(),
        ));
    }
    for field in &record.fields {
        let expected = compute_declared_field_identity(
            &record.declaration_id,
            &field.name,
            &field.field_type,
            field.version,
        );
        if field.identity != expected {
            return Err(invalid_hash(&field.name));
        }
    }
    Ok(())
}

fn validate_declared_schema_hashes(
    schema: &Schema,
    declarations: &DeclaredFieldRegistry,
    require_permission: bool,
) -> FoldDbResult<()> {
    for (name, declaration_id) in &schema.field_declarations {
        let record = declarations
            .get(declaration_id)
            .ok_or_else(|| invalid_hash(name))?;
        validate_declaration_identities(record)?;
        if require_permission
            && !schema
                .owner_app_id
                .as_deref()
                .is_some_and(|app| record.permits(app))
        {
            return Err(FoldDbError::Config(format!(
                "invalid_field_hash: app cannot reference declaration '{declaration_id}'"
            )));
        }
        let supplied = schema
            .field_hashes
            .get(name)
            .ok_or_else(|| invalid_hash(name))?;
        // A local name may alias a declared name. The stored identity determines
        // membership; recomputing from the local name would break that contract.
        let field = record
            .fields
            .iter()
            .find(|field| &field.identity == supplied)
            .ok_or_else(|| invalid_hash(name))?;
        if schema.field_types.get(name) != Some(&field.field_type)
            || schema.field_versions.get(name).copied().unwrap_or(1) != field.version
            || schema
                .field_descriptions
                .get(name)
                .is_none_or(|description| description.trim() != field.description.trim())
        {
            return Err(invalid_hash(name));
        }
    }
    Ok(())
}

fn invalid_hash(name: &str) -> FoldDbError {
    FoldDbError::Config(format!(
        "invalid_field_hash: '{name}' must use its canonical or verified declared field identity"
    ))
}

impl SchemaServiceState {
    pub(crate) fn validate_schema_declared_identities(
        &self,
        schema: &Schema,
        require_permission: bool,
    ) -> FoldDbResult<()> {
        let declarations = read_lock(&self.declared_fields, "declared_fields")?;
        validate_declared_schema_hashes(schema, &declarations, require_permission)
    }

    pub(crate) async fn refresh_schema_declarations(
        &self,
        schemas: &[&Schema],
    ) -> FoldDbResult<()> {
        if schemas
            .iter()
            .all(|schema| schema.field_declarations.is_empty())
        {
            return Ok(());
        }
        // Serialize with local declaration writes so an older backend read
        // cannot replace a declaration this instance just committed.
        let _write = self.declared_field_writes.lock().await;
        let records = self.storage.backend().load_all_declared_fields().await?;
        for record in &records {
            validate_declaration_identities(record)?;
        }
        write_lock(&self.declared_fields, "declared_fields")?.load(records);
        Ok(())
    }
}
