//! Validate vocabulary provenance before a resolver artifact enters a cache.
use crate::field_name_policy::{validate_new_field_name, NAME_POLICY_VERSION};
use crate::resolver_pack::SchemaSnapshotArtifact;
use std::collections::HashMap;

pub(crate) fn validate(snapshot: &SchemaSnapshotArtifact) -> Result<(), String> {
    if snapshot.name_policy_version != NAME_POLICY_VERSION {
        return Err(
            "resolver snapshot lacks approval for the active canonical-name policy".to_owned(),
        );
    }
    let mut vocabulary = HashMap::new();
    for field in &snapshot.canonical_fields {
        let name = field
            .canonical_field_id
            .strip_prefix("canonical:")
            .ok_or_else(|| "invalid canonical field id in resolver snapshot".to_owned())?;
        validate_new_field_name(name)?;
        if field.description.trim().is_empty()
            || vocabulary.insert(name, &field.field_type).is_some()
        {
            return Err(format!(
                "invalid or duplicate resolver canonical field '{name}'"
            ));
        }
    }
    for schema in &snapshot.schemas {
        for field in &schema.fields {
            validate_new_field_name(&field.field_name)?;
            if vocabulary.get(field.field_name.as_str()).copied() != Some(&field.field_type) {
                return Err(format!(
                    "unapproved_canonical_field: '{}' in resolver schema",
                    field.field_name
                ));
            }
        }
    }
    Ok(())
}
