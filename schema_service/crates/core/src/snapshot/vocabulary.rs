//! Import is a restore path, never another vocabulary-approval path.
use super::*;

impl SchemaServiceState {
    pub(super) fn validate_snapshot_vocabulary(
        &self,
        envelope: &SnapshotEnvelope,
    ) -> FoldDbResult<()> {
        let approved = read_lock(&self.canonical_fields, "canonical_fields")?;
        for (name, field) in &envelope.canonical_fields {
            crate::field_name_policy::validate_approved_field(name, field)?;
            if approved.get(name) != Some(field) {
                return Err(FoldDbError::Config(format!(
                    "unapproved_canonical_field: '{name}'; publish its definition with operator authority before import"
                )));
            }
        }
        for schema in &envelope.schemas {
            crate::field_name_policy::validate_schema_vocabulary(
                schema,
                &envelope.canonical_fields,
            )?;
        }
        let declarations = read_lock(&self.declared_fields, "declared_fields")?;
        for declaration in declarations.all() {
            for field in &declaration.fields {
                crate::field_name_policy::validate_declared_concept(
                    &envelope.canonical_fields,
                    &field.name,
                    &field.field_type,
                    &field.description,
                    field.version,
                )?;
            }
        }
        let releases = read_lock(&self.app_releases, "app_releases")?;
        for release in releases.values() {
            for identity in release.manifest.schemas.values() {
                if !envelope
                    .schemas
                    .iter()
                    .any(|schema| &schema.name == identity)
                {
                    return Err(FoldDbError::Config(format!(
                        "import removes schema '{identity}' referenced by a release"
                    )));
                }
            }
        }
        if envelope
            .embeddings
            .canonical_fields
            .keys()
            .any(|name| !envelope.canonical_fields.contains_key(name))
        {
            return Err(FoldDbError::Config(
                "invalid canonical embedding reference in snapshot".to_owned(),
            ));
        }
        Ok(())
    }
}
