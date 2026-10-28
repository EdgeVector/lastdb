//! Restores use the same field validation rules as normal declarations.
use super::*;

impl SchemaServiceState {
    pub(super) fn validate_snapshot_vocabulary(
        &self,
        envelope: &SnapshotEnvelope,
    ) -> FoldDbResult<()> {
        let candidates: Vec<_> = envelope
            .canonical_fields
            .iter()
            .map(|(name, field)| (name.clone(), field.clone()))
            .collect();
        self.checked_vocabulary_batch(&candidates)?;
        for (name, field) in &envelope.canonical_fields {
            crate::field_name_policy::validate_canonical_field(name, field)?;
        }
        for schema in &envelope.schemas {
            crate::field_name_policy::validate_schema_vocabulary(
                schema,
                &envelope.canonical_fields,
            )?;
            self.validate_schema_declared_identities(schema, false)?;
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
