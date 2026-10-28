//! Fail closed before a loaded registry or imported snapshot becomes visible.
use super::*;

impl SchemaServiceState {
    pub(super) fn validate_loaded_vocabulary(&self) -> FoldDbResult<()> {
        let fields = read_lock(&self.canonical_fields, "canonical_fields")?;
        for (name, field) in fields.iter() {
            crate::field_name_policy::validate_approved_field(name, field)?;
        }
        let schemas = read_lock(&self.schemas, "schemas")?;
        for schema in schemas.values() {
            crate::field_name_policy::validate_schema_vocabulary(schema, &fields)?;
        }
        Ok(())
    }
    pub(super) fn validate_loaded_schemas(
        &self,
        schemas: &HashMap<String, Schema>,
    ) -> FoldDbResult<()> {
        let fields = read_lock(&self.canonical_fields, "canonical_fields")?;
        for schema in schemas.values() {
            crate::field_name_policy::validate_schema_vocabulary(schema, &fields)?;
        }
        Ok(())
    }

    pub(super) fn validate_loaded_declarations(
        &self,
        records: &[crate::DeclaredFieldRecord],
    ) -> FoldDbResult<()> {
        let fields = read_lock(&self.canonical_fields, "canonical_fields")?;
        for record in records {
            for field in &record.fields {
                crate::field_name_policy::validate_declared_concept(
                    &fields,
                    &field.name,
                    &field.field_type,
                    field.version,
                )?;
            }
        }
        Ok(())
    }

    pub(crate) fn validate_loaded_release_schemas(
        &self,
        releases: &HashMap<String, crate::app_release::ReleaseRecord>,
    ) -> FoldDbResult<()> {
        let schemas = read_lock(&self.schemas, "schemas")?;
        let fields = read_lock(&self.canonical_fields, "canonical_fields")?;
        for release in releases.values() {
            for identity in release.manifest.schemas.values() {
                let schema = schemas.get(identity).ok_or_else(|| {
                    FoldDbError::Config(format!("release references missing schema '{identity}'"))
                })?;
                crate::field_name_policy::validate_schema_vocabulary(schema, &fields)?;
            }
        }
        Ok(())
    }
}
