//! Fail closed before a loaded registry or imported snapshot becomes visible.
use super::*;

impl SchemaServiceState {
    pub(super) fn validate_loaded_vocabulary(&self) -> FoldDbResult<()> {
        let fields = read_lock(&self.canonical_fields, "canonical_fields")?;
        for (name, field) in fields.iter() {
            crate::field_name_policy::validate_canonical_field(name, field)?;
        }
        let schemas = read_lock(&self.schemas, "schemas")?;
        for schema in schemas.values() {
            crate::field_name_policy::validate_schema_vocabulary(schema, &fields)?;
        }
        Ok(())
    }
    pub(super) async fn validate_loaded_schemas(
        &self,
        schemas: &HashMap<String, Schema>,
    ) -> FoldDbResult<()> {
        let mut names = Vec::new();
        for schema in schemas.values() {
            names.extend(
                crate::field_name_policy::schema_field_names(schema)
                    .into_iter()
                    .map(String::as_str),
            );
            names.extend(crate::field_name_policy::schema_referenced_field_names(
                schema,
            ));
        }
        self.refresh_vocabulary_for_names(&names).await?;
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
                    &field.description,
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
    /// Refresh only for a new concept. Existing definitions are immutable;
    /// new declarations must work across warm Lambda instances without a deploy.
    pub async fn validate_schema_field_vocabulary(&self, schema: &Schema) -> FoldDbResult<()> {
        let mut names: Vec<_> = crate::field_name_policy::schema_field_names(schema)
            .into_iter()
            .map(String::as_str)
            .collect();
        names.extend(crate::field_name_policy::schema_referenced_field_names(
            schema,
        ));
        self.refresh_vocabulary_for_names(&names).await?;
        self.normalized_schema_field_vocabulary(schema).map(|_| ())
    }

    pub fn normalized_schema_field_vocabulary(&self, schema: &Schema) -> FoldDbResult<Schema> {
        let mut proposal = schema.clone();
        let candidates = self.schema_vocabulary_candidates(schema)?;
        let checked = self.provisional_vocabulary_batch(&candidates)?;
        let mut vocabulary = read_lock(&self.canonical_fields, "canonical_fields")?.clone();
        vocabulary.extend(checked);
        crate::field_name_policy::fill_schema_field_defaults(&mut proposal, &vocabulary);
        crate::field_name_policy::validate_schema_vocabulary(&proposal, &vocabulary)?;
        Ok(proposal)
    }

    pub(crate) async fn refresh_vocabulary_for_names(&self, names: &[&str]) -> FoldDbResult<()> {
        for name in names {
            crate::field_name_policy::validate_new_field_name(name).map_err(FoldDbError::Config)?;
        }
        {
            let fields = read_lock(&self.canonical_fields, "canonical_fields")?;
            if names.iter().all(|name| fields.contains_key(*name)) {
                return Ok(());
            }
        }
        let loaded = self.storage.backend().load_all_canonical_fields().await?;
        self.install_canonical_vocabulary(&loaded)
    }

    pub(crate) fn install_canonical_vocabulary(
        &self,
        loaded: &HashMap<String, crate::types::CanonicalField>,
    ) -> FoldDbResult<()> {
        for (name, field) in loaded {
            crate::field_name_policy::validate_canonical_field(name, field)?;
        }
        let mut fields = write_lock(&self.canonical_fields, "canonical_fields")?;
        for (name, field) in loaded {
            crate::field_name_policy::validate_canonical_insert(&fields, name, field)?;
        }
        for (name, field) in loaded {
            if !fields.contains_key(name) {
                fields.insert(name.clone(), field.clone());
            }
        }
        Ok(())
    }
}
