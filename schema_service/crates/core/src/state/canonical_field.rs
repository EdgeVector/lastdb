use super::*;

impl SchemaServiceState {
    /// Trusted operator/seed entry point. HTTP callers must first verify the
    /// operator signature. Ordinary schema publication cannot call this path.
    pub async fn add_canonical_field(
        &self,
        name: &str,
        canonical: crate::types::CanonicalField,
    ) -> FoldDbResult<()> {
        self.add_canonical_fields(&[(name.to_owned(), canonical)])
            .await
    }

    /// Validate the complete batch before the first persistence or cache write.
    /// Persist first: a failed backend write must not grant in-memory approval.
    pub async fn add_canonical_fields(
        &self,
        fields: &[(String, crate::types::CanonicalField)],
    ) -> FoldDbResult<()> {
        let mut approved = Vec::with_capacity(fields.len());
        let mut seen = HashSet::new();
        for (name, input) in fields {
            if !seen.insert(name) {
                return Err(FoldDbError::Config(format!(
                    "duplicate canonical field '{name}'"
                )));
            }
            let mut field = input.clone();
            field.name_policy_version = crate::field_name_policy::NAME_POLICY_VERSION;
            crate::field_name_policy::validate_approved_field(name, &field)?;
            approved.push((name.clone(), field));
        }
        {
            let existing = read_lock(&self.canonical_fields, "canonical_fields")?;
            for (name, field) in &approved {
                crate::field_name_policy::validate_canonical_insert(&existing, name, field)?;
            }
        }
        self.storage
            .backend()
            .save_canonical_fields(&approved)
            .await?;
        {
            let mut cache = write_lock(&self.canonical_fields, "canonical_fields")?;
            for (name, field) in approved {
                cache.insert(name, field);
            }
        }
        // Embeddings remain a derived cache. Operators warm them explicitly;
        // a cold start never runs an embedding model for the seed vocabulary.
        self.bump_state_version();
        Ok(())
    }

    pub(crate) fn validate_declared_vocabulary(
        &self,
        fields: &[crate::declared_fields::DeclareFieldInput],
    ) -> FoldDbResult<()> {
        let vocabulary = read_lock(&self.canonical_fields, "canonical_fields")?;
        for input in fields {
            crate::field_name_policy::validate_declared_concept(
                &vocabulary,
                &input.name,
                &input.field_type,
                input.version,
            )?;
        }
        Ok(())
    }
}
