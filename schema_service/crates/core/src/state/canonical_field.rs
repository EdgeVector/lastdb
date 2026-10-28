use super::*;

impl SchemaServiceState {
    /// Use the same code policy for every source of canonical definitions.
    pub async fn add_canonical_field(
        &self,
        name: &str,
        canonical: crate::types::CanonicalField,
    ) -> FoldDbResult<()> {
        self.add_canonical_fields(&[(name.to_owned(), canonical)])
            .await
    }

    /// Validate the complete batch before the first persistence or cache write.
    /// Persist first: a failed backend write must not grant in-memory visibility.
    pub async fn add_canonical_fields(
        &self,
        fields: &[(String, crate::types::CanonicalField)],
    ) -> FoldDbResult<()> {
        let mut seen = HashSet::new();
        for (name, _) in fields {
            if !seen.insert(name) {
                return Err(FoldDbError::Config(format!(
                    "duplicate canonical field '{name}'"
                )));
            }
        }
        self.commit_vocabulary(
            fields,
            crate::vocabulary_commit::VocabularyCommit::Schemas(&[]),
        )
        .await
    }

    pub(crate) fn cache_committed_vocabulary(
        &self,
        fields: &[(String, crate::types::CanonicalField)],
    ) -> FoldDbResult<()> {
        let mut cache = write_lock(&self.canonical_fields, "canonical_fields")?;
        for (name, field) in fields {
            cache.insert(name.clone(), field.clone());
        }
        drop(cache);
        // Model vectors remain a derived cache, never an authority credential.
        self.bump_state_version();
        Ok(())
    }

    /// Read public definitions, never user record values or declaration permissions.
    pub async fn canonical_vocabulary(&self) -> FoldDbResult<serde_json::Value> {
        let fields = self.storage.backend().load_all_canonical_fields().await?;
        self.install_canonical_vocabulary(&fields)?;
        Ok(
            serde_json::json!({"name_policy_version": crate::field_name_policy::NAME_POLICY_VERSION, "fields": fields}),
        )
    }
}
