use super::*;
// lint:file-size-ok moved verbatim from add_schema.rs

impl SchemaServiceState {
    /// Add a canonical field to the registry directly, without running
    /// the LLM-backed classification pipeline.
    ///
    /// Used by `builtin_canonical_fields::seed` to pre-populate the
    /// registry at service startup with a curated list of common
    /// concepts (user_email, photo_caption, gps_latitude, …). Each
    /// entry carries its own `description`, `field_type`,
    /// `classification`, and `interest_category`, so the service can
    /// skip LLM calls for the hot cases.
    ///
    /// Idempotent: if the field already exists in the registry, this
    /// is a no-op and returns `Ok(())`. Otherwise the entry is inserted
    /// into the in-memory registry, an embedding is computed for
    /// semantic field matching, and the entry is persisted via the
    /// active storage backend.
    pub async fn add_canonical_field(
        &self,
        name: &str,
        canonical: crate::types::CanonicalField,
    ) -> FoldDbResult<()> {
        // Early return if already present — lock-scoped so the check
        // doesn't hold the write lock across the later embed + persist.
        {
            let fields = read_lock(&self.canonical_fields, "canonical_fields")?;
            if fields.contains_key(name) {
                return Ok(());
            }
        }

        // Compute the embedding outside any lock. Best-effort: in
        // environments where the embedding model isn't available
        // (e.g. fastembed in Lambda), we still insert the field; only
        // similarity matching is degraded, not exact lookups.
        let embed_text = Self::build_embedding_text(&canonical.description);
        let embedding = self.embedder.embed_text(&embed_text).ok();

        // Insert under the write locks. Re-check in case another task
        // raced us into the registry.
        {
            let mut fields = write_lock(&self.canonical_fields, "canonical_fields")?;
            if fields.contains_key(name) {
                return Ok(());
            }
            let mut embeddings = write_lock(
                &self.canonical_field_embeddings,
                "canonical_field_embeddings",
            )?;
            if let Some(ref vec) = embedding {
                embeddings.insert(name.to_string(), vec.clone());
            }
            fields.insert(name.to_string(), canonical.clone());
        }

        // Persist the field metadata first. If this fails, the
        // embedding never makes it to S3 either — safer than the
        // reverse ordering.
        self.persist_canonical_field(name, &canonical).await?;

        // Persist the embedding to the sibling blob. Best-effort: a
        // failure here leaves the in-memory embeddings ahead of S3
        // (fine — the blob is a cache), and the next
        // `warm-embeddings` run will re-populate. External-only; Sled
        // backends don't have an embeddings blob, but calling the
        // trait method is still a no-op-if-not-wired pattern.
        if let Some(vec) = embedding {
            let backend = self.storage.backend();
            if let Err(e) = backend.save_canonical_field_embedding(name, &vec).await {
                tracing::warn!(
                target: "schema_service::schema",
                        "Failed to persist canonical field embedding for '{}': {} — \
                         in-memory cache still populated, next warm-embeddings will recover",
                        name,
                        e
                    );
            }
        }

        Ok(())
    }
}
