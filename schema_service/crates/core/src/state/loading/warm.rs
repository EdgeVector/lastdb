use super::*;

impl SchemaServiceState {
    /// Rebuild the descriptive_name -> schema_name index.
    ///
    /// Does NOT rebuild the embeddings cache. Per-schema fastembed
    /// calls add up quickly at cold-start time: at 200 schemas × ~40ms
    /// per inference plus model-load overhead, this loop alone
    /// contributed ~10-15s to every Lambda cold start (measured
    /// 2026-04-23 against `SchemaServiceStack-dev`). Embeddings are
    /// only consumed by the semantic-similarity matching endpoints
    /// (see `state_matching::find_similar_schema`), which already
    /// handle an empty embedding cache by returning no-match. The
    /// cache gets populated organically as each `add_schema` writes
    /// its own embedding, so steady-state similarity matching still
    /// works for schemas the current Lambda instance registered.
    ///
    /// Callers that need eager-warming (e.g. a similarity-heavy
    /// workload after a fresh deploy) should call
    /// `warm_descriptive_name_embeddings` explicitly — ideally from a
    /// background task so the warm-up doesn't block request serving.
    pub(crate) fn rebuild_descriptive_name_index(&self) {
        let Ok(schemas) = self.schemas.read() else {
            return;
        };
        let Ok(mut index) = self.descriptive_name_index.write() else {
            return;
        };
        index.clear();
        for (name, schema) in schemas.iter() {
            // Skip superseded schemas — only the active expanded version
            // should be in the index.
            if schema.superseded_by.is_some() {
                continue;
            }
            if let Some(ref desc) = schema.descriptive_name {
                let key = descriptive_name_key(schema.owner_app_id.as_deref(), desc);
                index.insert(key, name.clone());
            }
        }
    }

    /// Eagerly embed every canonical field that doesn't already have
    /// a persisted embedding, populate the in-memory cache, and
    /// persist each new embedding to S3 so the next cold start
    /// inherits them.
    ///
    /// Wired up to `POST /v1/admin/warm-embeddings`. Meant for the
    /// one-shot backfill after deploying the persisted-embeddings
    /// feature against an S3 bucket whose blobs predate the change.
    /// Subsequent writes keep the cache in sync automatically.
    ///
    /// Returns `(computed, skipped)` counts.
    pub async fn warm_canonical_field_embeddings(&self) -> (usize, usize) {
        let work: Vec<(String, String)> = {
            let Ok(fields) = self.canonical_fields.read() else {
                return (0, 0);
            };
            let Ok(embeddings) = self.canonical_field_embeddings.read() else {
                return (0, 0);
            };
            fields
                .iter()
                .filter_map(|(name, canonical)| {
                    if embeddings.contains_key(name) {
                        return None;
                    }
                    Some((
                        name.clone(),
                        Self::build_embedding_text(&canonical.description),
                    ))
                })
                .collect()
        };

        let mut computed = 0;
        let mut skipped = 0;
        for (name, embed_text) in work {
            let Ok(vec) = self.embedder.embed_text(&embed_text) else {
                skipped += 1;
                continue;
            };
            if let Ok(mut embeddings) = self.canonical_field_embeddings.write() {
                embeddings.insert(name.clone(), vec.clone());
            }
            let backend = self.storage.backend();
            if let Err(e) = backend.save_canonical_field_embedding(&name, &vec).await {
                tracing::warn!(
                target: "schema_service::schema",
                            "warm_canonical_field_embeddings: failed to persist '{}': {}",
                            name,
                            e
                        );
            }
            computed += 1;
        }
        (computed, skipped)
    }

    /// Eagerly embed every current schema's `descriptive_name` that
    /// doesn't already have a persisted embedding, populate the
    /// in-memory cache, and persist each new embedding to S3.
    ///
    /// Wired up to `POST /v1/admin/warm-embeddings` alongside the
    /// canonical-field warm. Returns `(computed, skipped)` counts.
    pub async fn warm_descriptive_name_embeddings(&self) -> (usize, usize) {
        // (schema_hash, descriptive_name, namespaced_key). The embedder
        // sees the bare descriptive_name (the human-readable text it was
        // trained on); the in-memory cache is keyed by the namespaced
        // form so app-owned schemas and a same-named seed have separate
        // entries.
        let work: Vec<(String, String, String)> = {
            let Ok(schemas) = self.schemas.read() else {
                return (0, 0);
            };
            let Ok(embeddings) = self.descriptive_name_embeddings.read() else {
                return (0, 0);
            };
            schemas
                .iter()
                .filter_map(|(schema_hash, schema)| {
                    if schema.superseded_by.is_some() {
                        return None;
                    }
                    let desc = schema.descriptive_name.as_ref()?;
                    let key = descriptive_name_key(schema.owner_app_id.as_deref(), desc);
                    if embeddings.contains_key(&key) {
                        return None;
                    }
                    Some((schema_hash.clone(), desc.clone(), key))
                })
                .collect()
        };

        let mut computed = 0;
        let mut skipped = 0;
        for (schema_hash, desc, key) in work {
            match self.embedder.embed_text(&desc) {
                Ok(vec) => {
                    if let Ok(mut embeddings) = self.descriptive_name_embeddings.write() {
                        embeddings.insert(key, vec.clone());
                    }
                    let backend = self.storage.backend();
                    if let Err(e) = backend
                        .save_descriptive_name_embedding(&schema_hash, &vec)
                        .await
                    {
                        tracing::warn!(
                        target: "schema_service::schema",
                                            "warm_descriptive_name_embeddings: failed to persist '{}': {}",
                                            desc,
                                            e
                                        );
                    }
                    computed += 1;
                }
                Err(e) => {
                    tracing::warn!(
                    target: "schema_service::schema",
                                "Failed to embed descriptive_name '{}': {}",
                                desc,
                                e
                            );
                    skipped += 1;
                }
            }
        }
        (computed, skipped)
    }
}
