use super::*;
// lint:file-size-ok moved verbatim from the parent module; one method family per file

impl SchemaServiceState {
    /// Create a new schema service state with **local Last Store** storage.
    ///
    /// Requires the `local-store` feature (server_http / tests). Lambda uses
    /// [`Self::new_with_external`] with S3 instead.
    ///
    /// `db_path` is the Last Store home directory (replaces the former sled
    /// path). The `embedder` is injected by the caller — `schema_service_core`
    /// has zero fastembed/ONNX dependencies. Binaries typically pass
    /// `Arc::new(FoldDbFastEmbedder::new())` (see
    /// `schema_service_server_shared`); tests pass a mock.
    ///
    /// Safe to call from `#[tokio::test]` / inside a runtime: init runs on a
    /// dedicated thread so we never nest `block_on` on the caller's runtime.
    #[cfg(feature = "local-store")]
    pub fn new(db_path: &str, embedder: Arc<dyn Embedder>) -> FoldDbResult<Self> {
        let backend = Arc::new(LastStoreSchemaPersistence::open(db_path)?);
        Self::new_from_backend_blocking(backend, embedder)
    }

    #[cfg(feature = "local-store")]
    pub(super) fn new_from_backend_blocking(
        backend: Arc<dyn ExternalSchemaPersistence>,
        embedder: Arc<dyn Embedder>,
    ) -> FoldDbResult<Self> {
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::Builder::new()
            .name("schema-service-state-init".into())
            .spawn(move || {
                let result = (|| {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .map_err(|e| {
                            FoldDbError::Config(format!(
                                "failed to build tokio runtime for schema state init: {e}"
                            ))
                        })?;
                    rt.block_on(Self::new_with_external(backend, embedder))
                })();
                let _ = tx.send(result);
            })
            .map_err(|e| {
                FoldDbError::Config(format!("failed to spawn schema state init thread: {e}"))
            })?;
        rx.recv().map_err(|_| {
            FoldDbError::Config("schema state init thread ended without a result".to_string())
        })?
    }

    /// Create a schema service state backed by a caller-supplied
    /// persistence implementation.
    ///
    /// Used by remote deployments (e.g. the schema-infra Lambda) that
    /// want to store schema-service data in a cloud service instead
    /// of a local Sled database. The caller implements
    /// [`ExternalSchemaPersistence`] and passes it in via `Arc`.
    ///
    /// `fold_db` owns no knowledge of what's behind the trait —
    /// the implementation lives entirely outside this crate.
    pub async fn new_with_external(
        backend: Arc<dyn ExternalSchemaPersistence>,
        embedder: Arc<dyn Embedder>,
    ) -> FoldDbResult<Self> {
        Self::new_with_external_and_schema_mutation_gate_store(
            backend,
            embedder,
            SchemaMutationGateStore::default(),
        )
        .await
    }

    /// Create externally-backed state with a caller-supplied schema mutation
    /// gate quota store. Lambda uses this to swap local quota accounting for
    /// shared, TTL-backed counters while local/dev paths keep the default.
    pub async fn new_with_external_and_schema_mutation_gate_store(
        backend: Arc<dyn ExternalSchemaPersistence>,
        embedder: Arc<dyn Embedder>,
        schema_mutation_gate_store: SchemaMutationGateStore,
    ) -> FoldDbResult<Self> {
        let collection_name_anchors = Self::compute_anchor_embeddings(embedder.as_ref());

        let state = Self {
            schemas: Arc::new(RwLock::new(HashMap::new())),
            descriptive_name_index: Arc::new(RwLock::new(HashMap::new())),
            descriptive_name_embeddings: Arc::new(RwLock::new(HashMap::new())),
            field_embeddings: Arc::new(RwLock::new(HashMap::new())),
            canonical_fields: Arc::new(RwLock::new(HashMap::new())),
            canonical_field_embeddings: Arc::new(RwLock::new(HashMap::new())),
            native_resolve_embeddings: Arc::new(RwLock::new(HashMap::new())),
            embedder,
            storage: SchemaStorage::External(backend),
            system_schema_hashes: Arc::new(RwLock::new(HashSet::new())),
            seeding_in_progress: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            collection_name_anchors,
            near_misses: Arc::new(RwLock::new(Vec::new())),
            state_version: Arc::new(AtomicU64::new(0)),
            schema_writes: Arc::new(AtomicU64::new(0)),
            apps: Arc::new(RwLock::new(HashMap::new())),
            app_releases: Arc::new(RwLock::new(HashMap::new())),
            app_channels: Arc::new(RwLock::new(HashMap::new())),
            declared_fields: Arc::new(RwLock::new(
                crate::declared_fields::DeclaredFieldRegistry::new(),
            )),
            app_identity: Arc::new(RwLock::new(
                crate::app_identity::AppIdentityConfig::default(),
            )),
            schema_match_telemetry: Arc::new(RwLock::new(SchemaMatchTelemetrySnapshot::default())),
            schema_mutation_gate_config: Arc::new(RwLock::new(SchemaMutationGateConfig::default())),
            schema_mutation_gate_store,
        };

        state.load_all_from_external().await?;
        state.rebuild_descriptive_name_index();

        Ok(state)
    }

    /// Populate every in-memory cache from the external backend.
    /// Called from `new_with_external`.
    // lint:fn-size-ok moved verbatim from the parent module; splitting is separate work.
    pub(super) async fn load_all_from_external(&self) -> FoldDbResult<()> {
        let backend = self.storage.backend().clone();

        // Schemas + their persisted descriptive_name embeddings. Same
        // story as canonical fields below — the embeddings were the
        // cold-start bottleneck (>110s at 200 schemas × ~40ms per
        // fastembed call); now they're loaded from the blob that
        // `add_schema` maintains. Descriptive_name embeddings are
        // keyed by the schema's `identity_hash`, so after the
        // descriptive_name index is rebuilt from the loaded schemas,
        // we bridge the two.
        let loaded_schemas = backend.load_all_schemas().await?;
        let loaded_name_embeddings = backend
            .load_descriptive_name_embeddings()
            .await
            .unwrap_or_default();
        {
            let mut schemas = write_lock(&self.schemas, "schemas")?;
            schemas.clear();
            schemas.extend(loaded_schemas);
            tracing::info!(
            target: "schema_service::schema",
                "Schema service loaded {} schemas from external backend",
                schemas.len()
            );
        }
        // Bridge identity-hash-keyed persisted embeddings to the
        // descriptive_name-keyed in-memory cache that
        // `state_matching::find_similar_schema` reads. We need the
        // schemas to be loaded first so we can resolve each
        // identity_hash to the schema's current `descriptive_name`.
        {
            let schemas = read_lock(&self.schemas, "schemas")?;
            let mut embeddings = write_lock(
                &self.descriptive_name_embeddings,
                "descriptive_name_embeddings",
            )?;
            embeddings.clear();
            for (schema_hash, embedding) in loaded_name_embeddings {
                let Some(schema) = schemas.get(&schema_hash) else {
                    // Embedding for a schema that's no longer in the
                    // registry. Skip — the blob is a cache, not source
                    // of truth.
                    continue;
                };
                if schema.superseded_by.is_some() {
                    continue;
                }
                let Some(ref desc) = schema.descriptive_name else {
                    continue;
                };
                // Key by the same namespaced form used in
                // `descriptive_name_index` so `find_matching_descriptive_name`
                // can scope its semantic-similarity loop to schemas owned by
                // the same app.
                let key = descriptive_name_key(schema.owner_app_id.as_deref(), desc);
                embeddings.insert(key, embedding);
            }
            tracing::info!(
            target: "schema_service::schema",
                "Schema service bridged {} persisted descriptive_name embeddings",
                embeddings.len()
            );
        }

        // Canonical fields — populate the registry AND the persisted
        // embedding cache. Embeddings are NEVER re-computed at cold
        // start anymore: that path used to run fastembed on every
        // loaded field and blew past Lambda's 10s init cap when the
        // registry had >100 entries. The embeddings blob is
        // maintained by the write path (`add_canonical_field` calls
        // `save_canonical_field_embedding` after each successful
        // embed) and by a one-time backfill endpoint
        // (`POST /v1/admin/warm-embeddings`). If the blob is empty
        // (first deploy post-feature, pre-backfill), semantic
        // similarity falls back to the no-match branch — documented
        // degradation, not a failure.
        let loaded_canonical = backend.load_all_canonical_fields().await?;
        let loaded_canonical_embeddings = backend
            .load_canonical_field_embeddings()
            .await
            .unwrap_or_default();
        {
            let mut fields = write_lock(&self.canonical_fields, "canonical_fields")?;
            let mut embeddings = write_lock(
                &self.canonical_field_embeddings,
                "canonical_field_embeddings",
            )?;
            fields.clear();
            embeddings.clear();
            for (name, canonical) in loaded_canonical {
                fields.insert(name, canonical);
            }
            for (name, embedding) in loaded_canonical_embeddings {
                embeddings.insert(name, embedding);
            }
            tracing::info!(
            target: "schema_service::schema",
                "Schema service loaded {} canonical fields + {} persisted embeddings from external backend",
                fields.len(),
                embeddings.len()
            );
        }

        // Near-misses (Phase C shadow-mode audit log). Backends that
        // didn't opt in return an empty vec from the default trait impl,
        // which is the correct degraded state — shadow observability is
        // best-effort and must never fail startup.
        let loaded_near_misses = backend.load_all_near_misses().await.unwrap_or_default();
        {
            let mut near_misses = write_lock(&self.near_misses, "near_misses")?;
            let count = loaded_near_misses.len();
            *near_misses = loaded_near_misses;
            tracing::info!(
                target: "schema_service::schema",
                "Schema service loaded {} canonicalization near-misses from external backend",
                count
            );
        }

        // Apps (canonical app registry, app_identity v3.1 Lane B2b).
        // Backends that predate the apps blob return an empty map from the
        // default trait impl.
        let loaded_apps = backend.load_all_apps().await.unwrap_or_default();
        {
            let mut apps = write_lock(&self.apps, "apps")?;
            apps.clear();
            let count = loaded_apps.len();
            apps.extend(loaded_apps);
            tracing::info!(
                target: "schema_service::app_identity",
                "Schema service loaded {} apps from external backend",
                count
            );
        }

        // App releases + channels (`/v2` registry). Same opt-in shape: a
        // backend that predates the release store returns empty maps, so the
        // service starts with no published releases rather than failing.
        let loaded_releases = backend.load_all_releases().await.unwrap_or_default();
        let loaded_channels = backend.load_all_channels().await.unwrap_or_default();
        {
            let releases = loaded_releases.len();
            let channels = loaded_channels.len();
            self.install_loaded_releases(loaded_releases, loaded_channels)?;
            tracing::info!(
                target: "schema_service::app_release",
                "Schema service loaded {} app releases and {} channels from external backend",
                releases,
                channels
            );
        }

        // Declared fields (brain `design-lastdb-declared-fields`). Rebuilding
        // BOTH indexes here is what keeps a re-declare idempotent across a
        // restart — without the `(owner, handle)` index a client's next
        // declare would mint a fresh id and silently fork every identity it
        // had already stamped into its schemas.
        let loaded_declared_fields = backend.load_all_declared_fields().await.unwrap_or_default();
        {
            let mut declared = write_lock(&self.declared_fields, "declared_fields")?;
            let count = loaded_declared_fields.len();
            *declared = crate::declared_fields::DeclaredFieldRegistry::new();
            declared.load(loaded_declared_fields);
            tracing::info!(
                target: "schema_service::declared_fields",
                "Schema service loaded {} field declarations from external backend",
                count
            );
        }

        Ok(())
    }

    /// Load all schemas from storage
    pub async fn load_schemas(&self) -> FoldDbResult<()> {
        let loaded = self.storage.backend().load_all_schemas().await?;
        let mut schemas = write_lock(&self.schemas, "schemas")?;
        schemas.clear();
        let count = loaded.len();
        schemas.extend(loaded);
        tracing::info!(
            target: "schema_service::schema",
            "Schema service reloaded {} schemas from storage backend",
            count
        );
        Ok(())
    }

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
