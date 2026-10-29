use super::*;
use crate::schema_config::SchemaConfig;

impl SchemaServiceState {
    /// Create a schema service state whose data lives only in memory.
    ///
    /// Backed by [`InMemorySchemaPersistence`]: nothing survives a restart,
    /// so a deployed service must never call this.
    ///
    /// Used by the dev server, the eval tool and the offline seed generator.
    /// Lambda uses [`Self::new_with_external`] with S3 instead.
    ///
    /// The `embedder` is injected by the caller — `schema_service_core`
    /// has zero fastembed/ONNX dependencies. Binaries typically pass
    /// `Arc::new(FoldDbFastEmbedder::new())` (see
    /// `schema_service_server_shared`).
    ///
    /// Safe to call inside a runtime: the injected runtime runs init on a
    /// dedicated thread, so this does not nest on the caller's runtime.
    pub fn new_in_memory(
        embedder: Arc<dyn Embedder>,
        config: SchemaConfig,
        clock: Arc<dyn crate::clock::Clock>,
        runtime: Arc<dyn crate::blocking_runtime::BlockingRuntime>,
        ids: Arc<dyn crate::ids::IdGen>,
    ) -> FoldDbResult<Self> {
        let backend = Arc::new(InMemorySchemaPersistence::default());
        Self::new_from_backend_blocking(backend, embedder, config, clock, runtime, ids)
    }

    fn new_from_backend_blocking(
        backend: Arc<dyn ExternalSchemaPersistence>,
        embedder: Arc<dyn Embedder>,
        config: SchemaConfig,
        clock: Arc<dyn crate::clock::Clock>,
        runtime: Arc<dyn crate::blocking_runtime::BlockingRuntime>,
        ids: Arc<dyn crate::ids::IdGen>,
    ) -> FoldDbResult<Self> {
        let init = Arc::clone(&runtime);
        let boxed = crate::blocking_runtime::drive(
            init.as_ref(),
            "schema-service-state-init",
            "failed to build tokio runtime for schema state init",
            "failed to spawn schema state init thread",
            "schema state init thread ended without a result",
            Box::pin(async move {
                let state =
                    Self::new_with_external(backend, embedder, config, clock, runtime, ids).await?;
                Ok(Box::new(state) as crate::blocking_runtime::BlockingOutput)
            }),
        )?;
        Ok(crate::blocking_runtime::downcast(boxed))
    }

    /// Create a schema service state backed by a caller-supplied
    /// persistence implementation.
    ///
    /// Used by remote deployments (e.g. the schema-infra Lambda) that
    /// want to store schema-service data in a cloud service instead
    /// of process memory. The caller implements
    /// [`ExternalSchemaPersistence`] and passes it in via `Arc`.
    ///
    /// `fold_db` owns no knowledge of what's behind the trait —
    /// the implementation lives entirely outside this crate.
    pub async fn new_with_external(
        backend: Arc<dyn ExternalSchemaPersistence>,
        embedder: Arc<dyn Embedder>,
        config: SchemaConfig,
        clock: Arc<dyn crate::clock::Clock>,
        runtime: Arc<dyn crate::blocking_runtime::BlockingRuntime>,
        ids: Arc<dyn crate::ids::IdGen>,
    ) -> FoldDbResult<Self> {
        Self::new_with_external_and_schema_mutation_gate_store(
            backend,
            embedder,
            SchemaMutationGateStore::default(),
            config,
            clock,
            runtime,
            ids,
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
        config: SchemaConfig,
        clock: Arc<dyn crate::clock::Clock>,
        runtime: Arc<dyn crate::blocking_runtime::BlockingRuntime>,
        ids: Arc<dyn crate::ids::IdGen>,
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
            anti_field_embedding_cache: Arc::new(RwLock::new(None)),
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
            declared_field_writes: Arc::new(tokio::sync::Mutex::new(())),
            app_identity: Arc::new(RwLock::new(config.app_identity.clone())),
            schema_match_telemetry: Arc::new(RwLock::new(SchemaMatchTelemetrySnapshot::default())),
            schema_mutation_gate_config: Arc::new(RwLock::new(config.mutation_gate.clone())),
            schema_mutation_gate_store,
            schema_config: Arc::new(config),
            clock,
            runtime,
            ids,
        };

        state.load_all_from_external().await?;
        state.rebuild_descriptive_name_index();

        Ok(state)
    }

    /// Populate every in-memory cache from the external backend.
    /// Called from `new_with_external`.
    pub(super) async fn load_all_from_external(&self) -> FoldDbResult<()> {
        self.load_schemas_and_name_embeddings().await?;
        self.load_canonical_fields_and_embeddings().await?;
        self.load_near_misses_cache().await?;
        self.load_apps_cache().await?;
        self.load_releases_and_channels_cache().await?;
        self.load_declared_fields_cache().await?;
        self.validate_loaded_vocabulary()?;
        Ok(())
    }

    /// Schemas and their persisted descriptive_name embeddings.
    async fn load_schemas_and_name_embeddings(&self) -> FoldDbResult<()> {
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

        Ok(())
    }

    /// Canonical fields and their persisted embeddings.
    async fn load_canonical_fields_and_embeddings(&self) -> FoldDbResult<()> {
        let backend = self.storage.backend().clone();

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
                if fields.contains_key(&name) {
                    embeddings.insert(name, embedding);
                }
            }
            tracing::info!(
            target: "schema_service::schema",
                "Schema service loaded {} canonical fields + {} persisted embeddings from external backend",
                fields.len(),
                embeddings.len()
            );
        }

        Ok(())
    }

    /// Canonicalization near-misses.
    async fn load_near_misses_cache(&self) -> FoldDbResult<()> {
        let backend = self.storage.backend().clone();

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

        Ok(())
    }

    /// Canonical app registry.
    async fn load_apps_cache(&self) -> FoldDbResult<()> {
        let backend = self.storage.backend().clone();

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

        Ok(())
    }

    /// App releases and channels.
    async fn load_releases_and_channels_cache(&self) -> FoldDbResult<()> {
        let backend = self.storage.backend().clone();

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

        Ok(())
    }

    /// Declared fields.
    async fn load_declared_fields_cache(&self) -> FoldDbResult<()> {
        let backend = self.storage.backend().clone();

        // Declared fields (brain `design-lastdb-declared-fields`). Rebuilding
        // BOTH indexes here is what keeps a re-declare idempotent across a
        // restart — without the `(owner, handle)` index a client's next
        // declare would mint a fresh id and silently fork every identity it
        // had already stamped into its schemas.
        let loaded_declared_fields = backend.load_all_declared_fields().await?;
        self.validate_loaded_declarations(&loaded_declared_fields)?;
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
        self.validate_loaded_schemas(&loaded).await?;
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
}

mod warm;
