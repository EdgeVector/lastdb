use super::*;

impl SchemaServiceState {
    /// Advance the monotonic state-version counter and return the new
    /// value. Call this from every state-changing write path so
    /// `GET /v1/snapshot` clients can detect mid-flight changes (app_identity
    /// v3.1, Lane B2a). Cheap (`fetch_add`); fine to call inside a critical
    /// section.
    ///
    /// The counter is over-counting-safe: the contract is monotonicity, not
    /// exactness. Bumping at the entrypoint of a mutating method even when
    /// the method ultimately no-ops (e.g. add_schema sees a duplicate) still
    /// satisfies the client contract — clients re-fetch the snapshot on
    /// version change and the bytes are unchanged so it's a cheap miss.
    pub fn bump_state_version(&self) -> u64 {
        self.state_version.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Count one catalog write attempt. See [`Self::schema_writes`].
    pub fn record_schema_write(&self) -> u64 {
        self.schema_writes.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Read the catalog write counter without advancing it.
    #[must_use]
    pub fn current_schema_writes(&self) -> u64 {
        self.schema_writes.load(Ordering::SeqCst)
    }

    /// Read the current state-version counter without advancing it.
    pub fn current_state_version(&self) -> u64 {
        self.state_version.load(Ordering::SeqCst)
    }

    /// Look up the active schema bound to `(owner_app_id, descriptive_name)`
    /// in `descriptive_name_index`, verifying the resolved schema actually
    /// lives in the requested namespace before returning its identity hash.
    ///
    /// [`descriptive_name_key`] is not injective on
    /// `(Option<&str>, &str)`: `(None, "kanban/Tasks")` and
    /// `(Some("kanban"), "Tasks")` both encode to `"kanban/Tasks"`. A raw
    /// `index.get(key)` therefore can smuggle a schema from another
    /// namespace into a same-key hit — registering an app-owned
    /// `kanban/Tasks` against a legacy un-owned schema whose
    /// `descriptive_name` happens to be `"kanban/Tasks"` would otherwise
    /// route into the legacy slot (spurious expansion or a 409
    /// `DescriptiveNameConflict`). Re-resolving through the schemas map and
    /// discarding mismatched owners restores the namespace invariant the
    /// dedup index is meant to enforce.
    pub(crate) fn lookup_descriptive_name_in_namespace(
        &self,
        owner_app_id: Option<&str>,
        descriptive_name: &str,
    ) -> FoldDbResult<Option<String>> {
        let key = descriptive_name_key(owner_app_id, descriptive_name);
        let hash = {
            let index = read_lock(&self.descriptive_name_index, "descriptive_name_index")?;
            index.get(&key).cloned()
        };
        let Some(hash) = hash else {
            return Ok(None);
        };
        let schemas = read_lock(&self.schemas, "schemas")?;
        if let Some(existing) = schemas.get(&hash) {
            fn normalize(s: Option<&str>) -> Option<&str> {
                s.filter(|x| !x.is_empty())
            }
            if normalize(existing.owner_app_id.as_deref()) == normalize(owner_app_id) {
                return Ok(Some(hash));
            }
        }
        Ok(None)
    }

    pub fn schema_match_telemetry_snapshot(&self) -> SchemaMatchTelemetrySnapshot {
        self.schema_match_telemetry
            .read()
            .map(|snapshot| snapshot.clone())
            .unwrap_or_default()
    }

    pub fn record_schema_match_outcome(
        &self,
        outcome: &'static str,
        source: &str,
        fallback_reason: Option<&str>,
    ) {
        let Ok(mut snapshot) = self.schema_match_telemetry.write() else {
            return;
        };
        *snapshot.outcomes.entry(outcome.to_string()).or_insert(0) += 1;
        *snapshot
            .sources
            .entry(safe_telemetry_label(source))
            .or_insert(0) += 1;
        if let Some(reason) = fallback_reason {
            *snapshot
                .fallback_reasons
                .entry(safe_telemetry_label(reason))
                .or_insert(0) += 1;
        }
    }

    /// Get all schema names (public accessor for Lambda integration)
    pub fn get_schema_names(&self) -> FoldDbResult<Vec<String>> {
        let schemas = read_lock(&self.schemas, "schemas")?;
        Ok(schemas.keys().cloned().collect())
    }

    /// Get all schemas (public accessor for Lambda integration)
    pub fn get_all_schemas_cached(&self) -> FoldDbResult<Vec<Schema>> {
        let schemas = read_lock(&self.schemas, "schemas")?;
        Ok(schemas.values().cloned().collect())
    }

    /// Get a schema by name (public accessor for Lambda integration).
    ///
    /// Accepts either a content-addressed identity hash (the stable
    /// key every schema is indexed under) or a descriptive name like
    /// `"Persona"`. Descriptive-name lookups resolve via the
    /// `descriptive_name_index`, which is populated from the stored
    /// schemas' `descriptive_name` field on every load/rebuild and
    /// does NOT depend on the embedding model — so this path works
    /// even in Lambda environments where fastembed can't fetch its
    /// ONNX weights.
    ///
    /// fold_db_node always fetches Phase 1 built-ins by descriptive
    /// name at startup, so this fallback is load-bearing for booting
    /// any node against a fresh schema service.
    pub fn get_schema_by_name(&self, name: &str) -> FoldDbResult<Option<Schema>> {
        let schemas = read_lock(&self.schemas, "schemas")?;

        // Fast path: direct lookup by content-addressed identity hash.
        if let Some(schema) = schemas.get(name).cloned() {
            return Ok(Some(schema));
        }

        // Fallback: resolve `name` as a descriptive_name, then look
        // up the resulting identity_hash. Drop the schemas lock
        // before re-acquiring to avoid lock upgrade paths.
        drop(schemas);
        let resolved_hash: Option<String> = {
            let index = read_lock(&self.descriptive_name_index, "descriptive_name_index")?;
            index.get(name).cloned()
        };
        if let Some(hash) = resolved_hash {
            let schemas = read_lock(&self.schemas, "schemas")?;
            return Ok(schemas.get(&hash).cloned());
        }

        Ok(None)
    }

    /// Get schema count (public accessor for Lambda integration)
    pub fn get_schema_count(&self) -> usize {
        self.schemas.read().map_or(0, |s| s.len())
    }

    /// Record an identity hash as a system/infrastructure schema.
    ///
    /// Called by `builtin_schemas::seed()` during startup for each
    /// Phase 1 built-in. Idempotent. See
    /// [`SchemaServiceState::system_schema_hashes`] for background.
    pub fn mark_system_schema(&self, identity_hash: String) {
        if let Ok(mut set) = self.system_schema_hashes.write() {
            set.insert(identity_hash);
        }
    }

    /// Mark the start/end of an authoritative seed load. Wrap each committed
    /// seeder body in `set_seeding_in_progress(true)` … `(false)` so the fuzzy
    /// reuse-before-NEW path is disabled for those `add_schema` calls — seed
    /// schemas must register exactly as committed, never fuzzily merge into a
    /// near-neighbour. See [`Self::seeding_in_progress`].
    pub fn set_seeding_in_progress(&self, on: bool) {
        self.seeding_in_progress
            .store(on, std::sync::atomic::Ordering::SeqCst);
    }

    /// Whether an authoritative seeder is currently loading committed schemas.
    pub(crate) fn is_seeding_in_progress(&self) -> bool {
        self.seeding_in_progress
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    /// True if the given schema is a system/infrastructure schema
    /// (one of the Phase 1 built-ins such as Fingerprint, Edge,
    /// Identity, Persona, etc.).
    ///
    /// Accepts either an identity hash (the canonical storage key)
    /// or a descriptive_name (`"Persona"`, `"Fingerprint"`, …). This
    /// matches `get_schema_by_name`'s lookup semantics so callers
    /// can classify a schema by whatever key they already have.
    pub fn is_system_schema(&self, schema_name_or_hash: &str) -> bool {
        if let Ok(set) = self.system_schema_hashes.read() {
            if set.contains(schema_name_or_hash) {
                return true;
            }
        }

        let resolved_hash = match self.descriptive_name_index.read() {
            Ok(index) => index.get(schema_name_or_hash).cloned(),
            Err(_) => return false,
        };

        if let Some(hash) = resolved_hash {
            if let Ok(set) = self.system_schema_hashes.read() {
                return set.contains(&hash);
            }
        }

        false
    }
}
