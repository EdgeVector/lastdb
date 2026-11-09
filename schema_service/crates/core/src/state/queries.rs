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
    /// Callers look up a schema by descriptive name, so this fallback
    /// stays.
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
}
