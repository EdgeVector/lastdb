use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use super::locks::{lock_map, lock_set, read_map, write_map};
use super::SchemaCore;
use crate::schema::field_mapper::FieldMapperService;
use crate::schema::types::SchemaError;
use crate::schema::SchemaState;

impl SchemaCore {
    /// Creates a new SchemaCore with DbOperations (storage abstraction)
    pub async fn new(
        db_ops: std::sync::Arc<crate::db_operations::DbOperations>,
    ) -> Result<Self, SchemaError> {
        // Start not-ready so any concurrent reader during the store scan sees
        // 503 rather than empty-ok (the post-rekey hang window).
        let schemas = Arc::new(RwLock::new(std::collections::HashMap::new()));
        let field_mapper = FieldMapperService::new(db_ops.clone(), schemas.clone());
        let schema_core = Self {
            schemas,
            schema_states: Arc::new(Mutex::new(std::collections::HashMap::new())),
            superseded_by: Arc::new(Mutex::new(std::collections::HashMap::new())),
            retired_name_claims: Arc::new(Mutex::new(std::collections::HashSet::new())),
            db_ops: db_ops.clone(),
            field_mapper,
            catalog_ready: AtomicBool::new(false),
            coherence_bound: Arc::new(Mutex::new(std::collections::HashMap::new())),
        };

        // Hydrate from durable store (may take minutes on large post-rekey homes).
        let stored_schemas = db_ops.get_all_schemas().await?;
        let schema_states = db_ops.get_all_schema_states().await?;
        let superseded_by = db_ops.get_all_superseded_by().await?;
        let retired_name_claims = db_ops.list_retired_name_claims().await?;

        {
            let mut guard = write_map(&schema_core.schemas, "schemas")?;
            *guard = stored_schemas;
        }
        {
            let mut guard = lock_map(&schema_core.schema_states, "schema_states")?;
            *guard = schema_states;
        }
        {
            let mut guard = lock_map(&schema_core.superseded_by, "superseded_by")?;
            *guard = superseded_by;
        }
        {
            let mut guard = lock_set(&schema_core.retired_name_claims, "retired_name_claims")?;
            *guard = retired_name_claims.into_iter().collect();
        }
        schema_core.mark_catalog_ready();

        Ok(schema_core)
    }

    /// Whether the initial schema catalog load from the store has finished.
    ///
    /// Used by `GET /api/schemas` so a hydrating node answers 503 not-ready
    /// instead of `{count:0, ok:true}` (safe-upgrade false RED 2026-07-30).
    #[must_use]
    pub fn catalog_ready(&self) -> bool {
        self.catalog_ready.load(Ordering::Acquire)
    }

    /// Mark the in-memory catalog ready for list/query (production path after
    /// the initial store scan, and after a full reload_from_store completes).
    pub(crate) fn mark_catalog_ready(&self) {
        self.catalog_ready.store(true, Ordering::Release);
    }

    /// Production path: begin a catalog rehydrate (e.g. full reload). While
    /// false, `GET /api/schemas` returns 503 not-ready.
    pub(crate) fn mark_catalog_not_ready(&self) {
        self.catalog_ready.store(false, Ordering::Release);
    }

    /// Test/helper: force the catalog readiness flag (e.g. simulate hydration).
    pub fn set_catalog_ready_for_test(&self, ready: bool) {
        self.catalog_ready.store(ready, Ordering::Release);
    }

    pub fn get_schema_states(
        &self,
    ) -> Result<std::collections::HashMap<String, SchemaState>, SchemaError> {
        Ok(lock_map(&self.schema_states, "schema_states")?.clone())
    }

    pub fn get_schema_state_cached(&self, schema_name: &str) -> Result<SchemaState, SchemaError> {
        Ok(lock_map(&self.schema_states, "schema_states")?
            .get(schema_name)
            .copied()
            .unwrap_or_default())
    }

    pub async fn set_schema_state(
        &self,
        schema_name: &str,
        schema_state: SchemaState,
    ) -> Result<(), SchemaError> {
        // Persist to database first - this is the source of truth
        self.db_ops
            .store_schema_state(schema_name, &schema_state)
            .await?;

        // Update in-memory cache only after successful persistence
        lock_map(&self.schema_states, "schema_states")?
            .insert(schema_name.to_string(), schema_state);

        Ok(())
    }

    pub async fn block_schema(&self, schema_name: &str) -> Result<(), SchemaError> {
        self.set_schema_state(schema_name, SchemaState::Blocked)
            .await
    }

    /// Block a schema and record its successor for query redirection.
    /// Used during schema expansion: the old schema is blocked locally,
    /// and queries against it transparently redirect to the new schema.
    pub async fn block_and_supersede(
        &self,
        old_name: &str,
        new_name: &str,
    ) -> Result<(), SchemaError> {
        self.set_schema_state(old_name, SchemaState::Blocked)
            .await?;

        self.db_ops.store_superseded_by(old_name, new_name).await?;

        lock_map(&self.superseded_by, "superseded_by")?
            .insert(old_name.to_string(), new_name.to_string());

        Ok(())
    }

    /// Retire one schema's claim on its `descriptive_name`.
    ///
    /// This is the supported retire primitive. It exists because the two
    /// operations that look like they should do this cannot:
    ///
    /// - Renaming the schema is not a rename. `descriptive_name` folds into
    ///   `compute_identity_hash_parts`, so re-declaring under another name
    ///   mints a NEW schema and leaves the old one Available under the old
    ///   name — the duplicate claim survives, now with a sibling.
    /// - [`Self::block_schema`] retires too much. A `Blocked` schema is
    ///   redirected by [`Self::get_schema_following_supersession`] for EVERY
    ///   lookup, including one that names it by identity hash, so it breaks
    ///   every by-hash pin that deliberately addresses the predecessor.
    ///
    /// Retiring the name claim changes exactly one thing: descriptive-name
    /// resolution stops offering this schema as a candidate. Its identity
    /// hash, its [`SchemaState`], its data, and every by-hash or
    /// by-canonical-name read are untouched.
    ///
    /// Returns `true` when the durable record changed. Idempotent.
    ///
    /// # Errors
    /// [`SchemaError::NotFound`] when no schema is installed under
    /// `schema_name`, so a typo cannot silently retire nothing.
    pub async fn retire_name_claim(&self, schema_name: &str) -> Result<bool, SchemaError> {
        self.set_name_claim_retired(schema_name, true).await
    }

    /// Restore a retired claim so the schema answers `descriptive_name`
    /// resolution again. The inverse of [`Self::retire_name_claim`].
    ///
    /// # Errors
    /// [`SchemaError::NotFound`] when no schema is installed under
    /// `schema_name`.
    pub async fn restore_name_claim(&self, schema_name: &str) -> Result<bool, SchemaError> {
        self.set_name_claim_retired(schema_name, false).await
    }

    /// Remove one installed schema identity from the catalog and the RAM cache.
    ///
    /// ACK is catalog absence: later `get_schema_metadata` misses. Product
    /// tips stay until a janitor. Returns whether the catalog had the
    /// identity. A miss still evicts RAM so a retry is idempotent.
    pub async fn drop_schema(&self, schema_name: &str) -> Result<bool, SchemaError> {
        let existed = self.db_ops.drop_schema(schema_name).await?;
        {
            let mut schemas = write_map(&self.schemas, "schemas")?;
            schemas.remove(schema_name);
        }
        {
            let mut states = lock_map(&self.schema_states, "schema_states")?;
            states.remove(schema_name);
        }
        {
            let mut claims = lock_set(&self.retired_name_claims, "retired_name_claims")?;
            claims.remove(schema_name);
        }
        {
            let mut superseded = lock_map(&self.superseded_by, "superseded_by")?;
            superseded.remove(schema_name);
        }
        Ok(existed)
    }

    /// Drop every cached identity whose `owner_app_id` equals `owner_app`.
    ///
    /// Walks the in-memory catalog (O(installed schemas)), not product rows.
    /// Returns the canonical names that were present and dropped.
    pub async fn drop_schemas_owned_by(&self, owner_app: &str) -> Result<Vec<String>, SchemaError> {
        let names: Vec<String> = {
            let schemas = read_map(&self.schemas, "schemas")?;
            schemas
                .iter()
                .filter(|(_, schema)| schema.owner_app_id.as_deref() == Some(owner_app))
                .map(|(name, _)| name.clone())
                .collect()
        };
        let mut dropped = Vec::new();
        for name in names {
            if self.drop_schema(&name).await? {
                dropped.push(name);
            }
        }
        dropped.sort();
        Ok(dropped)
    }

    async fn set_name_claim_retired(
        &self,
        schema_name: &str,
        retired: bool,
    ) -> Result<bool, SchemaError> {
        // Persist first — the durable record is the source of truth, exactly
        // like `set_schema_state`.
        let changed = self
            .db_ops
            .set_schema_name_claim_retired(schema_name, retired)
            .await?;
        let mut guard = lock_set(&self.retired_name_claims, "retired_name_claims")?;
        if retired {
            guard.insert(schema_name.to_string());
        } else {
            guard.remove(schema_name);
        }
        Ok(changed)
    }

    /// True when this schema no longer answers `descriptive_name` resolution.
    ///
    /// # Errors
    /// [`SchemaError::InvalidData`] when the in-memory set lock is poisoned.
    pub fn name_claim_retired(&self, schema_name: &str) -> Result<bool, SchemaError> {
        Ok(lock_set(&self.retired_name_claims, "retired_name_claims")?.contains(schema_name))
    }

    /// Every schema name whose claim on its `descriptive_name` is retired.
    ///
    /// # Errors
    /// [`SchemaError::InvalidData`] when the in-memory set lock is poisoned.
    pub fn retired_name_claims(&self) -> Result<std::collections::HashSet<String>, SchemaError> {
        Ok(lock_set(&self.retired_name_claims, "retired_name_claims")?.clone())
    }

    /// Get the database operations.
    pub fn db_ops(&self) -> &Arc<crate::db_operations::DbOperations> {
        &self.db_ops
    }

    /// Creates a new SchemaCore for testing purposes with a temporary database
    pub async fn new_for_testing() -> Result<Self, SchemaError> {
        let tmp = tempfile::TempDir::new()
            .map_err(|e| SchemaError::InvalidData(e.to_string()))?
            .keep();
        let store = std::sync::Arc::new(
            crate::storage::LastStoreNamespacedStore::open(&tmp)
                .map_err(|e| SchemaError::InvalidData(e.to_string()))?,
        ) as std::sync::Arc<dyn crate::storage::NamespacedStore>;
        let db_ops = std::sync::Arc::new(
            crate::db_operations::DbOperations::from_namespaced_store(store)
                .await
                .map_err(|e| SchemaError::InvalidData(e.to_string()))?,
        );
        Self::new(db_ops).await
    }
}
