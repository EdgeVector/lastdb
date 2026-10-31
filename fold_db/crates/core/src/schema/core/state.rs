use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use super::locks::{lock_map, lock_set, read_map, write_map};
use super::SchemaCore;
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
        let schema_core = Self {
            schemas,
            schema_states: Arc::new(Mutex::new(std::collections::HashMap::new())),
            superseded_by: Arc::new(Mutex::new(std::collections::HashMap::new())),
            retired_name_claims: Arc::new(Mutex::new(std::collections::HashSet::new())),
            db_ops: db_ops.clone(),
            catalog_ready: AtomicBool::new(false),
        };

        // Hydrate from durable store (may take minutes on large post-rekey homes).
        let mut stored_schemas = db_ops.get_all_schemas().await?;
        // The home-level marker replaces the per-schema flag as the proof of
        // the owned layout. A serving node writes it after this admission
        // (`FoldDB::initialize_from_init`).
        let home_has_owned_layout_marker = db_ops.atoms().owned_layout_recorded().await?;
        for schema in stored_schemas.values_mut() {
            super::load::owned::admit_stored_schema(schema, home_has_owned_layout_marker)?;
        }
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

    /// Mark the in-memory catalog ready for list/query after the initial store scan.
    pub(crate) fn mark_catalog_ready(&self) {
        self.catalog_ready.store(true, Ordering::Release);
    }

    pub fn get_schema_states(
        &self,
    ) -> Result<std::collections::HashMap<String, SchemaState>, SchemaError> {
        let mut states = lock_map(&self.schema_states, "schema_states")?.clone();
        for (name, staged) in self.staged_schemas()? {
            states.insert(name, staged.state);
        }
        Ok(states)
    }

    pub fn get_schema_state_cached(&self, schema_name: &str) -> Result<SchemaState, SchemaError> {
        if self.native_read_snapshot_active() {
            return self.captured_schema_state(schema_name)?.ok_or_else(|| {
                SchemaError::StorageRead {
                    detail: "native schema state requires its captured catalog value".into(),
                }
            });
        }
        if let Some(staged) = self.staged_schema(schema_name)? {
            return Ok(staged.state);
        }
        Ok(lock_map(&self.schema_states, "schema_states")?
            .get(schema_name)
            .copied()
            .unwrap_or_default())
    }

    /// All schema value and key reads share the resolved schema's state gate.
    pub(crate) fn require_queryable_schema(&self, schema_name: &str) -> Result<(), SchemaError> {
        if self.get_schema_state_cached(schema_name)? == SchemaState::Blocked {
            return Err(SchemaError::Blocked(format!(
                "Schema '{schema_name}' is blocked and cannot be queried"
            )));
        }
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

    /// The cached identities whose `owner_app_id` equals `owner_app`.
    ///
    /// # Errors
    /// [`SchemaError::InvalidData`] when the in-memory catalog lock is poisoned.
    pub fn schema_names_owned_by(&self, owner_app: &str) -> Result<Vec<String>, SchemaError> {
        let schemas = read_map(&self.schemas, "schemas")?;
        Ok(schemas
            .iter()
            .filter(|(_, schema)| schema.owner_app_id.as_deref() == Some(owner_app))
            .map(|(name, _)| name.clone())
            .collect())
    }

    async fn set_name_claim_retired(
        &self,
        schema_name: &str,
        retired: bool,
    ) -> Result<bool, SchemaError> {
        // Persist first. The durable record is the source of truth.
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
}
