use super::SchemaCore;

pub(super) mod owned;
mod rules;
use crate::schema::types::{Schema, SchemaError};
use crate::schema::SchemaState;
use schema_types::IdentityRecompute;

impl SchemaCore {
    /// Fetches a schema by name, checking both in-memory cache and database.
    /// This is the resolution the executor uses, so any field/owner-validation
    /// gate that runs alongside the executor must use this method — not
    /// [`Self::get_schema_metadata`] — or it will disagree with what the
    /// executor actually serves (see #618).
    ///
    /// Note: This is STRICTLY case-sensitive.
    pub async fn get_schema_following_supersession(
        &self,
        schema_name: &str,
    ) -> Result<Option<Schema>, SchemaError> {
        self.get_schema_resolved(schema_name, false).await
    }

    /// Read-path variant of [`Self::get_schema_following_supersession`]. The
    /// in-memory hit is a [`Schema::clone_for_read_shared`] copy taken under a
    /// shared read lock, so concurrent queries resolve their schema in
    /// parallel. A field holds no data, so the copy is small. Use this
    /// whenever the result drives a query read.
    pub async fn get_schema_following_supersession_for_read(
        &self,
        schema_name: &str,
    ) -> Result<Option<Schema>, SchemaError> {
        self.get_schema_resolved(schema_name, true).await
    }

    /// Internal resolution helper. When `for_read` is set, the in-memory hit
    /// is a [`Schema::clone_for_read_shared`] copy under a shared read lock
    /// (the hot query path); otherwise it comes from the metadata lookup.
    async fn get_schema_resolved(
        &self,
        schema_name: &str,
        for_read: bool,
    ) -> Result<Option<Schema>, SchemaError> {
        if self.native_read_snapshot_active() {
            return self.resolve_native_schema(schema_name, for_read).await;
        }

        // 1. Try exact match in memory
        if for_read {
            // Hot query path: `clone_for_read_shared` takes `&self`, so
            // this runs under a SHARED read lock — N concurrent readers
            // resolve their schema in parallel instead of serializing
            // through one exclusive lock.
            let stripped = self.cached_schema_for_read(schema_name)?;
            if let Some(schema) = stripped {
                return Ok(Some(schema));
            }
        } else if let Some(schema) = self.get_schema_metadata(schema_name)? {
            return Ok(Some(schema));
        }

        // 2. Try exact match in database (refresh cache if found)
        if let Some(schema) = self
            .db_ops
            .get_schema(schema_name)
            .await
            .map_err(|e| SchemaError::InvalidData(e.to_string()))?
        {
            // Update memory
            let cached_name = schema.name.clone();
            self.load_schema_internal(schema.clone()).await?;
            tracing::info!(
                "Refreshed schema '{}' from database (stale cache)",
                cached_name
            );
            if for_read {
                // Return the cached copy, as the in-memory hot path does.
                // `load_schema_internal` populated runtime_fields on the
                // cache entry. Shared read lock + `&self` clone, same as
                // the in-memory hot path above.
                let stripped = self.cached_schema_for_read(&cached_name)?;
                if let Some(stripped) = stripped {
                    return Ok(Some(stripped));
                }
            }
            return Ok(Some(schema));
        }

        Ok(None)
    }

    /// Install already verified definitions in one private native request.
    pub async fn load_schema_batch(&self, schemas: Vec<Schema>) -> Result<(), SchemaError> {
        let runtime = self.db_ops.native_runtime();
        crate::storage::disk_pack::transaction::request_scope(runtime, async {
            for schema in schemas {
                self.load_schema_internal(schema).await?;
            }
            Ok(())
        })
        .await
    }

    /// Prepare definitions, write durable metadata, then publish the matching cache state.
    pub async fn load_schema_internal(&self, mut schema: Schema) -> Result<(), SchemaError> {
        schema
            .validate_alternate_keys()
            .map_err(SchemaError::InvalidData)?;
        // Preserve runtime metadata supplied by callers; wire schemas need population.
        if schema.runtime_fields.is_empty() {
            schema.populate_runtime_fields()?;
        }
        schema.ensure_field_hashes();

        // One raw read supplies both continuity and the new/refresh decision.
        let existing = self.db_ops.get_schema(&schema.name).await?;
        owned::check_owned_declaration(&schema)?;
        Self::stamp_identity_for_load(&mut schema, existing.as_ref());

        let refresh = existing.is_some();
        if let Some(stored) = existing {
            rules::prepare_refresh(&mut schema, &stored)?;
        }
        let name = schema.name.clone();
        self.db_ops.store_schema(&name, &schema).await?;

        // Read state AFTER the schema write. A missing durable state uses the
        // cached value lazily, so a refresh cannot turn Blocked into Available.
        let state = if refresh {
            match self.db_ops.get_schema_state(&name).await? {
                Some(state) => state,
                None => self.get_schema_state_cached(&name)?,
            }
        } else {
            self.db_ops
                .store_schema_state(&name, &SchemaState::Available)
                .await?;
            SchemaState::Available
        };
        // Native writes stage this publication until the complete disk root.
        self.publish_schema_cache(&name, schema, state)?;
        Ok(())
    }

    /// Recompute the cached identity hash (`get_identity_hash`) so the
    /// firing-snapshot writer can stamp `schema_versions` on every capture
    /// without a per-fire recompute. Recomputed on load because the input may
    /// have come from the schema service (no hash set) or from a
    /// re-registration with changed fields.
    ///
    /// The recompute is GATED. It used to be unconditional, which meant a
    /// binary implementing an older algorithm overwrote a correct hash with
    /// its own answer and left no trace — indistinguishable from an upgrade.
    /// That is exactly how 43 rows on the primary ended up stale. An
    /// identity stamped with a NEWER `algo_version` than this binary
    /// implements is now preserved and reported.
    fn stamp_identity_for_load(schema: &mut Schema, existing_schema: Option<&Schema>) {
        if let IdentityRecompute::RefusedDowngrade { stored, binary } =
            schema.recompute_identity_hash_unless_newer()
        {
            tracing::warn!(
                schema = %schema.name,
                stored_algo_version = stored,
                binary_algo_version = binary,
                identity_hash = %schema.identity_hash.as_deref().unwrap_or("<none>"),
                "refusing to downgrade schema identity: this schema was minted by a \
                 newer identity algorithm than this binary implements. Keeping the \
                 stored identity. This node is older than its own data — upgrade it \
                 rather than re-registering the schema.",
            );
        }

        // Second downgrade guard, for the case the first cannot see: the
        // incoming definition carries no identity (or an unstamped one) but the row
        // already on disk was stamped by a newer binary. Recomputing above is
        // harmless; *persisting* it over the stored row would be the downgrade.
        // Adopt the stored identity instead.
        if let Some(stored) = rules::preserve_stored_identity(schema, existing_schema) {
            tracing::warn!(
                schema = %schema.name,
                stored_algo_version = stored,
                binary_algo_version = schema_types::IDENTITY_HASH_ALGO_VERSION,
                "refusing to overwrite a stored schema identity minted by a newer \
                 identity algorithm than this binary implements. Keeping the stored \
                 identity for this row.",
            );
        }
    }
}
