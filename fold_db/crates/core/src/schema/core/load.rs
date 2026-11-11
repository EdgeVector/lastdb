use super::SchemaCore;

pub(super) mod owned;
mod rules;
use crate::schema::types::{Schema, SchemaError};
use crate::schema::SchemaState;
use schema_types::IdentityRecompute;

impl SchemaCore {
    /// Fetches a schema by name, checking both in-memory cache and database.
    ///
    /// Note: This is STRICTLY case-sensitive.
    pub async fn resolve_schema(&self, schema_name: &str) -> Result<Option<Schema>, SchemaError> {
        if let Some(schema) = self.get_schema_metadata(schema_name)? {
            return Ok(Some(schema));
        }

        if let Some(schema) = self
            .db_ops
            .get_schema(schema_name)
            .await
            .map_err(|e| SchemaError::InvalidData(e.to_string()))?
        {
            let cached_name = schema.name.clone();
            self.load_schema_internal(schema.clone()).await?;
            tracing::info!(
                "Refreshed schema '{}' from database (stale cache)",
                cached_name
            );
            if let Some(cached) = self.get_schema_metadata(&cached_name)? {
                return Ok(Some(cached));
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
        schema.validate()?;
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
