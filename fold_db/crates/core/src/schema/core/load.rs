use super::SchemaCore;
use crate::schema::types::{Schema, SchemaError};
use crate::schema::SchemaState;
use schema_types::IdentityRecompute;

pub(super) fn admit_stored_schema(schema: &Schema) -> Result<(), SchemaError> {
    if schema.record_keys.is_none() {
        return Err(SchemaError::InvalidData(
            "stored schema has no explicit record_keys".into(),
        ));
    }
    schema
        .validate_owned_schema()
        .map_err(SchemaError::InvalidData)
}

fn check_owned_declaration(schema: &Schema) -> Result<(), SchemaError> {
    if schema.record_keys.is_none() {
        return Err(SchemaError::InvalidData(
            "this home requires explicit record_keys; field-derived keys are unsupported".into(),
        ));
    }
    schema
        .validate_owned_schema()
        .map_err(SchemaError::InvalidData)
}

fn prepare_refresh(schema: &mut Schema, stored: &Schema) -> Result<(), SchemaError> {
    if schema.record_keys != stored.record_keys || schema.alternate_keys != stored.alternate_keys {
        return Err(SchemaError::InvalidData(
            "explicit primary/alternate key declarations are immutable; declare a new schema and copy explicitly"
                .into(),
        ));
    }
    schema.validate()?;
    Ok(())
}

fn preserve_stored_identity(schema: &mut Schema, stored: Option<&Schema>) -> Option<u32> {
    let stored = stored?;
    let version = schema_types::refuses_identity_downgrade(
        stored.identity_hash.as_deref(),
        stored.identity_hash_algo_version,
    )?;
    schema.identity_hash = stored.identity_hash.clone();
    schema.identity_hash_algo_version = stored.identity_hash_algo_version;
    Some(version)
}

impl SchemaCore {
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
            tracing::info!("Refreshed schema '{cached_name}' from database (stale cache)");
            if let Some(cached) = self.get_schema_metadata(&cached_name)? {
                return Ok(Some(cached));
            }
            return Ok(Some(schema));
        }

        Ok(None)
    }

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

    pub async fn load_schema_internal(&self, mut schema: Schema) -> Result<(), SchemaError> {
        schema
            .validate_alternate_keys()
            .map_err(SchemaError::InvalidData)?;
        schema.validate()?;
        schema.ensure_field_hashes();

        let existing = self.db_ops.get_schema(&schema.name).await?;
        check_owned_declaration(&schema)?;
        Self::stamp_identity_for_load(&mut schema, existing.as_ref());

        let refresh = existing.is_some();
        if let Some(stored) = existing {
            prepare_refresh(&mut schema, &stored)?;
        }
        let name = schema.name.clone();
        self.db_ops.store_schema(&name, &schema).await?;

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
                "refusing to downgrade schema identity minted by a newer algorithm",
            );
        }

        if let Some(stored) = preserve_stored_identity(schema, existing_schema) {
            tracing::warn!(
                schema = %schema.name,
                stored_algo_version = stored,
                binary_algo_version = schema_types::IDENTITY_HASH_ALGO_VERSION,
                "refusing to overwrite a stored schema identity minted by a newer algorithm",
            );
        }
    }
}
