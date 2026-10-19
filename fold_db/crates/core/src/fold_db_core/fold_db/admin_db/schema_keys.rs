//! Schema-facing key pages use one captured schema, key, and body boundary.

use super::*;
use crate::schema::SchemaError;
use crate::storage::disk_pack::logical_paths::field;
use std::sync::Arc;

impl FoldDB {
    /// Return API keys for this schema's live records, with a physical-key cursor.
    pub async fn list_schema_record_keys(
        &self,
        requested: &str,
        schema_id: &str,
        limit: usize,
        cursor: Option<&str>,
        hash_filter: Option<&str>,
    ) -> Result<SchemaRecordKeysReport, FoldDbError> {
        let scope = self
            .db_ops()
            .read_scope()
            .await
            .map_err(SchemaError::from)?;
        scope
            .run_result(self.list_schema_record_keys_captured(
                requested,
                schema_id,
                limit,
                cursor,
                hash_filter,
            ))
            .await
            .map_err(FoldDbError::Schema)
    }

    async fn list_schema_record_keys_captured(
        &self,
        requested: &str,
        schema_id: &str,
        limit: usize,
        cursor: Option<&str>,
        hash_filter: Option<&str>,
    ) -> Result<SchemaRecordKeysReport, SchemaError> {
        let mut schema = self
            .schema_manager()
            .get_schema_following_supersession_for_read(schema_id)
            .await?
            .ok_or_else(|| SchemaError::NotFound(format!("schema not found: {schema_id}")))?;
        crate::schema::require_logical_layout(&schema.schema_type)?;
        let hash_field = schema
            .key
            .as_ref()
            .and_then(|key| key.hash_field.clone())
            .filter(|name| !name.is_empty());
        let range_field = schema
            .key
            .as_ref()
            .and_then(|key| key.range_field.clone())
            .filter(|name| !name.is_empty());
        let key_field = hash_field.as_ref().cloned().ok_or_else(|| {
            SchemaError::InvalidField(format!("schema '{requested}' has no hash_field to list"))
        })?;
        let molecule = schema
            .field_molecule_uuids
            .as_ref()
            .and_then(|molecules| molecules.get(&key_field).cloned())
            .or_else(|| {
                schema
                    .runtime_fields
                    .get_mut(&key_field)
                    .and_then(|field| field.inner.molecule_uuid().cloned())
            })
            .unwrap_or_else(|| crate::atom::deterministic_molecule_uuid(&schema.name, &key_field));
        let named_layout = matches!(
            schema.schema_type,
            crate::schema::types::schema::DeclarativeSchemaType::HashRange
        );
        if let Some(runtime) = self.db_ops().namespaced_store().disk_pack_runtime() {
            runtime
                .preload_bound_field(Arc::clone(self.db_ops()), &schema, &key_field)
                .await?;
        }
        let binding = field::FieldScope::new(&schema, &key_field)?;
        let (pairs, next_cursor, has_more) = field::query_scope(binding.run(
            self.db_ops().atoms().list_schema_record_keys_filtered(
                &molecule,
                limit,
                cursor,
                hash_filter,
                named_layout,
            ),
        ))
        .await?;
        Ok(SchemaRecordKeysReport {
            schema: requested.to_string(),
            schema_id: schema.name.clone(),
            key_field,
            hash_field,
            range_field,
            hash_filter: hash_filter.map(str::to_owned),
            molecule,
            keys: pairs
                .into_iter()
                .map(|(hash, range)| SchemaRecordKey { hash, range })
                .collect(),
            next_cursor,
            has_more,
            truncated: has_more,
        })
    }
}
