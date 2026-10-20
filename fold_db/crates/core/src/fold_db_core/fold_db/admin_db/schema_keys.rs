//! Schema-facing key pages use one captured schema, key, and body boundary.

use super::*;
use crate::db_operations::SchemaRecordKeysRequest;
use crate::schema::types::key_value::KeyValue;
use crate::schema::types::Schema;
use crate::schema::SchemaError;
use crate::storage::disk_pack::logical_paths::field;
use futures::FutureExt;
use std::sync::Arc;

type KeyPage = (Vec<(String, String)>, Option<String>, bool);

impl FoldDB {
    /// Return a personal schema's live API keys, with a physical-key cursor.
    pub async fn list_schema_record_keys(
        &self,
        requested: &str,
        schema_id: &str,
        limit: usize,
        cursor: Option<&str>,
        hash_filter: Option<&str>,
    ) -> Result<SchemaRecordKeysReport, FoldDbError> {
        self.list_schema_record_keys_read(
            SchemaRecordKeysRequest {
                requested,
                schema_id,
                limit,
                cursor,
                hash_filter,
            },
            None,
        )
        .await
    }

    /// Resolve the caller's database membership within the same captured read.
    pub async fn list_schema_record_keys_with_access(
        &self,
        request: SchemaRecordKeysRequest<'_>,
        access: &AccessContext,
    ) -> Result<SchemaRecordKeysReport, FoldDbError> {
        self.list_schema_record_keys_read(request, Some(access))
            .await
    }

    async fn list_schema_record_keys_read(
        &self,
        request: SchemaRecordKeysRequest<'_>,
        access: Option<&AccessContext>,
    ) -> Result<SchemaRecordKeysReport, FoldDbError> {
        let scope = self
            .db_ops()
            .read_scope()
            .await
            .map_err(SchemaError::from)?;
        scope
            .run_result(self.list_schema_record_keys_captured(request, access))
            .await
            .map_err(FoldDbError::Schema)
    }

    async fn list_schema_record_keys_captured(
        &self,
        request: SchemaRecordKeysRequest<'_>,
        access: Option<&AccessContext>,
    ) -> Result<SchemaRecordKeysReport, SchemaError> {
        let SchemaRecordKeysRequest {
            requested,
            schema_id,
            hash_filter,
            ..
        } = request;
        let storage_prefix = match access {
            Some(access) => {
                self.db_ops()
                    .db_catalog()
                    .resolve_storage_prefix(
                        access.db_locator.as_deref(),
                        schema_id,
                        access.storage_prefix.as_deref(),
                    )
                    .await?
            }
            None => None,
        };
        let mut schema = self
            .schema_manager()
            .get_schema_following_supersession_for_read(schema_id)
            .await?
            .ok_or_else(|| SchemaError::NotFound(format!("schema not found: {schema_id}")))?;
        self.schema_manager()
            .require_queryable_schema(&schema.name)?;
        crate::schema::require_logical_layout(&schema.schema_type)?;
        for field in schema.runtime_fields.values_mut() {
            field
                .common_mut()
                .set_storage_prefix(storage_prefix.clone());
        }
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
        let (pairs, next_cursor, has_more) = self
            .list_schema_key_page(
                &schema,
                &key_field,
                &molecule,
                &request,
                storage_prefix.as_deref(),
            )
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

    /// Keep the field binding, membership proof, and physical refill in one read.
    async fn list_schema_key_page(
        &self,
        schema: &Schema,
        key_field: &str,
        molecule: &str,
        request: &SchemaRecordKeysRequest<'_>,
        storage_prefix: Option<&str>,
    ) -> Result<KeyPage, SchemaError> {
        let named_layout = matches!(
            schema.schema_type,
            crate::schema::types::schema::DeclarativeSchemaType::HashRange
        );
        if let Some(runtime) = self.db_ops().namespaced_store().disk_pack_runtime() {
            runtime
                .preload_bound_field(Arc::clone(self.db_ops()), schema, key_field)
                .await?;
        }
        let binding = field::FieldScope::new(schema, key_field)?;
        let check = |keys: Vec<KeyValue>| {
            async move {
                crate::db_operations::logical_membership::logical_key_membership(
                    self.db_ops(),
                    schema,
                    &keys,
                    storage_prefix,
                )
                .await
            }
            .boxed()
        };
        field::query_scope(
            binding.run(self.db_ops().atoms().list_schema_record_keys_filtered(
                molecule,
                request.limit,
                request.cursor,
                request.hash_filter,
                crate::db_operations::atom_store::SchemaKeyMembership {
                    named_layout,
                    check: &check,
                },
                storage_prefix,
            )),
        )
        .await
    }
}
