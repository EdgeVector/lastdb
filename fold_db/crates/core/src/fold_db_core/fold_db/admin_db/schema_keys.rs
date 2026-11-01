//! Schema-facing key pages read the record keys of one schema in one captured view.

use super::*;
use crate::db_operations::SchemaRecordKeysRequest;
use crate::schema::types::Schema;
use crate::schema::SchemaError;
use lastdb_model::record_space::record_space_uuid;

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
        let (molecule, (pairs, next_cursor, has_more)) = self
            .schema_key_page(&schema, &request, storage_prefix.as_deref())
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

    /// The listed record space and one page of its record keys. The cursor
    /// is a record key.
    pub(in crate::fold_db_core::fold_db) async fn schema_key_page(
        &self,
        schema: &Schema,
        request: &SchemaRecordKeysRequest<'_>,
        storage_prefix: Option<&str>,
    ) -> Result<(String, KeyPage), SchemaError> {
        let atoms = self.db_ops().atoms();
        let comparison = atoms.key_comparison_context(schema).await?;
        let page = atoms
            .list_record_keys(
                schema,
                &comparison,
                request.limit,
                request.cursor,
                request.hash_filter,
                storage_prefix,
            )
            .await?;
        Ok((record_space_uuid(&schema.name), page))
    }
}
