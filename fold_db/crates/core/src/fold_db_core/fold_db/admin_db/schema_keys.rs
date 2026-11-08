//! Schema-facing key pages read the record keys of one schema in one captured view.

use super::*;
use crate::db_operations::SchemaRecordKeysRequest;
use crate::schema::types::Schema;
use crate::schema::SchemaError;
use lastdb_model::record_space::record_space_uuid;

type KeyPage = (Vec<(String, String)>, Option<String>, bool);

impl FoldDB {
    /// Return a schema's live API keys, with a physical-key cursor.
    pub async fn list_schema_record_keys(
        &self,
        request: SchemaRecordKeysRequest<'_>,
    ) -> Result<SchemaRecordKeysReport, FoldDbError> {
        let scope = self
            .db_ops()
            .read_scope()
            .await
            .map_err(SchemaError::from)?;
        scope
            .run_result(self.list_schema_record_keys_captured(request))
            .await
            .map_err(FoldDbError::Schema)
    }

    async fn list_schema_record_keys_captured(
        &self,
        request: SchemaRecordKeysRequest<'_>,
    ) -> Result<SchemaRecordKeysReport, SchemaError> {
        let SchemaRecordKeysRequest {
            requested,
            schema_id,
            hash_filter,
            ..
        } = request;
        let schema = self
            .schema_manager()
            .resolve_schema_for_read(schema_id)
            .await?
            .ok_or_else(|| SchemaError::NotFound(format!("schema not found: {schema_id}")))?;
        self.schema_manager()
            .require_queryable_schema(&schema.name)?;
        crate::schema::require_logical_layout(&schema.schema_type)?;
        let key_definition = schema.require_record_keys()?.primary.clone();
        let (field_key, (pairs, next_cursor, has_more)) =
            self.schema_key_page(&schema, &request).await?;
        Ok(SchemaRecordKeysReport {
            schema: requested.to_string(),
            schema_id: schema.name.clone(),
            key_definition,
            hash_filter: hash_filter.map(str::to_owned),
            field_key,
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
    ) -> Result<(String, KeyPage), SchemaError> {
        let records = self.db_ops().records();
        let comparison = records.key_comparison_context(schema).await?;
        let page = records
            .list_record_keys(
                schema,
                &comparison,
                request.limit,
                request.cursor,
                request.hash_filter,
            )
            .await?;
        Ok((record_space_uuid(&schema.name), page))
    }
}
