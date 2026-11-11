//! Owner-facing DB delete ledger, retention backfill, schema key pages, and storage reports.

use super::FoldDB;
use crate::db_operations::{
    RecordDeleteLedgerEntry, SchemaRecordKey, SchemaRecordKeysReport, SchemaRecordKeysRequest,
    SchemaStorageReport,
};
use crate::error::FoldDbError;
use crate::schema::types::{schema::DeclarativeSchemaType, KeyValue, Schema, SchemaError};
use lastdb_model::record_space::record_space_uuid;
use serde_json::{json, Value};
use std::collections::HashMap;

type KeyPage = (Vec<(String, String)>, Option<String>, bool);

impl FoldDB {
    pub async fn list_record_delete_ledger(
        &self,
        limit: usize,
    ) -> Result<Vec<RecordDeleteLedgerEntry>, FoldDbError> {
        self.db_ops()
            .records()
            .list_record_delete_ledger(limit)
            .await
            .map_err(|e| FoldDbError::Database(format!("list_record_delete_ledger: {e}")))
    }

    pub async fn backfill_schema_retention_page(
        &self,
        name: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Value, FoldDbError> {
        let schema = self
            .schema_manager()
            .get_schema_metadata(name)?
            .ok_or_else(|| SchemaError::NotFound(format!("schema not found: {name}")))?;
        let range_keyed = match schema.schema_type {
            DeclarativeSchemaType::Hash => false,
            DeclarativeSchemaType::HashRange => true,
            _ => {
                return Err(SchemaError::InvalidData(
                    "retention backfill supports Hash and HashRange schemas".into(),
                )
                .into())
            }
        };
        let bound = crate::fold_db_core::purge::budget::max_erase_keys(
            false,
            crate::fold_db_core::purge::budget::EraseShape::of(&schema),
            4096,
        );
        if bound == 0 {
            return Err(SchemaError::InvalidData(
                "schema is too wide for one bounded retention page".into(),
            )
            .into());
        }
        let page = self
            .list_schema_record_keys(SchemaRecordKeysRequest {
                requested: name,
                schema_id: name,
                limit: limit.clamp(1, 64).min(bound),
                cursor,
                hash_filter: None,
            })
            .await?;
        if page.schema_id != schema.name {
            return Err(SchemaError::InvalidData(
                "retention backfill refuses a redirected schema; use its current name".into(),
            )
            .into());
        }
        let keys: Vec<_> = page
            .keys
            .iter()
            .map(|key| {
                KeyValue::new(
                    Some(key.hash.clone()),
                    range_keyed.then(|| key.range.clone()),
                )
            })
            .collect();
        let count = self
            .db_ops()
            .records()
            .in_background_scope(async {
                let clocks = if range_keyed {
                    keys.into_iter().map(|key| (key, 0)).collect()
                } else {
                    crate::fold_db_core::purge::retention_tip_clocks(
                        self.db_ops(),
                        &self.schema_manager(),
                        &schema.name,
                        &keys,
                    )
                    .await?
                };
                self.db_ops()
                    .schemas()
                    .backfill_retention_rows(name, clocks, range_keyed)
                    .await
            })
            .await?;
        Ok(json!({
            "schema": name,
            "scope": "personal",
            "tracked": count,
            "next_cursor": page.next_cursor,
            "has_more": page.has_more,
            "complete": !page.has_more,
            "existing_records_may_expire": true
        }))
    }

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
            .resolve_schema(schema_id)
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

    async fn schema_key_page(
        &self,
        schema: &Schema,
        request: &SchemaRecordKeysRequest<'_>,
    ) -> Result<(String, KeyPage), SchemaError> {
        let records = self.db_ops().records();
        let comparison = records.key_comparison_context(schema)?;
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

    pub async fn db_schema_storage(&self) -> Result<SchemaStorageReport, FoldDbError> {
        let schemas = self
            .db_ops()
            .get_all_schemas()
            .await
            .map_err(|e| FoldDbError::Database(format!("list schemas: {e}")))?;
        let names: Vec<String> = schemas.values().map(|schema| schema.name.clone()).collect();
        let mut breakdown = self
            .db_ops()
            .records()
            .record_storage_breakdown(&names)
            .await
            .map_err(|e| FoldDbError::Database(format!("schema record storage: {e}")))?;
        let mut display = HashMap::new();
        for (key, schema) in &schemas {
            let label = schema
                .descriptive_name
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .unwrap_or(schema.name.trim());
            if label.is_empty() {
                continue;
            }
            display.insert(key.clone(), label.to_string());
            display.insert(schema.name.clone(), label.to_string());
            if let Some(hash) = schema.identity_hash.as_ref() {
                display.insert(hash.clone(), label.to_string());
            }
        }
        breakdown.apply_display_names(&display);
        Ok(SchemaStorageReport::from_breakdown(breakdown))
    }
}
