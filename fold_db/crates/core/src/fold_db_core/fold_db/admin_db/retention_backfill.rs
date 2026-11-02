//! Explicit retention backfill uses bounded live-key pages, then exact record reads.

use super::*;
use crate::schema::types::{schema::DeclarativeSchemaType, KeyValue};
use crate::schema::SchemaError;
use serde_json::{json, Value};

impl FoldDB {
    /// Backfill one personal-schema page. The caller confirms that existing
    /// records may expire. Each page is durable before its cursor is returned.
    /// New writes must still use their ordinary retention tracking path.
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
            .list_schema_record_keys(name, name, limit.clamp(1, 64).min(bound), cursor, None)
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
        Ok(
            json!({ "schema": name, "scope": "personal", "tracked": count,
            "next_cursor": page.next_cursor, "has_more": page.has_more,
            "complete": !page.has_more, "existing_records_may_expire": true }),
        )
    }
}
