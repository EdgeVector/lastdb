//! Point reads of one schema in one storage read, answered with the stored records.

use super::QueryExecutor;
use crate::db_operations::record_store::RecordStore;
use crate::schema::types::field::HashRangeFilter;
use crate::schema::types::key_comparison::KeyComparisonContext;
use crate::schema::types::schema::DeclarativeSchemaType;
use crate::schema::types::{KeyValue, Schema, SchemaError};
use lastdb_model::record_value::StoredRecord;
use std::collections::HashMap;
use std::sync::Arc;

/// The canonical key of one point read and its live record, if the key holds one.
/// The record is the stored one, shared with the warm set and not copied.
pub type PointRecord = (KeyValue, Option<Arc<StoredRecord>>);

impl QueryExecutor {
    /// Point reads of one schema: one schema lookup and one storage read for all
    /// of them. Each filter names one key: `HashRangeKey`, or `HashKey` on a Hash
    /// schema. Each answer is that key in canonical form with its live record.
    /// The answer is `None` for a filter that is not a point read or that fails
    /// alone; read that filter with [`QueryExecutor::query_window`], which also
    /// gives its error. An error here means the schema cannot be read this way.
    pub async fn point_records(
        &self,
        schema_name: &str,
        filters: Vec<HashRangeFilter>,
    ) -> Result<Vec<Option<PointRecord>>, SchemaError> {
        self.in_read_scope(self.point_records_current(schema_name, filters))
            .await
    }

    async fn point_records_current(
        &self,
        schema_name: &str,
        filters: Vec<HashRangeFilter>,
    ) -> Result<Vec<Option<PointRecord>>, SchemaError> {
        let schema = self
            .schema_manager
            .resolve_schema(schema_name)
            .await?
            .ok_or_else(|| SchemaError::InvalidData(format!("Schema '{schema_name}' not found")))?;
        self.schema_manager.require_queryable_schema(&schema.name)?;
        crate::schema::require_logical_layout(&schema.schema_type)?;
        let records = self.db_ops.records();
        let cmp = records.key_comparison_context(&schema)?;
        cmp.verify_active()?;
        // Two reads of one key share one storage address and one stored record.
        let (mut slot_of, mut addresses) = (HashMap::new(), Vec::new());
        let mut points = Vec::with_capacity(filters.len());
        for filter in filters {
            let point = point_key(records, &schema, &cmp, filter).map(|(key, address)| {
                let at = *slot_of
                    .entry(address)
                    .or_insert_with_key(|address: &String| {
                        addresses.push(address.clone());
                        addresses.len() - 1
                    });
                (key, at)
            });
            points.push(point);
        }
        let found = records.read_records(&addresses).await?;
        Ok(points
            .into_iter()
            .map(|point| {
                let (key, at) = point?;
                let record = found.get(at).cloned().flatten().filter(|r| r.is_live());
                // The per-read path refuses a stored range outside the range type.
                if record.is_some() {
                    cmp.validate_range(key.range.as_deref().unwrap_or(""))
                        .ok()?;
                }
                Some((key, record))
            })
            .collect())
    }
}

/// The canonical key and storage address of a point read, as `select_records`
/// forms them. `None` for a filter that is not one key, or that fails.
fn point_key(
    records: &RecordStore,
    schema: &Schema,
    cmp: &KeyComparisonContext,
    filter: HashRangeFilter,
) -> Option<(KeyValue, String)> {
    let ranged = schema.schema_type == DeclarativeSchemaType::HashRange;
    let key = match cmp.canonical_filter(Some(filter)).ok()?? {
        HashRangeFilter::HashRangeKey { hash, range } => {
            KeyValue::new(Some(hash), ranged.then_some(range))
        }
        HashRangeFilter::HashKey(hash) if !ranged => KeyValue::new(Some(hash), None),
        _ => return None,
    };
    let address = records.record_key(schema, &key).ok()?;
    Some((key, address))
}
