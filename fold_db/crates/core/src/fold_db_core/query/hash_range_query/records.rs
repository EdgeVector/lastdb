//! The read of a home: keyed records, not field keys.
//!
//! One key selector reads records by key, or one range of records under one
//! hash, applies the keyed window, and returns every stored field of each
//! record and its one metadata block (decision D7).

mod range;

use super::HashRangeQueryProcessor;
use crate::db_operations::record_store::KeyedRecord;
use crate::fold_db_core::query::{QueryRecords, Record};
use crate::schema::types::field::{HashRangeFilter, KeyWindow};
use crate::schema::types::KeyValuePageOrder;
use crate::schema::{Schema, SchemaError};

impl HashRangeQueryProcessor {
    /// One captured read of keyed records.
    pub(super) async fn query_records(
        &self,
        schema: &Schema,
        filter: &HashRangeFilter,
        window: Option<KeyWindow>,
    ) -> Result<QueryRecords, SchemaError> {
        let rows = self.select_rows(schema, filter, window).await?;
        Ok(to_records(rows))
    }

    /// The live rows that a key-restricted selector names.
    pub(super) async fn count_records(
        &self,
        schema: &Schema,
        filter: &HashRangeFilter,
    ) -> Result<usize, SchemaError> {
        Ok(self.select_rows(schema, filter, None).await?.len())
    }

    /// The rows of one selector in page order, bounded by `window`.
    async fn select_rows(
        &self,
        schema: &Schema,
        filter: &HashRangeFilter,
        window: Option<KeyWindow>,
    ) -> Result<Vec<KeyedRecord>, SchemaError> {
        if let Some((hash, bound)) = range::range_under_hash(schema, filter) {
            return self.range_rows(schema, hash, bound, window).await;
        }
        let mut rows = self.db_ops.records().select_records(schema, filter).await?;
        rows.retain(|(_, record)| record.is_live());
        Ok(match window {
            Some(window) => bound(rows, window),
            None => rows,
        })
    }
}

/// The window over rows in page order.
fn bound(rows: Vec<KeyedRecord>, window: KeyWindow) -> Vec<KeyedRecord> {
    match window {
        KeyWindow::Offset { offset, limit } => rows.into_iter().skip(offset).take(limit).collect(),
        KeyWindow::After { after, limit } => rows
            .into_iter()
            .filter(|(key, _)| key.cmp_page_order(&after).is_gt())
            .take(limit)
            .collect(),
    }
}

/// The reply rows: every stored field of each record and its one metadata
/// block. An empty payload still represents a live record.
fn to_records(rows: Vec<KeyedRecord>) -> QueryRecords {
    let mut out = QueryRecords::with_capacity(rows.len());
    for (key, record) in rows {
        out.insert(key, Record::from_stored(&record));
    }
    out
}
