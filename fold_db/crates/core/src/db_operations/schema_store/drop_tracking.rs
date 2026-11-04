//! Release schema-owned expiry tracking in bounded, resumable native batches.

use super::catalog_keys::{
    retention_age_latest_prefix, retention_age_prefix, retention_hash_partition_prefix,
};
use super::{SchemaStore, StateDelete, Tracking};
use crate::schema::SchemaError;
use crate::storage::disk_pack::transaction;
use crate::storage::format::SCHEMA_RELEASE_ROWS;
use lastdb_model::kind_partition::exact_prefix_bounds;

fn ranges(schema: &str) -> [(Tracking, String, String); 3] {
    [
        (Tracking::Age, retention_age_prefix(schema)),
        (Tracking::Latest, retention_age_latest_prefix(schema)),
        (Tracking::Partition, retention_hash_partition_prefix(schema)),
    ]
    .map(|(kind, prefix)| {
        let (start, end) = exact_prefix_bounds(&prefix);
        (kind, start, end)
    })
}

impl SchemaStore {
    /// The logical drop has already blocked the schema and released its data.
    /// An interrupted cleanup keeps that block and resumes from remaining keys.
    pub(super) async fn release_drop_tracking(&self, schema: &str) -> Result<(), SchemaError> {
        for (kind, start, end) in ranges(schema) {
            loop {
                let removed = transaction::scope(self.runtime.clone(), async {
                    let rows = self
                        .schema_states_store
                        .inner()
                        .scan_range_paged(start.as_bytes(), end.as_bytes(), SCHEMA_RELEASE_ROWS)
                        .await?;
                    let count = rows.len();
                    let keys = rows
                        .into_iter()
                        .map(|(key, _)| {
                            String::from_utf8(key)
                                .map_err(|error| SchemaError::InvalidData(error.to_string()))
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    if !keys.is_empty() {
                        self.format
                            .delete_states(vec![StateDelete::Selected { schema, kind, keys }])
                            .await?;
                    }
                    Ok::<_, SchemaError>(count)
                })
                .await?;
                if removed == 0 {
                    break;
                }
            }
        }
        Ok(())
    }

    /// Check inside the final native scope so metadata added between release
    /// batches cannot survive the schema record's removal.
    pub(super) async fn drop_tracking_remains(&self, schema: &str) -> Result<bool, SchemaError> {
        let ranges = ranges(schema);
        let store = self.schema_states_store.inner();
        let pages =
            futures::future::try_join_all(ranges.iter().map(|(_, start, end)| {
                store.scan_range_paged(start.as_bytes(), end.as_bytes(), 1)
            }))
            .await?;
        Ok(pages.iter().any(|page| !page.is_empty()))
    }
}
