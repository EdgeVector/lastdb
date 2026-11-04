//! Release schema-owned expiry tracking in bounded, resumable native batches.

use super::catalog_keys::{
    retention_age_latest_prefix, retention_age_prefix, retention_hash_partition_prefix,
};
use super::{SchemaStore, StateDelete, Tracking};
use crate::db_operations::record_store::RangeWalk;
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
            let mut walk = RangeWalk::new(start.as_bytes(), end.as_bytes(), SCHEMA_RELEASE_ROWS);
            while let Some((start, end, asked)) = walk.request() {
                let result = transaction::scope(self.runtime.clone(), async {
                    let scan = self
                        .schema_states_store
                        .inner()
                        .scan_range_yielding(&start, &end, asked, 1)
                        .await?;
                    let count = scan.rows.len();
                    let keys = scan
                        .rows
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
                    Ok::<_, SchemaError>((count, scan.resume))
                })
                .await;
                match result {
                    Ok((count, resume)) => walk.finish_yield(count, resume.as_deref()),
                    Err(error @ SchemaError::ReadResourceLimit { .. }) => {
                        if !walk.read_too_big() {
                            return Err(error);
                        }
                    }
                    Err(error) => return Err(error),
                }
            }
        }
        Ok(())
    }
}
