//! Bounded observed-partition reads, including progress through deleted keys.

use super::catalog_keys::{retention_hash_partition_prefix, SchemaRetentionHashPartition};
use super::SchemaStore;
use crate::db_operations::record_store::RangeWalk;
use crate::schema::SchemaError;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use lastdb_model::kind_partition::exact_prefix_bounds;

impl SchemaStore {
    /// A cursor is bound to one retention anchor's registry. Empty pages can
    /// carry a continuation when only deleted keys used the read budget.
    pub async fn schema_retention_hash_partitions_page(
        &self,
        schema: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<(Vec<String>, Option<String>), SchemaError> {
        let anchor = self.retention_anchor(schema).await?;
        let (first, end) = exact_prefix_bounds(&retention_hash_partition_prefix(&anchor));
        let start = match cursor {
            Some(cursor) => URL_SAFE_NO_PAD.decode(cursor).map_err(invalid)?,
            None => first.as_bytes().to_vec(),
        };
        if start.as_slice() < first.as_bytes() || start.as_slice() >= end.as_bytes() {
            return Err(invalid(
                "partition cursor differs from its retention anchor",
            ));
        }
        let mut walk = RangeWalk::new(&start, end.as_bytes(), limit.clamp(1, 256));
        while let Some((start, end, asked)) = walk.request() {
            let result = self
                .schema_states_store
                .inner()
                .scan_range_yielding(&start, &end, asked, 0)
                .await;
            match result.map_err(SchemaError::from) {
                Ok(scan) => {
                    walk.finish_yield(scan.rows.len(), scan.resume.as_deref());
                    let hashes = scan
                        .rows
                        .into_iter()
                        .map(|(_, bytes)| {
                            serde_json::from_slice::<SchemaRetentionHashPartition>(&bytes)
                                .map(|entry| entry.hash)
                                .map_err(invalid)
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    let cursor = walk
                        .request()
                        .map(|(start, _, _)| URL_SAFE_NO_PAD.encode(start));
                    return Ok((hashes, cursor));
                }
                Err(error @ SchemaError::ReadResourceLimit { .. }) => {
                    if !walk.read_too_big() {
                        return Err(error);
                    }
                }
                Err(error) => return Err(error),
            }
        }
        Ok((Vec::new(), None))
    }

    /// The TTL worker needs complete membership and follows every page.
    pub async fn schema_retention_hash_partitions(
        &self,
        schema: &str,
    ) -> Result<Vec<String>, SchemaError> {
        let mut hashes = Vec::new();
        let mut cursor = None;
        loop {
            let (page, next) = self
                .schema_retention_hash_partitions_page(schema, cursor.as_deref(), 256)
                .await?;
            hashes.extend(page);
            cursor = next;
            if cursor.is_none() {
                break;
            }
        }
        hashes.sort();
        hashes.dedup();
        Ok(hashes)
    }
}

fn invalid(error: impl std::fmt::Display) -> SchemaError {
    SchemaError::InvalidData(format!("retention partition page: {error}"))
}
