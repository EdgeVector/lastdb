//! Erase sizing, key rendering, and retention ages.

use std::sync::Arc;

use crate::db_operations::DbOperations;
use crate::schema::types::key_value::KeyValue;
use crate::schema::{SchemaCore, SchemaError};

/// Human-readable rendering of a KeyValue for log/error messages.
pub(crate) fn describe_key(kv: &KeyValue) -> String {
    match (&kv.hash, &kv.range) {
        (Some(h), Some(r)) => format!("hash='{h}' range='{r}'"),
        (Some(h), None) => format!("hash='{h}'"),
        (None, Some(r)) => format!("range='{r}'"),
        (None, None) => "<single>".to_string(),
    }
}

/// Write time of each live record in unix seconds, via one batch read by key.
pub(crate) async fn retention_tip_clocks(
    db_ops: &Arc<DbOperations>,
    schemas: &Arc<SchemaCore>,
    name: &str,
    keys: &[KeyValue],
) -> Result<Vec<(KeyValue, u64)>, SchemaError> {
    let schema = schemas.require_schema_metadata(name)?;
    let record_store = db_ops.records();
    let record_keys = keys
        .iter()
        .map(|key| record_store.record_key(&schema, key))
        .collect::<Result<Vec<_>, _>>()?;
    let records = record_store.read_records(&record_keys).await?;
    Ok(keys
        .iter()
        .zip(records)
        .filter_map(|(key, record)| {
            let record = record.filter(|r| r.is_live())?;
            Some((key.clone(), record.written_at()? / 1_000_000_000))
        })
        .collect())
}

pub mod budget {
    use crate::alternate_keys::MAX_ALTERNATE_ENTRIES;
    use crate::schema::types::Schema;

    const ERASE_CHUNK_KEYS: usize = 64;
    const CHUNK_FIXED_OPERATIONS: usize = 8;
    const WRITE_REQUEST_READ_KEYS: usize = 40_000;
    const ALTERNATE_READS_PER_ENTRY: usize = 4;
    const APPLIED_DELETE_OPERATIONS_PER_KEY: usize = 2;
    const APPLIED_DELETE_READS_PER_KEY: usize = 2;

    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    pub struct EraseShape {
        pub fields: usize,
        pub alternates: usize,
    }

    impl EraseShape {
        #[must_use]
        pub fn of(schema: &Schema) -> Self {
            Self {
                fields: schema
                    .fields
                    .as_ref()
                    .map_or(1, |fields| fields.len().max(1)),
                alternates: schema
                    .record_keys
                    .as_ref()
                    .map_or(0, |keys| keys.alternates.len()),
            }
        }
    }

    fn operations_per_field(delete: bool) -> usize {
        if delete {
            5
        } else {
            4
        }
    }

    fn reads_per_field(delete: bool) -> usize {
        if delete {
            13
        } else {
            12
        }
    }

    fn fits(delete: bool, keys: usize, shape: EraseShape, limit: usize) -> bool {
        let chunks = keys.div_ceil(ERASE_CHUNK_KEYS);
        let chunk_ops = chunks.saturating_mul(shape.fields.saturating_add(CHUNK_FIXED_OPERATIONS));
        let ops = keys
            .saturating_mul(shape.fields)
            .saturating_mul(operations_per_field(delete))
            .saturating_add(chunk_ops)
            .saturating_add(keys.saturating_mul(shape.alternates))
            .saturating_add(keys.saturating_mul(APPLIED_DELETE_OPERATIONS_PER_KEY));
        let reads = keys
            .saturating_mul(shape.fields)
            .saturating_mul(reads_per_field(delete))
            .saturating_add(chunk_ops)
            .saturating_add(
                keys.saturating_mul(shape.alternates)
                    .saturating_mul(ALTERNATE_READS_PER_ENTRY),
            )
            .saturating_add(keys.saturating_mul(APPLIED_DELETE_READS_PER_KEY));
        ops <= limit
            && reads <= WRITE_REQUEST_READ_KEYS
            && keys.saturating_mul(shape.alternates) <= MAX_ALTERNATE_ENTRIES
    }

    #[must_use]
    pub fn max_erase_keys(delete: bool, shape: EraseShape, limit: usize) -> usize {
        let width = shape.fields.max(1);
        let mut keys = (limit / width.saturating_mul(operations_per_field(delete)))
            .min(WRITE_REQUEST_READ_KEYS / width.saturating_mul(reads_per_field(delete)))
            .min(MAX_ALTERNATE_ENTRIES / shape.alternates.max(1));
        while keys > 0 && !fits(delete, keys, shape, limit) {
            keys -= 1;
        }
        keys
    }
}
