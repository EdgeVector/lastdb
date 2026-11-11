//! Explicit named alternate addresses pointing to complete primary records.

use crate::db_operations::record_store::RecordStore;
use crate::schema::types::field::HashRangeFilter;
use crate::schema::types::{KeyValue, Schema};
use crate::schema::SchemaError;
use crate::storage::format::records::{alternate_bounds, AlternateAddress};

pub use crate::storage::format::records::{AlternateEntry, MAX_ALTERNATE_ENTRIES};
pub use lastdb_model::field_key_id::alternate_field_key_id as field_key;

pub(crate) fn storage_key(
    records: &RecordStore,
    schema: &Schema,
    name: &str,
    key: &KeyValue,
    primary: &KeyValue,
) -> Result<String, SchemaError> {
    storage_address(records, schema, name, key, primary).map(AlternateAddress::into_string)
}

pub(crate) fn storage_address(
    records: &RecordStore,
    schema: &Schema,
    name: &str,
    key: &KeyValue,
    primary: &KeyValue,
) -> Result<AlternateAddress, SchemaError> {
    let id = field_key(schema, name);
    let target = records.record_address(schema, primary)?;
    AlternateAddress::new(&records.alternate_codec(schema, name)?, &id, key, &target)
}

pub(crate) fn invalid(message: &str) -> SchemaError {
    SchemaError::InvalidData(message.into())
}

impl RecordStore {
    pub(crate) async fn select_alternate_entries(
        &self,
        schema: &Schema,
        name: &str,
        filter: &HashRangeFilter,
        after: Option<&AlternateEntry>,
        limit: usize,
    ) -> Result<Vec<AlternateEntry>, SchemaError> {
        if limit > MAX_ALTERNATE_ENTRIES {
            return Err(invalid("alternate page exceeds 4096 entries"));
        }
        let field_key = field_key(schema, name);
        let codec = self.alternate_codec(schema, name)?;
        let (mut start, end) = alternate_bounds(&codec, &field_key, filter)?;
        if let Some(after) = after {
            let cursor = storage_key(self, schema, name, &after.matched, &after.primary)?;
            if cursor < start || cursor >= end {
                return Err(invalid(
                    "alternate cursor is outside the requested key range",
                ));
            }
            start = format!("{cursor}\0");
        }
        let rows = self
            .reader()
            .scan_range_paged(start.as_bytes(), end.as_bytes(), limit)
            .await?;
        let mut entries = Vec::with_capacity(rows.len());
        for (key, value) in rows {
            let entry: AlternateEntry =
                serde_json::from_slice(&value).map_err(|_| invalid("invalid alternate entry"))?;
            if storage_key(self, schema, name, &entry.matched, &entry.primary)?.as_bytes() != key {
                return Err(invalid(
                    "alternate entry coordinates differ from their storage key",
                ));
            }
            entries.push(entry);
        }
        Ok(entries)
    }
}
