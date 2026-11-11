//! One-hash selection of typed key-to-primary entries. No field values live here.

use super::*;
use crate::db_operations::record_store::RecordStore;
use crate::schema::types::field::HashRangeFilter;
use crate::storage::format::records::alternate_bounds;

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
