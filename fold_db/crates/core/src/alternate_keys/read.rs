//! One-hash selection of typed key-to-primary entries. No payload tips live here.
//! An org database keeps its entries under its own storage scope prefix.

use super::*;
use crate::db_operations::record_store::RecordStore;
use crate::schema::types::field::HashRangeFilter;

impl RecordStore {
    pub(crate) async fn select_alternate_entries(
        &self,
        schema: &Schema,
        name: &str,
        filter: &HashRangeFilter,
        after: Option<&KeyValue>,
        limit: usize,
        scope: Option<&str>,
    ) -> Result<Vec<AlternateEntry>, SchemaError> {
        if limit > MAX_ALTERNATE_ENTRIES {
            return Err(invalid("alternate page exceeds 4096 entries"));
        }
        let (mut start, end) = alternate_bounds(self, schema, name, filter)?;
        if let Some(after) = after {
            let cursor = storage_key(self, schema, name, after)?;
            if cursor < start || cursor >= end {
                return Err(invalid(
                    "alternate cursor is outside the requested key range",
                ));
            }
            start = format!("{cursor}\0");
        }
        let (start, end) = (
            build_storage_key(scope, &start),
            build_storage_key(scope, &end),
        );
        let rows = self
            .raw()
            .inner()
            .scan_range_paged(start.as_bytes(), end.as_bytes(), limit)
            .await?;
        let mut entries = Vec::with_capacity(rows.len());
        for (key, value) in rows {
            let entry: AlternateEntry =
                serde_json::from_slice(&value).map_err(|_| invalid("invalid alternate entry"))?;
            if build_storage_key(scope, &storage_key(self, schema, name, &entry.matched)?)
                .as_bytes()
                != key
            {
                return Err(invalid(
                    "alternate entry coordinates differ from their storage key",
                ));
            }
            entries.push(entry);
        }
        Ok(entries)
    }
}

fn alternate_bounds(
    records: &RecordStore,
    schema: &Schema,
    name: &str,
    filter: &HashRangeFilter,
) -> Result<(String, String), SchemaError> {
    let field_key = field_key(schema, name);
    let codec = records.key_codec_for_field_key(&field_key)?;
    let encode = |hash: &str, range: Option<&str>| {
        storage_key(
            records,
            schema,
            name,
            &KeyValue::new(Some(hash.to_string()), range.map(str::to_owned)),
        )
    };
    match filter {
        HashRangeFilter::HashRangeKey { hash, range } => {
            let key = encode(hash, Some(range))?;
            Ok((key.clone(), format!("{key}\0")))
        }
        HashRangeFilter::HashKey(hash) => {
            let prefix = codec
                .api_hash_range_scan_prefix_for_hash(&field_key, hash)
                .map_err(|_| invalid("invalid alternate hash"))?;
            let prefix = format!(
                "{PREFIX}{}",
                prefix.strip_prefix("mk:").expect("typed prefix")
            );
            Ok(lastdb_model::kind_partition::exact_prefix_bounds(&prefix))
        }
        HashRangeFilter::HashRangePrefix { hash, prefix } => {
            codec
                .require_text_range_prefix()
                .map_err(|_| invalid("alternate range prefix requires a text field"))?;
            let prefix = encode(hash, Some(prefix))?;
            Ok(lastdb_model::kind_partition::exact_prefix_bounds(&prefix))
        }
        HashRangeFilter::HashRangeRange { hash, start, end } => {
            Ok((encode(hash, Some(start))?, encode(hash, Some(end))?))
        }
        _ => Err(invalid(
            "alternate reads require an exact key or a range under one hash",
        )),
    }
}
