//! Raw value length preflight follows canonical current-key placement.

use super::*;
use std::collections::BTreeMap;

pub(crate) fn raw_value_lengths<F: FnMut(u64) -> StorageResult<()>>(
    source: &LastStore,
    namespace: &str,
    keys: &[String],
    mut admit: F,
) -> StorageResult<Vec<Option<laststore::PointValueSize>>> {
    let mut output: Vec<Option<laststore::PointValueSize>> = vec![None; keys.len()];
    let mut reads: BTreeMap<String, Vec<(usize, String)>> = BTreeMap::new();
    for (slot, key) in keys.iter().enumerate() {
        let collection = if namespace == "main" {
            key_routing::main_collection_for_key(key.as_bytes())
        } else {
            namespace
        };
        reads
            .entry(collection.to_owned())
            .or_default()
            .push((slot, key.clone()));
    }
    for (collection, entries) in reads {
        let ids: Vec<_> = entries.iter().map(|(_, key)| key.clone()).collect();
        let lengths = source
            .point_value_lengths(&collection, &ids, |peak| {
                admit(peak).map_err(|error| laststore::Error::Config(error.to_string()))
            })
            .map_err(LastStoreKvStore::map_error)?;
        for ((slot, _), length) in entries.into_iter().zip(lengths) {
            output[slot] = length;
        }
    }
    Ok(output)
}
