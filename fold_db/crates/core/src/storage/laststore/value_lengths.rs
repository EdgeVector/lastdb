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
        let collections = if namespace == "main" {
            key_routing::main_collections_for_key(key.as_bytes())
                .into_iter()
                .map(str::to_owned)
                .collect()
        } else {
            vec![namespace.to_owned()]
        };
        for collection in collections {
            reads
                .entry(collection)
                .or_default()
                .push((slot, key.clone()));
        }
    }
    for (collection, entries) in reads {
        let ids: Vec<_> = entries.iter().map(|(_, key)| key.clone()).collect();
        let lengths = source
            .point_value_lengths(&collection, &ids, |peak| {
                admit(peak).map_err(|error| laststore::Error::Config(error.to_string()))
            })
            .map_err(LastStoreKvStore::map_error)?;
        for ((slot, _), length) in entries.into_iter().zip(lengths) {
            if let Some(length) = length {
                output[slot] = Some(match output[slot] {
                    None => length,
                    Some(previous) => laststore::PointValueSize {
                        bytes: previous.bytes.max(length.bytes),
                        may_inflate: previous.may_inflate || length.may_inflate,
                        open_peak: previous.open_peak.max(length.open_peak),
                    },
                });
            }
        }
    }
    Ok(output)
}
