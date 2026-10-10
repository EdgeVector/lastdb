//! The keep-small shards of the dropped names.
//!
//! A receiptless name has no molecule list in a receipt. Its meter shard may
//! still name molecules. Those molecules are candidates only. Window 1 reports
//! them and keeps them.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use fold_db::db_operations::keep_small::{keep_small_schema_shard_key, KeepSmallSchemaShard};
use fold_db::storage::traits::NamespacedStore;

use super::keys::{mol_key, MolKey};
use super::ReapError;

/// The keep-small collection.
pub(crate) const COLLECTION: &str = "keep_small";

/// The exact shard keys of the dropped names, one per spelling.
pub(crate) fn shard_keys(names: &BTreeSet<String>) -> Vec<String> {
    names
        .iter()
        .map(|name| keep_small_schema_shard_key(name))
        .collect()
}

/// What the shards of the dropped names hold.
#[derive(Debug, Default)]
pub(crate) struct ShardReport {
    /// Dropped names that have a shard.
    pub names_with_shard: BTreeSet<String>,
    /// Shards that do not decode. They are not an abort: the report is
    /// advisory only.
    pub undecodable: usize,
    /// Candidate molecules: digest to (one spelling, owner name).
    pub molecules: BTreeMap<MolKey, (String, String)>,
}

/// Read the shards of `names` with one batch read.
pub(crate) async fn read_shards(
    store: &Arc<dyn NamespacedStore>,
    names: &BTreeSet<String>,
) -> Result<ShardReport, ReapError> {
    let kv = store
        .open_namespace(COLLECTION)
        .await
        .map_err(|error| ReapError::Failed(format!("open keep_small: {error}")))?;
    let ordered: Vec<&String> = names.iter().collect();
    let keys: Vec<Vec<u8>> = ordered
        .iter()
        .map(|name| keep_small_schema_shard_key(name).into_bytes())
        .collect();
    let values = kv
        .get_many(keys)
        .await
        .map_err(|error| ReapError::Failed(format!("read keep_small shards: {error}")))?;
    let mut report = ShardReport::default();
    for (name, value) in ordered.into_iter().zip(values) {
        let Some(value) = value else { continue };
        report.names_with_shard.insert(name.clone());
        let Ok(shard) = serde_json::from_slice::<KeepSmallSchemaShard>(&value) else {
            report.undecodable += 1;
            continue;
        };
        let ids = shard
            .molecules
            .keys()
            .chain(shard.molecule_tip_sources.keys())
            .chain(shard.molecule_key_bytes.keys())
            .chain(shard.molecule_schema.keys());
        for id in ids {
            report
                .molecules
                .entry(mol_key(id))
                .or_insert_with(|| (id.clone(), name.clone()));
        }
    }
    Ok(report)
}
