//! Delete retired portable journal keys during admission.
//!
//! The predicate in `storage::retired` decides. This step only pages those
//! prefixes and writes tombstones. Values are dropped unread.

use super::run_node::{self, RUN_DIR};
use super::{map, native_routing};
use crate::storage::error::StorageResult;
use crate::storage::format::{MAIN_NAMESPACE, METADATA_NAMESPACE};
use crate::storage::retired;
use laststore::disk_run::home::{HomeMutation, ReadBudget, RunHome};
use std::path::Path;

const BATCH: usize = 32;

pub(in crate::storage::disk_pack) fn clear(root: &Path, node_key: &[u8; 32]) {
    let mut removed = 0u64;
    let result = sweep(root, node_key, &mut removed);
    match result {
        Ok(()) if removed > 0 => {
            tracing::info!(removed, "retired portable journal keys deleted");
        }
        Ok(()) => {}
        Err(error) => tracing::warn!(
            %error,
            removed,
            "retired portable journal cleanup will retry on the next open"
        ),
    }
}

fn sweep(root: &Path, node_key: &[u8; 32], removed: &mut u64) -> StorageResult<()> {
    let home = RunHome::open(&root.join(RUN_DIR), &run_node::home_key(node_key)).map_err(map)?;
    for namespace in [METADATA_NAMESPACE, MAIN_NAMESPACE] {
        for prefix in retired::retired_prefixes(namespace) {
            sweep_prefix(&home, namespace, prefix, removed)?;
        }
    }
    Ok(())
}

fn sweep_prefix(
    home: &RunHome,
    namespace: &str,
    prefix: &str,
    removed: &mut u64,
) -> StorageResult<()> {
    loop {
        let keys = next_batch(home, namespace, prefix)?;
        if keys.is_empty() {
            return Ok(());
        }
        let count = keys.len() as u64;
        let collection = native_routing::collection_for_namespace(namespace);
        let operations = keys
            .into_iter()
            .map(|key| HomeMutation {
                collection: collection.to_string(),
                key,
                body: None,
            })
            .collect();
        home.commit(operations).map_err(map)?;
        *removed += count;
    }
}

fn next_batch(home: &RunHome, namespace: &str, prefix: &str) -> StorageResult<Vec<String>> {
    let budget = ReadBudget {
        pages: 4096,
        blocks: 65536,
        bytes: 8 * 1024 * 1024,
        keys: (BATCH as u64).saturating_add(8),
    };
    let mut read = home.read(budget);
    let collection = native_routing::collection_for_namespace(namespace);
    let end = exclusive_end(prefix);
    let mut range = read
        .administrative_range(collection, prefix.as_bytes(), end.as_bytes())
        .map_err(map)?;
    let page = range.next_page(BATCH).map_err(map)?;
    let mut keys = Vec::new();
    for (key, value) in page.records {
        drop(value);
        if retired::is_retired_key(namespace, key.as_bytes()) {
            keys.push(key);
        }
    }
    Ok(keys)
}

fn exclusive_end(prefix: &str) -> String {
    let mut bytes = prefix.as_bytes().to_vec();
    let last = bytes.last_mut().expect("retired prefix");
    assert!(prefix.is_ascii() && *last < 0xff, "retired prefix end");
    *last += 1;
    String::from_utf8(bytes).expect("ascii prefix")
}
