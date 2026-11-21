//! Apply VerifiedHistory LWW winners into FoldDB records.
//!
//! Pin mapping (settle PR Q4): history namespace = FoldDB schema name, record
//! key = primary hash key, value = UTF-8 JSON object of fields (empty value =
//! tombstone delete). The record writes live in `signed_history_materialize`.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;

use fold_db::fold_db_core::FoldDB;
use mutation_log::VerifiedHistory;
use serde_json::Value;

use crate::host::owned::ResolvedStore;
use crate::host::Host;

/// What a sync step needs about one database: where its local files live,
/// its id, and the FoldDB its winners land in.
pub(super) struct DatabaseCtx<'a> {
    pub(super) home: &'a Path,
    pub(super) database_id: &'a str,
    pub(super) store: &'a FoldDB,
}

/// The FoldDB holding `database_id`: the personal store or an owned sibling.
pub(super) fn store_for(host: &Host, database_id: &str) -> Option<Arc<FoldDB>> {
    match host.owned.resolve(database_id) {
        Ok(ResolvedStore::Owner) => Some(Arc::clone(&host.db)),
        Ok(ResolvedStore::Owned(store)) => Some(store),
        Ok(ResolvedStore::Absent) | Err(_) => None,
    }
}

pub(super) async fn apply_history_records(
    store: &FoldDB,
    history: &VerifiedHistory,
) -> Result<(), String> {
    ensure_record_schemas(store, history).await?;
    signed_history_materialize::apply_history(store, history).await
}

/// Phase 1 history has no schema entries. Declare each namespace from the JSON
/// keys of its winners before the first FoldDB write.
async fn ensure_record_schemas(store: &FoldDB, history: &VerifiedHistory) -> Result<(), String> {
    let mut fields_by_ns: BTreeMap<&str, BTreeSet<String>> = BTreeMap::new();
    for ((namespace, _), winner) in history.records() {
        if winner.value.is_empty() {
            continue;
        }
        let Ok(Value::Object(map)) = serde_json::from_slice::<Value>(&winner.value) else {
            continue;
        };
        let entry = fields_by_ns.entry(namespace.as_str()).or_default();
        for key in map.keys() {
            entry.insert(key.clone());
        }
    }
    for (namespace, fields) in fields_by_ns {
        if fields.is_empty() {
            signed_history_materialize::ensure_hash_schema(store, namespace).await?;
        } else {
            signed_history_materialize::ensure_string_fields_schema(store, namespace, &fields)
                .await?;
        }
    }
    Ok(())
}
