//! Apply VerifiedHistory LWW winners into FoldDB records.
//!
//! Mapping: history namespace = FoldDB schema name, record
//! key = primary hash key, value = UTF-8 JSON object of fields (empty value =
//! tombstone delete). Schema definitions come from signed `schema` entries.
//! The record writes live in `signed_history_materialize`.

use std::sync::Arc;

use fold_db::fold_db_core::FoldDB;
use mutation_log::VerifiedHistory;

use crate::host::owned::ResolvedStore;
use crate::host::Host;

/// What a sync step needs about one database: its id, and the FoldDB its winners land in.
pub(super) struct DatabaseCtx<'a> {
    pub(super) database_id: &'a str,
    pub(super) store: &'a FoldDB,
}

/// The FoldDB holding `database_id`: the personal store or an owned sibling.
pub(crate) fn store_for(host: &Host, database_id: &str) -> Option<Arc<FoldDB>> {
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
    signed_history_materialize::declare_history_schemas(store, history).await?;
    signed_history_materialize::apply_history(store, history).await?;
    // A lease closing can leave a record with no write that counts. The loop
    // above visits only records the history still holds, so these keys are
    // brought to the history one by one.
    let keys = excluded_keys(history);
    if keys.is_empty() {
        return Ok(());
    }
    signed_history_materialize::reconcile_keys(store, history, keys.iter()).await
}

/// The record keys that entries excluded by a lease closing wrote.
fn excluded_keys(history: &VerifiedHistory) -> std::collections::BTreeSet<(String, String)> {
    history
        .excluded_entries()
        .into_iter()
        .filter_map(|entry| {
            history
                .decode_data_entry(entry)
                .map(|(namespace, payload)| (namespace, payload.record_key))
        })
        .collect()
}

/// Bring `store` back to the history at the keys of a write whose signed
/// entries were not all confirmed.
///
/// A write commits to the store first, because the store validates it, and is
/// signed second. The history is the truth, so a record with no signed entry
/// must not stay: each key goes back to its history winner, or to no record.
/// Entries that were signed before the failure keep their records.
pub(crate) async fn revert_unsigned(
    host: &Host,
    database_id: &str,
    store: &FoldDB,
    keys: &[(String, String)],
) {
    let reverted = match super::histories_for(host).snapshot(database_id) {
        Ok(history) => signed_history_materialize::reconcile_keys(store, &history, keys).await,
        Err(error) => Err(error),
    };
    if let Err(error) = reverted {
        tracing::warn!(
            target: "lastdbd::log_sync",
            id = %database_id,
            %error,
            "an unsigned write could not be reverted to the history"
        );
    }
}
