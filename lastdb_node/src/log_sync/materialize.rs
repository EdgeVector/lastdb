//! Apply VerifiedHistory LWW winners into FoldDB records.
//!
//! Mapping: history namespace = FoldDB schema name, record
//! key = primary hash key, value = UTF-8 JSON object of fields (empty value =
//! tombstone delete). Schema definitions come from signed `schema` entries.
//! The record writes live in `signed_history_materialize`. External file Puts
//! fetch the sealed blob through the same path personal sync uses.

use std::path::Path;
use std::sync::Arc;

use fold_db::fold_db_core::FoldDB;
use mutation_log::VerifiedHistory;

use super::history_store::HistoryMap;
use super::materialize_files;
use super::relay::RelayClient;
use crate::host::owned::ResolvedStore;
use crate::host::Host;
use crate::personal_seal::Sealer;

/// What a sync step needs about one database: its id, home, and the FoldDB its winners land in.
pub(super) struct DatabaseCtx<'a> {
    pub(super) database_id: &'a str,
    pub(super) store: &'a FoldDB,
    pub(super) home: &'a Path,
}

/// The FoldDB holding `database_id`: the personal store or an owned sibling.
pub(crate) fn store_for(host: &Host, database_id: &str) -> Option<Arc<FoldDB>> {
    match host.owned.resolve(database_id) {
        Ok(ResolvedStore::Owner) => Some(Arc::clone(&host.db)),
        Ok(ResolvedStore::Owned(store)) => Some(store),
        Ok(ResolvedStore::Absent) | Err(_) => None,
    }
}

/// Materialize `database_id`'s history winners and heal any `dirty_keys` left
/// behind by a previously failed unsigned-write rollback.
pub(super) async fn rematerialize(
    histories: &HistoryMap,
    database_id: &str,
    store: &FoldDB,
    home: &Path,
    client: Option<&RelayClient>,
    sealer: Option<&Sealer>,
) -> Result<(), String> {
    let history = histories.snapshot(database_id)?;
    if history.owner().is_some() {
        apply_history_records(store, &history, home, database_id, client, sealer).await?;
    }
    let dirty = histories.take_dirty_keys(database_id);
    if dirty.is_empty() {
        return Ok(());
    }
    if let Err(error) = reconcile(
        store,
        &history,
        dirty.iter(),
        home,
        database_id,
        client,
        sealer,
    )
    .await
    {
        histories.restore_dirty_keys(database_id, dirty);
        return Err(error);
    }
    Ok(())
}

async fn apply_history_records(
    store: &FoldDB,
    history: &VerifiedHistory,
    home: &Path,
    database_id: &str,
    client: Option<&RelayClient>,
    sealer: Option<&Sealer>,
) -> Result<(), String> {
    signed_history_materialize::declare_history_schemas(store, history).await?;
    match (client, sealer) {
        (Some(client), Some(sealer)) => {
            apply_with_files(store, history, home, database_id, client, sealer).await?;
        }
        _ => {
            signed_history_materialize::apply_history(store, history).await?;
        }
    }
    let keys = excluded_keys(history);
    if keys.is_empty() {
        return Ok(());
    }
    reconcile(
        store,
        history,
        keys.iter(),
        home,
        database_id,
        client,
        sealer,
    )
    .await
}

async fn reconcile<'a>(
    store: &FoldDB,
    history: &VerifiedHistory,
    keys: impl IntoIterator<Item = &'a (String, String)>,
    home: &Path,
    database_id: &str,
    client: Option<&RelayClient>,
    sealer: Option<&Sealer>,
) -> Result<(), String> {
    match (client, sealer) {
        (Some(client), Some(sealer)) => {
            for (namespace, record_key) in keys {
                if let Some(winner) = history
                    .records()
                    .get(&(namespace.clone(), record_key.clone()))
                {
                    materialize_files::apply_with_file(
                        &materialize_files::FileApply {
                            store,
                            history,
                            client,
                            sealer,
                            home,
                            database_id,
                        },
                        namespace,
                        record_key,
                        winner,
                    )
                    .await?;
                } else {
                    signed_history_materialize::reconcile_keys(
                        store,
                        history,
                        std::slice::from_ref(&(namespace.clone(), record_key.clone())),
                    )
                    .await?;
                }
            }
            Ok(())
        }
        _ => signed_history_materialize::reconcile_keys(store, history, keys).await,
    }
}

async fn apply_with_files(
    store: &FoldDB,
    history: &VerifiedHistory,
    home: &Path,
    database_id: &str,
    client: &RelayClient,
    sealer: &Sealer,
) -> Result<(), String> {
    let mut winners: Vec<_> = history.records().iter().collect();
    winners.sort_by(|(key_a, rec_a), (key_b, rec_b)| {
        let pos_a = (
            rec_a.position.lamport,
            rec_a.position.node_key,
            rec_a.position.device_id,
        );
        let pos_b = (
            rec_b.position.lamport,
            rec_b.position.node_key,
            rec_b.position.device_id,
        );
        pos_b.cmp(&pos_a).then_with(|| key_a.cmp(key_b))
    });
    let ctx = materialize_files::FileApply {
        store,
        history,
        client,
        sealer,
        home,
        database_id,
    };
    for ((namespace, record_key), winner) in winners {
        materialize_files::apply_with_file(&ctx, namespace, record_key, winner).await?;
    }
    Ok(())
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

/// Record keys whose unsigned store write could not be reverted immediately,
/// scheduling reconciliation on the next materialization turn.
pub(crate) fn mark_dirty_keys(host: &Host, database_id: &str, keys: &[(String, String)]) {
    super::histories_for(host).mark_dirty_keys(database_id, keys);
}

/// Bring `store` back to the history at the keys of a write whose signed
/// entries were not all confirmed. Returns `true` when every key was reconciled
/// immediately; on failure, records `keys` in `dirty_keys` and schedules a
/// materialization retry so the next sync turn heals them.
pub(crate) async fn revert_unsigned(
    host: &Host,
    database_id: &str,
    store: &FoldDB,
    keys: &[(String, String)],
) -> bool {
    let histories = super::histories_for(host);
    let reverted = match histories.snapshot(database_id) {
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
        histories.mark_dirty_keys(database_id, keys);
        return false;
    }
    true
}
