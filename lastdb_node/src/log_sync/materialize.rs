//! Apply VerifiedHistory LWW winners into FoldDB records.
//!
//! Pin mapping (settle PR Q4): history namespace = FoldDB schema name, record
//! key = primary hash key, value = UTF-8 JSON object of fields (empty value =
//! tombstone delete). Schema definitions come from signed `schema` entries.
//! The record writes live in `signed_history_materialize`.

use std::path::Path;
use std::sync::Arc;

use fold_db::fold_db_core::FoldDB;
use mutation_log::VerifiedHistory;

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
    signed_history_materialize::declare_history_schemas(store, history).await?;
    signed_history_materialize::apply_history(store, history).await
}
