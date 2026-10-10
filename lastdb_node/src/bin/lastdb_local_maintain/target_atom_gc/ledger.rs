//! A new count-only ledger, independent of any interrupted manual GC row.

use super::*;
use fold_db::db_operations::{AtomDeleteLedgerEntry, AtomStore, DeleteLedgerHandle};

pub(super) const CALLER: &str = "offline-physical-target-atom-gc";

pub(super) async fn begin(
    opened: &HomeStore,
    atoms: &AtomStore,
    saved: &model::Plan,
) -> Result<DeleteLedgerHandle, String> {
    let mut row = AtomDeleteLedgerEntry::gc_atoms(CALLER, &saved.started_at);
    row.atoms_deleted = saved.counts.candidate_copies;
    row.atoms_scanned = saved.counts.target_copies_read;
    row.atoms_skipped_recent = saved.counts.recent_copies;
    row.atoms_skipped_undatable = saved.counts.undated_copies;
    row.storage_keys_deleted = saved.counts.candidate_copies + saved.counts.derived_storage_keys;
    let handle = atoms
        .begin_delete_ledger_row(None, row)
        .await
        .map_err(err)?;
    opened
        .store
        .restore_durability_barrier()
        .await
        .map_err(err)?;
    verify(opened, handle.key(), saved, false).await?;
    Ok(handle)
}

pub(super) async fn verify(
    opened: &HomeStore,
    key: &str,
    saved: &model::Plan,
    committed: bool,
) -> Result<(), String> {
    let main = opened.store.open_namespace("main").await.map_err(err)?;
    let value = main
        .get(key.as_bytes())
        .await
        .map_err(err)?
        .ok_or("target delete ledger is absent")?;
    let row: AtomDeleteLedgerEntry = serde_json::from_slice(&value).map_err(err)?;
    if row.version != 1
        || row.verb != "gc-atoms"
        || row.caller != CALLER
        || row.committed != committed
        || row.scan_started_at.as_deref() != Some(saved.started_at.as_str())
        || row.atoms_deleted != saved.counts.candidate_copies
        || row.atoms_scanned != saved.counts.target_copies_read
        || row.storage_keys_deleted
            != saved.counts.candidate_copies + saved.counts.derived_storage_keys
        || row.atoms_skipped_recent != saved.counts.recent_copies
        || row.atoms_skipped_undatable != saved.counts.undated_copies
        || row.tip_versions_pruned != 0
        || row.history_rows_deleted != 0
        || row.file_blobs_deleted != 0
        || row.schema.is_some()
        || row.key_fingerprint.is_some()
    {
        return Err("the durable target delete ledger differs from the exact plan".into());
    }
    Ok(())
}
