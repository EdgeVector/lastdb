//! Exact physical deletion, durable count-only ledger, and mutable compaction.

use super::*;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use fold_db::db_operations::{AtomDeleteLedgerEntry, AtomStore};
use fold_db::storage::laststore::{offline_physical_collection_pair, CollectionCompactOptions};
use std::collections::BTreeMap;

pub(super) async fn execute(
    args: &FileBlobGcArgs,
    plan: &model::Plan,
) -> Result<model::Report, String> {
    prove_stopped(args)?;
    let opened = crate::home::open_home(&args.home, args.i_know_this_is_primary)?;
    let current = inventory::build(&args.home, &opened, &plan.started_at).await?;
    model::exact_plan_equal(plan, &current)?;
    prove_stopped(args)?;
    let raw_store = opened
        .base
        .raw_last_store()
        .ok_or("home has no physical LastStore")?;
    let csn_before = raw_store.csn_high_water();
    let mut by_collection: BTreeMap<String, Vec<&model::BlobRow>> = BTreeMap::new();
    for row in &plan.candidates {
        by_collection
            .entry(row.collection.clone())
            .or_default()
            .push(row);
    }
    // A finished or interrupted execution cannot be silently repeated.
    model::write_private(
        &args.plan_dir,
        "file-blob-execute-intent.json",
        &plan.counts,
    )?;
    let atoms = AtomStore::from_namespaced_store(Arc::clone(&opened.store))
        .await
        .map_err(err)?;
    let mut report = model::Report {
        event: "file_blob_gc_offline",
        execute: true,
        counts: model::PublicCounts::from(&plan.counts),
        ledger_committed: false,
        file_blobs_deleted: 0,
        compactions: Vec::new(),
        atom_retirement_state_unchanged: false,
        fresh_snapshot_required: true,
        pre_blob_snapshot_writer_count: u64::try_from(plan.pre_blob_snapshot_writer_map.len())
            .unwrap_or(u64::MAX),
        csn_before,
        csn_after: csn_before,
    };
    let handle = begin_ledger(&opened, &atoms, plan).await?;
    let ledger_key = handle.key().to_string();
    delete_rows(args, plan, &opened, &by_collection, &mut report).await?;
    // The commit API absorbs storage errors; the durable reread is mandatory.
    atoms
        .commit_delete_ledger_row(handle, |row| {
            row.file_blobs_deleted = report.file_blobs_deleted;
            row.storage_keys_deleted = report.file_blobs_deleted;
        })
        .await;
    opened
        .store
        .restore_durability_barrier()
        .await
        .map_err(err)?;
    verify_ledger(&opened, &ledger_key, plan, true).await?;
    report.ledger_committed = true;
    for collection in by_collection.keys() {
        prove_stopped(args)?;
        let compact = opened
            .base
            .compact_collection(CollectionCompactOptions {
                collection: collection.clone(),
                dry_run: false,
                seed_committed_history: false,
            })
            .await
            .map_err(err)?;
        if !compact.executed || !compact.compactable_here || compact.bytes_after.is_none() {
            return Err("file blob mutable compaction did not complete".into());
        }
        report.compactions.push(compact);
    }
    opened
        .store
        .restore_durability_barrier()
        .await
        .map_err(err)?;
    report.csn_after = raw_store.csn_high_water();
    if report.csn_after < report.csn_before {
        return Err("storage durability counter moved backwards".into());
    }
    if model::retirement_state(&opened.store_root)? != plan.retirement_state_sha256 {
        return Err("atom retirement state changed during mutable file blob reclaim".into());
    }
    report.atom_retirement_state_unchanged = true;
    prove_stopped(args)?;
    model::write_private(&args.plan_dir, "file-blob-execute.json", &report)?;
    Ok(report)
}

async fn begin_ledger(
    opened: &HomeStore,
    atoms: &AtomStore,
    plan: &model::Plan,
) -> Result<fold_db::db_operations::DeleteLedgerHandle, String> {
    let mut entry =
        AtomDeleteLedgerEntry::gc_file_blobs("offline-physical-file-blob-gc", &plan.started_at);
    entry.atoms_scanned = plan.counts.atoms_read;
    entry.file_blobs_scanned = plan.counts.file_blobs_read;
    entry.file_blobs_referenced = plan.counts.file_blobs_referenced;
    entry.file_blobs_skipped_recent = plan.counts.file_blobs_recent;
    entry.file_blobs_deleted = plan.counts.candidate_rows;
    entry.storage_keys_deleted = plan.counts.candidate_rows;
    entry.file_blob_bytes_freed_approx = plan.counts.candidate_stored_bytes;
    let handle = atoms
        .begin_delete_ledger_row(None, entry)
        .await
        .map_err(err)?;
    let ledger_key = handle.key().to_string();
    opened
        .store
        .restore_durability_barrier()
        .await
        .map_err(err)?;
    verify_ledger(opened, &ledger_key, plan, false).await?;
    Ok(handle)
}

async fn delete_rows(
    args: &FileBlobGcArgs,
    plan: &model::Plan,
    opened: &HomeStore,
    by_collection: &BTreeMap<String, Vec<&model::BlobRow>>,
    report: &mut model::Report,
) -> Result<(), String> {
    let raw_store = opened
        .base
        .raw_last_store()
        .ok_or("home has no physical LastStore")?;
    let crypto =
        crate::home::load_home_crypto(&args.home).ok_or("home has no at-rest identity key")?;
    for (collection, rows) in by_collection {
        let (raw, _) = offline_physical_collection_pair(
            Arc::clone(&raw_store),
            collection,
            Arc::clone(&crypto),
        );
        for batch in rows.chunks(1000) {
            prove_stopped(args)?;
            let keys = exact_batch(&*raw, batch).await?;
            raw.batch_delete(keys.clone()).await.map_err(err)?;
            opened
                .store
                .restore_durability_barrier()
                .await
                .map_err(err)?;
            let exists = raw.exists_many(keys).await.map_err(err)?;
            if exists.len() != batch.len() || exists.iter().any(|present| *present) {
                return Err("file blob delete batch did not remove every exact key".into());
            }
            report.file_blobs_deleted += batch.len() as u64;
        }
    }
    if report.file_blobs_deleted != plan.counts.candidate_rows {
        return Err("deleted file blob count differs from the exact plan".into());
    }
    Ok(())
}

async fn exact_batch(
    raw: &dyn fold_db::storage::traits::KvStore,
    rows: &[&model::BlobRow],
) -> Result<Vec<Vec<u8>>, String> {
    let keys = rows
        .iter()
        .map(|row| STANDARD.decode(&row.key_b64).map_err(err))
        .collect::<Result<Vec<_>, _>>()?;
    let values = raw.get_many(keys.clone()).await.map_err(err)?;
    if values.len() != rows.len() {
        return Err("file blob recheck batch count differs".into());
    }
    for (row, value) in rows.iter().zip(values) {
        let value = value.ok_or("exact file blob candidate is absent")?;
        if model::digest(&value) != row.raw_sha256 || value.len() as u64 != row.raw_bytes {
            return Err("exact file blob candidate bytes changed".into());
        }
    }
    Ok(keys)
}

async fn verify_ledger(
    opened: &HomeStore,
    key: &str,
    plan: &model::Plan,
    committed: bool,
) -> Result<(), String> {
    let main = opened.store.open_namespace("main").await.map_err(err)?;
    let bytes = main
        .get(key.as_bytes())
        .await
        .map_err(err)?
        .ok_or("delete ledger is absent")?;
    let row: AtomDeleteLedgerEntry = serde_json::from_slice(&bytes).map_err(err)?;
    if row.version != 1
        || row.verb != "gc-file-blobs"
        || row.committed != committed
        || row.atoms_deleted != 0
        || row.file_blobs_deleted != plan.counts.candidate_rows
        || row.storage_keys_deleted != plan.counts.candidate_rows
        || row.file_blob_bytes_freed_approx != plan.counts.candidate_stored_bytes
        || row.scan_started_at.as_deref() != Some(plan.started_at.as_str())
    {
        return Err("durable file blob delete ledger differs from the exact plan".into());
    }
    Ok(())
}
