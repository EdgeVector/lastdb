//! Append-only exact body deletes, then derived keys, with durable evidence.

use super::*;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use fold_db::db_operations::AtomStore;
use fold_db::storage::laststore::offline_physical_collection_pair;
use fold_db::storage::traits::KvStore;

const BATCH_KEYS: usize = 1000;

pub(super) async fn execute(
    args: &TargetAtomGcArgs,
    saved: &model::Plan,
) -> Result<model::Report, String> {
    prove_stopped(args)?;
    {
        let readonly = open_home_for_offline_read(&args.home)?;
        if readonly.seam != "at-rest-seam" {
            return Err("target atom execution requires the at-rest reader".into());
        }
        let current = plan::build(args, &readonly, &saved.started_at).await?;
        model::exact_plan_equal(saved, &current)?;
    }
    prove_stopped(args)?;
    let opened = crate::home::open_home(&args.home, args.i_know_this_is_primary)?;
    if opened.seam != "at-rest-seam" {
        return Err("target atom execution requires the at-rest reader".into());
    }
    let current = plan::build(args, &opened, &saved.started_at).await?;
    model::exact_plan_equal(saved, &current)?;
    prove_stopped(args)?;
    let raw_store = opened
        .base
        .raw_last_store()
        .ok_or("home has no physical LastStore")?;
    let csn_before = raw_store.csn_high_water();
    let crypto =
        crate::home::load_home_crypto(&args.home).ok_or("home has no at-rest identity key")?;
    let (raw, _) = offline_physical_collection_pair(Arc::clone(&raw_store), "atoms", crypto);
    let derived = opened.store.open_namespace("main").await.map_err(err)?;
    // create_new prevents any silent repeat after a complete or partial run.
    private_io::write(
        &args.plan_dir,
        "target-atom-execute-intent.json",
        &saved.counts,
    )?;
    let atoms = AtomStore::from_namespaced_store(Arc::clone(&opened.store))
        .await
        .map_err(err)?;
    let handle = ledger::begin(&opened, &atoms, saved).await?;
    let ledger_key = handle.key().to_string();
    private_io::write(&args.plan_dir, "target-atom-ledger-key.json", &ledger_key)?;
    let mut report = model::Report::planned(saved);
    report.execute = true;
    report.atom_retirement_state_unchanged = false;
    report.normal_owner_compaction_required = true;
    report.fresh_snapshot_required = true;
    report.csn_before = csn_before;
    input::read(args)?;
    delete_bodies(args, saved, &opened, &*raw, &mut report).await?;
    // No derived edge disappears until every admitted body copy is absent.
    verify_all_bodies(saved, &*raw).await?;
    input::read(args)?;
    delete_derived(args, saved, &opened, &*derived, &mut report).await?;
    verify_all_bodies(saved, &*raw).await?;
    input::read(args)?;
    if model::retirement_state(&opened.store_root)? != saved.retirement_state_sha256 {
        return Err("atom retirement state changed during append-only target reclaim".into());
    }
    prove_stopped(args)?;
    // The ledger API absorbs commit errors; its durable reread is mandatory.
    atoms.commit_delete_ledger_row(handle, |_| {}).await;
    opened
        .store
        .restore_durability_barrier()
        .await
        .map_err(err)?;
    ledger::verify(&opened, &ledger_key, saved, true).await?;
    report.ledger_committed = true;
    report.atom_retirement_state_unchanged = true;
    report.csn_after = raw_store.csn_high_water();
    if report.csn_after < report.csn_before {
        return Err("storage durability counter moved backwards".into());
    }
    prove_stopped(args)?;
    private_io::write(&args.plan_dir, "target-atom-execute.json", &report)?;
    Ok(report)
}

async fn delete_bodies(
    args: &TargetAtomGcArgs,
    saved: &model::Plan,
    opened: &HomeStore,
    raw: &dyn KvStore,
    report: &mut model::Report,
) -> Result<(), String> {
    let mut copies = saved
        .candidates
        .iter()
        .flat_map(|atom| &atom.copies)
        .collect::<Vec<_>>();
    copies
        .sort_by(|a, b| (a.shard, a.group_id, &a.key_b64).cmp(&(b.shard, b.group_id, &b.key_b64)));
    for batch in copies.chunks(BATCH_KEYS) {
        prove_stopped(args)?;
        let keys = exact_body_batch(raw, batch).await?;
        raw.batch_delete(keys.clone()).await.map_err(err)?;
        opened
            .store
            .restore_durability_barrier()
            .await
            .map_err(err)?;
        require_absent(raw, keys).await?;
        report.atom_copies_deleted += batch.len() as u64;
    }
    if report.atom_copies_deleted != saved.counts.candidate_copies {
        return Err("exact target atom body count differs".into());
    }
    Ok(())
}

async fn exact_body_batch(
    raw: &dyn KvStore,
    copies: &[&model::BodyCopy],
) -> Result<Vec<Vec<u8>>, String> {
    let keys = copies
        .iter()
        .map(|copy| STANDARD.decode(&copy.key_b64).map_err(err))
        .collect::<Result<Vec<_>, _>>()?;
    let values = raw.get_many(keys.clone()).await.map_err(err)?;
    if values.len() != copies.len() {
        return Err("exact target body batch count differs".into());
    }
    for (copy, value) in copies.iter().zip(values) {
        if copy.collection != "atoms" {
            return Err("target delete plan contains a non-admitted physical collection".into());
        }
        let value = value.ok_or("exact target atom body is absent")?;
        if value.len() as u64 != copy.raw_bytes || model::digest(&value) != copy.raw_sha256 {
            return Err("exact target atom body bytes changed".into());
        }
    }
    Ok(keys)
}

async fn verify_all_bodies(saved: &model::Plan, raw: &dyn KvStore) -> Result<(), String> {
    let keys = saved
        .candidates
        .iter()
        .flat_map(|atom| &atom.copies)
        .map(|copy| STANDARD.decode(&copy.key_b64).map_err(err))
        .collect::<Result<Vec<_>, _>>()?;
    for batch in keys.chunks(BATCH_KEYS) {
        require_absent(raw, batch.to_vec()).await?;
    }
    Ok(())
}

async fn delete_derived(
    args: &TargetAtomGcArgs,
    saved: &model::Plan,
    opened: &HomeStore,
    main: &dyn KvStore,
    report: &mut model::Report,
) -> Result<(), String> {
    let keys = saved
        .candidates
        .iter()
        .flat_map(|atom| &atom.derived_keys)
        .map(|key| key.as_bytes().to_vec())
        .collect::<Vec<_>>();
    for batch in keys.chunks(BATCH_KEYS) {
        prove_stopped(args)?;
        main.batch_delete(batch.to_vec()).await.map_err(err)?;
        opened
            .store
            .restore_durability_barrier()
            .await
            .map_err(err)?;
        require_absent(main, batch.to_vec()).await?;
        report.derived_storage_keys_deleted += batch.len() as u64;
    }
    if report.derived_storage_keys_deleted != saved.counts.derived_storage_keys {
        return Err("derived target delete key count differs".into());
    }
    Ok(())
}

async fn require_absent(kv: &dyn KvStore, keys: Vec<Vec<u8>>) -> Result<(), String> {
    let expected = keys.len();
    let present = kv.exists_many(keys).await.map_err(err)?;
    if present.len() != expected || present.iter().any(|value| *value) {
        return Err("an exact target delete key remains present".into());
    }
    Ok(())
}
