//! Bounded copy, encrypted progress, and exact verification against held roots.

use super::{conversion_plan::ConversionPlan, invalid, source::budget, StorageResult};
use crate::storage::disk_pack::map;
use laststore::disk_run::home::{AdministrativePage, HomeMutation, RunHome, RunRead};
use serde::{Deserialize, Serialize};

pub(super) const CHECKPOINT_KEY: &str = "typed-range-conversion-v1";
pub(super) const CHECKPOINT_COLLECTION: &str = "__native_home";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Phase {
    Main,
    Cas,
    Copied,
    Complete,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Checkpoint {
    pub(super) version: u8,
    pub(super) job: String,
    pub(super) homes: [uuid::Uuid; 2],
    pub(super) phase: Phase,
    pub(super) after: Option<Vec<u8>>,
    pub(super) records: [u64; 2],
    pub(super) logical_bytes: [u64; 2],
}

impl Checkpoint {
    pub(super) fn mutation(&self) -> StorageResult<HomeMutation> {
        Ok(HomeMutation {
            collection: CHECKPOINT_COLLECTION.into(),
            key: CHECKPOINT_KEY.into(),
            body: Some(
                serde_json::to_vec(self)
                    .map_err(|_| invalid("cannot encode encrypted progress"))?,
            ),
        })
    }
}

pub(super) fn page(source: &RunRead, after: Option<&[u8]>) -> StorageResult<AdministrativePage> {
    let mut limit = 128;
    loop {
        let result = source
            .fork(budget(256 * 1024 * 1024, 4096))
            .administrative_snapshot_page(after, limit);
        match result {
            Err(laststore::Error::ReadBudgetExceeded) if limit > 1 => limit /= 2,
            _ => return result.map_err(map),
        }
    }
}

pub(super) fn copy_page(
    source: &RunRead,
    target: &[std::sync::Arc<RunHome>; 2],
    plan: &ConversionPlan,
    checkpoint: &mut Checkpoint,
) -> StorageResult<bool> {
    let slot = match checkpoint.phase {
        Phase::Main => 0,
        Phase::Cas => 1,
        _ => return Ok(true),
    };
    let page = page(source, checkpoint.after.as_deref())?;
    let mut batch = Vec::new();
    for row in page.records {
        let translated = plan.record(row, slot)?;
        checkpoint.records[slot] += 1;
        checkpoint.logical_bytes[slot] += (translated.collection.len()
            + translated.key.len()
            + translated.body.as_ref().map_or(0, Vec::len))
            as u64;
        batch.push(translated);
    }
    checkpoint.after = page.resume.or_else(|| checkpoint.after.take());
    if slot == 0 {
        batch.push(checkpoint.mutation()?);
        commit(&target[0], batch)?;
    } else {
        // CAS publishes first. A crash before the main checkpoint repeats
        // the same exact CAS keys and values, without counter changes.
        if !batch.is_empty() {
            commit(&target[1], batch)?;
        }
        commit(&target[0], vec![checkpoint.mutation()?])?;
    }
    Ok(page.exhausted)
}

pub(super) fn verify(
    source: &RunRead,
    target: &RunHome,
    plan: &ConversionPlan,
    slot: usize,
) -> StorageResult<u64> {
    let mut after = None;
    let mut count = 0;
    loop {
        let page = page(source, after.as_deref())?;
        let expected = page
            .records
            .into_iter()
            .map(|row| plan.record(row, slot))
            .collect::<StorageResult<Vec<_>>>()?;
        compare(target, &expected)?;
        count += expected.len() as u64;
        if page.exhausted {
            return Ok(count);
        }
        after = page.resume;
    }
}

pub(super) fn compare(target: &RunHome, expected: &[HomeMutation]) -> StorageResult<()> {
    let coordinates: Vec<_> = expected
        .iter()
        .map(|row| (row.collection.clone(), row.key.clone()))
        .collect();
    let found = target
        .read(budget(256 * 1024 * 1024, 10_000))
        .get_many(&coordinates)
        .map_err(map)?;
    if found.len() != expected.len() {
        return Err(invalid("verification batch is incomplete"));
    }
    for (row, value) in expected.iter().zip(found) {
        if value.map_err(map)? != row.body {
            return Err(invalid(
                "a target record differs from its translated source",
            ));
        }
    }
    Ok(())
}

pub(super) fn count(target: &RunHome) -> StorageResult<u64> {
    let source = target.read(budget(8 * 1024 * 1024, 1));
    let mut after = None;
    let mut count = 0;
    loop {
        let page = page(&source, after.as_deref())?;
        count += page.records.len() as u64;
        if page.exhausted {
            return Ok(count);
        }
        after = page.resume;
    }
}

pub(super) fn commit(target: &RunHome, batch: Vec<HomeMutation>) -> StorageResult<()> {
    RunHome::check_mutations(&batch).map_err(map)?;
    let bytes = super::capacity::batch_allowance(&batch)?;
    let policy = super::capacity::policy()?;
    let space = laststore::space::reserve_space(target.path(), &policy, bytes).map_err(map)?;
    target.commit(batch).map_err(map)?;
    drop(space);
    while target.needs_merge() {
        let _space =
            laststore::space::reserve_space(target.path(), &policy, target.status().file_bytes())
                .map_err(map)?;
        if target.merge_step().map_err(map)?.is_none() {
            break;
        }
    }
    Ok(())
}

impl ConversionPlan {
    pub(super) fn record(&self, row: HomeMutation, slot: usize) -> StorageResult<HomeMutation> {
        if row.collection == CHECKPOINT_COLLECTION && row.key == CHECKPOINT_KEY {
            return Err(invalid("the source contains reserved conversion progress"));
        }
        if (row.collection == "cas_blobs") != (slot == 1) {
            return Err(invalid("a source record belongs to the other native home"));
        }
        if let Some(next) = self
            .metadata
            .get(&(row.collection.clone(), row.key.clone()))
        {
            return Ok(next.clone());
        }
        if matches!(row.collection.as_str(), "field_tips" | "blob_ref_edges") {
            return Err(invalid("a source contains a retired physical alias"));
        }
        if row.collection == "molecule_keys" && row.key.starts_with("mkb:") {
            return Err(invalid("a molecule metadata row has no schema profile"));
        }
        if matches!(
            row.collection.as_str(),
            "tips"
                | "indexes"
                | "proteins"
                | "molecule_ref_edges"
                | "molecule_keys"
                | "schemas"
                | "schema_states"
        ) {
            return Err(invalid(
                "a control record is absent from the bound metadata inventory",
            ));
        }
        Ok(row)
    }
}
