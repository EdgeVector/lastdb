//! [`AtomStore::commit_tip_changes`] is the only function that writes `mk:` tip
//! records, conflict record keys, atom bodies, and counters. It runs inside the
//! native write request scope, so every read sees the committed root, the
//! accepted batches, and this request's pending batch, and the one
//! `batch_mutate` at the end joins the pending batch of the request.

use super::build::build_mutations;
use super::plan::{plan_atom, AtomBefore, AtomPlan, BodyOffer, PlanError};
use super::read::Offer;
use super::records::{dedupe_records, with_new_record_rows};
use super::report::CounterEvent;
use crate::db_operations::atom_store::{AtomStore, PerKeyRecord};
use crate::schema::SchemaError;
use crate::write_preparation::measure;
use std::collections::{BTreeMap, HashMap};

pub(crate) use super::commit_types::*;

impl AtomStore {
    /// Plan and write one change of tip records and conflict records.
    ///
    /// The caller holds the native write gate through a request scope. A call
    /// outside a scope on a native store is a defect and fails here, because the
    /// atomic batch is the whole safety argument.
    pub(crate) async fn commit_tip_changes(
        &self,
        set: TipChangeSet<'_>,
    ) -> Result<TipCommit, SchemaError> {
        self.require_write_scope()?;
        let TipChangeSet {
            scope,
            tips,
            bodies,
            body_source,
            records,
            releases,
            rows,
            deletes,
            guarded_deletes,
        } = set;
        let tips = collapse_tips(tips);
        let old = measure("old_tips_us", self.read_old_tips(&tips)).await?;
        let records = dedupe_records(records);
        let mut rows = rows;
        let new_records = self.read_new_records(&records).await?;
        let released = self.read_released_records(releases).await?;
        let file_deltas = released.file_deltas(&records, &new_records);
        let (key_rows, key_deletes) = self
            .history_key_rows(&records, &new_records, &released.history_slots)
            .await?;
        rows.extend(key_rows);
        self.note_touched_atoms(&tips, &old, (&records, &new_records));
        self.note_released_atoms(&released);
        let mut deltas = ref_deltas(&tips, &old, &records, &new_records);
        released.lower(&mut deltas);
        let rows = with_new_record_rows(rows, &records, &new_records);
        let records_released = released.count();
        let mut deletes = deletes;
        deletes.extend(key_deletes);
        deletes.extend(released.into_deletes());
        let guarded = self.guarded_delete_keys(guarded_deletes).await?;
        let guarded_deleted = guarded.len() as u64;
        deletes.extend(guarded);
        let before = measure("atom_counters_us", self.read_counters(&deltas, scope)).await?;
        let sources = BodySources {
            bodies,
            source: body_source,
        };
        let offers = measure(
            "body_offers_us",
            self.offer_bodies(&deltas, &before, &sources),
        )
        .await?;
        let plans = plan_all(&deltas, &before, &offers)?;
        let (mutations, mut commit) = measure(
            "build_rows_us",
            build_mutations(
                self,
                scope,
                &tips,
                &old,
                (rows, deletes, file_deltas),
                plans,
                offers,
            ),
        )
        .await?;
        commit.old_tips = old.len() as u64;
        commit.records_released = records_released;
        commit.guarded_deleted = guarded_deleted;
        if !mutations.is_empty() {
            self.main_store
                .inner()
                .batch_mutate(mutations)
                .await
                .map_err(|error| {
                    SchemaError::InvalidData(format!("commit tip changes: {error}"))
                })?;
        }
        Ok(commit)
    }

    /// Tell the verifier which atoms this batch changes, while a run reads.
    /// Every atom of an old or a new tip or of a new record counts, also when
    /// the net change of its count is zero: a tip that moves from one atom to
    /// another changes what a page of tips shows. One atomic load when no run
    /// reads.
    fn note_touched_atoms(
        &self,
        tips: &BTreeMap<String, Option<PerKeyRecord>>,
        old: &HashMap<String, PerKeyRecord>,
        (records, new_records): (&[RecordRefs], &[bool]),
    ) {
        let watch = self.counter_verify().watch();
        if !watch.is_active() {
            return;
        }
        let new_tips = tips.values().flatten();
        let atoms = new_tips
            .chain(old.values())
            .map(|tip| tip.entry.atom_uuid.as_str())
            .chain(
                records
                    .iter()
                    .zip(new_records)
                    .filter(|(_, is_new)| **is_new)
                    .flat_map(|(record, _)| record.atoms.iter().map(String::as_str)),
            );
        watch.note(atoms);
    }

    /// A native writer needs a request scope.
    pub(in crate::db_operations::atom_store) fn require_write_scope(
        &self,
    ) -> Result<(), SchemaError> {
        let Some(runtime) = self
            .namespaced_store
            .as_ref()
            .and_then(|store| store.disk_pack_runtime())
        else {
            return Ok(());
        };
        if crate::storage::disk_pack::transaction::publication::active(Some(runtime)) {
            return Ok(());
        }
        Err(SchemaError::InvalidData(
            "atom counter changes require a complete native write scope".into(),
        ))
    }
}

/// Keep the last change of each tip key, in key order.
fn collapse_tips(tips: Vec<TipChange>) -> BTreeMap<String, Option<PerKeyRecord>> {
    let mut out = BTreeMap::new();
    for tip in tips {
        out.insert(tip.key, tip.new);
    }
    out
}

/// The net change of references for each atom. Zero entries stay out.
fn ref_deltas(
    tips: &BTreeMap<String, Option<PerKeyRecord>>,
    old: &HashMap<String, PerKeyRecord>,
    records: &[RecordRefs],
    new_records: &[bool],
) -> BTreeMap<String, i64> {
    let mut deltas: BTreeMap<String, i64> = BTreeMap::new();
    for (key, new) in tips {
        if let Some(old) = old.get(key) {
            *deltas.entry(old.entry.atom_uuid.clone()).or_default() -= 1;
        }
        if let Some(new) = new {
            *deltas.entry(new.entry.atom_uuid.clone()).or_default() += 1;
        }
    }
    for (record, is_new) in records.iter().zip(new_records) {
        if !is_new {
            continue;
        }
        for atom in &record.atoms {
            *deltas.entry(atom.clone()).or_default() += 1;
        }
    }
    deltas.retain(|_, delta| *delta != 0);
    deltas
}

fn plan_all(
    deltas: &BTreeMap<String, i64>,
    before: &HashMap<String, AtomBefore>,
    offers: &HashMap<String, Offer>,
) -> Result<Vec<(String, AtomPlan)>, SchemaError> {
    let mut plans = Vec::with_capacity(deltas.len());
    for (atom, delta) in deltas {
        let state = before.get(atom).expect("every delta atom has a state");
        let offer = offers.get(atom).map(|offer| BodyOffer {
            blobs: offer.blobs.as_slice(),
            size: offer.size,
            source_schema: offer.atom.source_schema_name(),
        });
        let plan = plan_atom(state, *delta, offer.as_ref()).map_err(|error| match error {
            PlanError::NoBody => SchemaError::InvalidData(format!(
                "a tip names atom {atom}, which has no counter and no body to store"
            )),
            PlanError::Overflow => {
                SchemaError::InvalidData(format!("atom {atom} reference count overflows"))
            }
        })?;
        plans.push((atom.clone(), plan));
    }
    Ok(plans)
}

impl TipCommit {
    pub(super) fn note_fault(&mut self, event: &CounterEvent) {
        if !matches!(event, CounterEvent::Created | CounterEvent::Deleted) {
            self.counter_events += 1;
        }
    }
}
