//! The store side of the planner: read the old state, plan, write one batch.
//!
//! [`AtomStore::commit_tip_changes`] is the only function that writes `mk:` tip
//! records, conflict record keys, atom bodies, and counters. It runs inside the
//! native write request scope, so every read sees the committed root, the
//! accepted batches, and this request's pending batch, and the one
//! `batch_mutate` at the end joins the pending batch of the request.

use super::build::build_mutations;
use super::molecule::molecule_changes;
use super::plan::{plan_atom, AtomBefore, AtomPlan, BodyOffer, PlanError};
use super::read::Offer;
use super::report::CounterEvent;
use crate::atom::Atom;
use crate::db_operations::atom_store::{AtomStore, PerKeyRecord, SealedAtom};
use crate::schema::SchemaError;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};

/// One tip record change. `new == None` removes the tip record.
pub(crate) struct TipChange {
    /// The full storage key of the `mk:` record, scope prefix included.
    pub key: String,
    pub new: Option<PerKeyRecord>,
}

/// One conflict record that holds references to atoms.
pub(crate) struct RecordRefs {
    /// The full storage key of the record that names the atoms.
    pub key: String,
    /// The distinct atoms that the record names.
    pub atoms: Vec<String>,
    /// The molecule the record belongs to. When the record is new, the planner
    /// raises `records` in the header of this molecule, in the same batch.
    pub molecule: String,
}

/// Everything one planner run needs.
pub(crate) struct TipChangeSet<'a> {
    pub scope: Option<&'a str>,
    pub tips: Vec<TipChange>,
    /// Bodies that this request created. The planner writes a body only for an
    /// atom that gains its first reference in this scope.
    pub bodies: &'a [SealedAtom],
    /// A scope to read a missing body from, for a scope that copies tips from
    /// another scope. `Some(None)` names the personal scope.
    pub body_source: Option<Option<&'a str>>,
    /// Conflict records written by this run. The planner adds one reference per
    /// distinct atom for each record that is new.
    pub records: Vec<RecordRefs>,
    /// Other rows of the same batch: molecule headers, order log, record rows.
    pub rows: Vec<(String, Value)>,
    /// Other keys that the same batch deletes.
    pub deletes: Vec<Vec<u8>>,
}

/// Where the planner finds a body for an atom that is new in the scope.
pub(super) struct BodySources<'a> {
    pub bodies: &'a [SealedAtom],
    pub source: Option<Option<&'a str>>,
}

/// An atom whose body a planner run deleted, with the blob references it held.
#[derive(Debug)]
pub(crate) struct DeletedAtom {
    pub uuid: String,
    pub blobs: Vec<String>,
}

/// What a planner run did, for reports and meters.
#[derive(Debug, Default)]
pub(crate) struct TipCommit {
    /// Bodies that this run wrote, for the keep-small meters.
    pub created: Vec<Atom>,
    /// Atoms whose count reached zero. The run deleted their bodies.
    pub deleted: Vec<DeletedAtom>,
    /// Counter faults: an underflow, an absent counter, or a dangling removal.
    pub counter_events: u64,
}

impl TipCommit {
    /// Fold the result of a later planner call of the same request into this one.
    pub(crate) fn absorb(&mut self, other: Self) {
        self.created.extend(other.created);
        self.deleted.extend(other.deleted);
        self.counter_events += other.counter_events;
    }
}

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
        self.require_atomic_scope()?;
        let TipChangeSet {
            scope,
            tips,
            bodies,
            body_source,
            records,
            rows,
            deletes,
        } = set;
        let tips = collapse_tips(tips);
        let old = self.read_old_tips(&tips).await?;
        let new_records = self.read_new_records(&records).await?;
        self.note_touched_atoms(&tips, &old, (&records, &new_records));
        let deltas = ref_deltas(&tips, &old, &records, &new_records);
        let changes = molecule_changes(&tips, &old, &records, &new_records);
        let before = self.read_counters(&deltas, scope).await?;
        let sources = BodySources {
            bodies,
            source: body_source,
        };
        let offers = self
            .offer_bodies(&deltas, &before, &tips, &sources, scope)
            .await?;
        let plans = plan_all(&deltas, &before, &offers)?;
        let (rows, molecule_run) = self.apply_molecule_counters(scope, &changes, rows).await?;
        let (mutations, mut commit) =
            build_mutations(self, scope, &tips, &old, (rows, deletes), plans, offers).await?;
        for (molecule, fault) in molecule_run.faults {
            self.counter_report().note_molecule(&molecule, fault);
            commit.counter_events += 1;
        }
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

    /// A native store commits one batch only inside a request scope.
    fn require_atomic_scope(&self) -> Result<(), SchemaError> {
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
