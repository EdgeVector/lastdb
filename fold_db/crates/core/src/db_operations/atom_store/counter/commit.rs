//! [`AtomStore::commit_tip_changes`] is the only function that writes `mk:` tip
//! records, conflict record keys, atom bodies, and counters. It runs inside the
//! native write request scope, so every read sees the committed root, the
//! accepted batches, and this request's pending batch, and the one
//! `batch_mutate` at the end joins the pending batch of the request.

use super::build::build_mutations;
use super::molecule::molecule_changes;
use super::plan::{plan_atom, AtomBefore, AtomPlan, BodyOffer, PlanError};
use super::read::Offer;
use super::records::{dedupe_records, with_new_record_rows};
use super::report::CounterEvent;
use super::{AtomSize, DeletedAtom};
use crate::atom::{molecule_key_codec, Atom};
use crate::db_operations::atom_store::{AtomStore, PerKeyRecord};
use crate::schema::SchemaError;
use crate::write_preparation::measure;
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
    /// File references stored only in an archived tip's metadata.
    pub files: Vec<String>,
    /// Rows that only a new record writes: its retention queue entry and its
    /// slot directory entry. A repeat of a decision writes none of them.
    pub new_rows: Vec<(String, Value)>,
    /// The molecule the record belongs to. When the record is new, the planner
    /// raises `records` in the header of this molecule, in the same batch. A
    /// release of the record lowers it in the batch of the release.
    pub molecule: String,
}

/// One kept record to release. The planner reads the history row at `history`
/// in the write gate, lowers one reference for each distinct atom the row
/// names, lowers `records` of the molecule that the row names, and deletes the
/// row and `deletes` in the same batch.
pub(crate) struct ReleaseRequest {
    /// The full storage key of the history row that holds the references.
    pub history: String,
    /// The other rows of the record (conflict row, directory entry, queue
    /// entry). They go with the history row, or when it is already gone.
    pub deletes: Vec<Vec<u8>>,
}

/// A delete that runs only while the key still holds the value the caller read.
/// The planner reads the key in the write gate. A key that holds other bytes, or
/// none, is left alone.
pub(crate) struct GuardedDelete {
    pub key: Vec<u8>,
    pub expected: Vec<u8>,
}

/// Everything one planner run needs.
#[derive(Default)]
pub(crate) struct TipChangeSet<'a> {
    pub history: super::super::atom_history::HistoryMode,
    pub scope: Option<&'a str>,
    pub tips: Vec<TipChange>,
    /// Bodies that this request created. The planner writes a body only for an
    /// atom that gains its first reference in this scope.
    pub bodies: &'a [Atom],
    /// A scope to read a missing body from, for a scope that copies tips from
    /// another scope. `Some(None)` names the personal scope.
    pub body_source: Option<Option<&'a str>>,
    /// Conflict records written by this run. The planner adds one reference per
    /// distinct atom for each record that is new.
    pub records: Vec<RecordRefs>,
    /// Conflict records this run releases (a purge of their key, or the end of
    /// their retention window). See `records.rs`.
    pub releases: Vec<ReleaseRequest>,
    /// Other rows of the same batch: molecule headers, order log, record rows.
    pub rows: Vec<(String, Value)>,
    /// Other keys that the same batch deletes.
    pub deletes: Vec<Vec<u8>>,
    /// Keys that the batch deletes only if they still hold the bytes read
    /// before (the retirement of a queue bucket).
    pub guarded_deletes: Vec<GuardedDelete>,
}

/// Where the planner finds a body for an atom that is new in the scope.
pub(super) struct BodySources<'a> {
    pub bodies: &'a [Atom],
    pub source: Option<Option<&'a str>>,
}

/// The keep-small debit of one removed tip: its storage key, its stored row
/// length, and its atom's size when the commit knew it.
pub(crate) type TipDebit = (
    String,
    u64,
    Option<crate::db_operations::MoleculeTipCounterSource>,
);

/// The tip a changed key held before the run, read inside the write gate.
#[derive(Debug, Clone)]
pub(crate) struct PreviousTip {
    /// Length of the stored tip record, as the keep-small meters count rows.
    pub row_bytes: u64,
    pub atom_uuid: String,
}

/// What a planner run did, for reports and meters.
#[derive(Debug, Default)]
pub(crate) struct TipCommit {
    /// Bodies that this run wrote, for the keep-small meters.
    pub created: Vec<Atom>,
    /// Atoms whose count reached zero. The run deleted their bodies.
    pub deleted: Vec<DeletedAtom>,
    /// History rows of conflict records this run released.
    pub records_released: u64,
    /// Counter faults: an underflow, an absent counter, or a dangling removal.
    pub counter_events: u64,
    /// Old tips of changed keys, for the keep-small replacement debit.
    pub previous: HashMap<String, PreviousTip>,
    /// The size of each atom whose count changed, from its counter or from
    /// the body this run created. An atom whose counter has no size is absent.
    pub atom_sizes: HashMap<String, AtomSize>,
    /// Each molecule header row the run wrote, with its old and new length,
    /// read under the scoped key.
    pub headers: Vec<super::molecule::HeaderChange>,
    /// Guarded deletes that found the expected bytes and deleted the key.
    pub guarded_deleted: u64,
}

impl TipCommit {
    /// Fold the result of a later planner call of the same request into this one.
    pub(crate) fn absorb(&mut self, other: Self) {
        self.created.extend(other.created);
        self.deleted.extend(other.deleted);
        self.records_released += other.records_released;
        self.counter_events += other.counter_events;
        self.previous.extend(other.previous);
        self.atom_sizes.extend(other.atom_sizes);
        self.headers.extend(other.headers);
        self.guarded_deleted += other.guarded_deleted;
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
            history,
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
        let recorded = self
            .prepare_atom_history(history, scope, &tips, &old)
            .await?;
        let mut records = records;
        records.extend(recorded.refs);
        let records = dedupe_records(records);
        let mut rows = rows;
        rows.extend(recorded.rows);
        let new_records = self.read_new_records(&records).await?;
        let released = self.read_released_records(releases).await?;
        let file_deltas = released.file_deltas(&records, &new_records);
        self.note_touched_atoms(&tips, &old, (&records, &new_records));
        self.note_released_atoms(&released);
        let mut deltas = ref_deltas(&tips, &old, &records, &new_records);
        released.lower(&mut deltas);
        let mut changes = molecule_changes(&tips, &old, &records, &new_records);
        released.lower_molecules(&mut changes);
        let rows = with_new_record_rows(rows, &records, &new_records);
        let records_released = released.count();
        let mut deletes = deletes;
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
        let (rows, molecule_run) = measure(
            "molecule_counters_us",
            self.apply_molecule_counters(scope, &changes, rows),
        )
        .await?;
        let previous = previous_tips(&old);
        let atom_sizes = atom_sizes(&before, &offers);
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
        commit.previous = previous;
        commit.atom_sizes = atom_sizes;
        commit.headers = molecule_run.headers;
        commit.records_released = records_released;
        commit.guarded_deleted = guarded_deleted;
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
        // A molecule changes with its tips and its records. The header checks
        // treat a difference for such a molecule as a candidate.
        let molecules = tips
            .keys()
            .filter_map(|key| molecule_key_codec::molecule_uuid_from_storage_key(key))
            .chain(
                records
                    .iter()
                    .zip(new_records)
                    .filter(|(_, is_new)| **is_new)
                    .map(|(record, _)| record.molecule.as_str()),
            );
        watch.note(molecules);
    }

    /// A native store commits one batch only inside a request scope.
    pub(in crate::db_operations::atom_store) fn require_atomic_scope(
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

/// The old tip of each changed key.
fn previous_tips(old: &HashMap<String, PerKeyRecord>) -> HashMap<String, PreviousTip> {
    old.iter()
        .map(|(key, record)| {
            let row_bytes =
                crate::db_operations::keep_small::molecule_counter_row_bytes(record).unwrap_or(0);
            (
                key.clone(),
                PreviousTip {
                    row_bytes,
                    atom_uuid: record.entry.atom_uuid.clone(),
                },
            )
        })
        .collect()
}

/// The known size of each atom whose count changes: the body this run
/// creates, else the size its counter holds.
fn atom_sizes(
    before: &HashMap<String, AtomBefore>,
    offers: &HashMap<String, super::read::Offer>,
) -> HashMap<String, AtomSize> {
    let mut sizes: HashMap<String, AtomSize> = before
        .iter()
        .filter_map(|(atom, state)| Some((atom.clone(), state.counter.as_ref()?.size?)))
        .collect();
    for (atom, offer) in offers {
        if let Some(size) = offer.size {
            sizes.insert(atom.clone(), size);
        }
    }
    sizes
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
