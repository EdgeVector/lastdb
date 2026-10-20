//! The writes of the recount: counters, and the bodies of atoms nothing names.
//!
//! This is the only code outside the planner that writes counters and bodies on
//! purpose. It runs on a stopped home, so no writer and no gate exist; each
//! batch is atomic, and the recount is safe to run again after a stop.

use super::decide::Decision;
use super::report::{Change, RecountReport};
use crate::atom::atom_key_codec;
use crate::db_operations::atom_store::counter::scan::PairRead;
use crate::db_operations::atom_store::counter::AtomCounter;
use crate::db_operations::atom_store::{AtomStore, BlobRefEdge};
use crate::schema::types::field::build_storage_key;
use crate::schema::SchemaError;
use crate::storage::KvMutation;

/// Operations in one batch.
const BATCH_OPERATIONS: usize = 400;
/// The most changes the report lists.
const LISTED_CHANGES: usize = 50;

/// The mutations of one batch, and the scope they belong to.
pub(super) struct Pending<'a> {
    pub scope: Option<&'a str>,
    mutations: Vec<KvMutation>,
}

impl<'a> Pending<'a> {
    pub(super) fn new(scope: Option<&'a str>) -> Self {
        Self {
            scope,
            mutations: Vec::new(),
        }
    }

    pub(super) fn is_full(&self) -> bool {
        self.mutations.len() >= BATCH_OPERATIONS
    }
}

fn to_bytes<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, SchemaError> {
    serde_json::to_vec(value)
        .map_err(|error| SchemaError::InvalidData(format!("serialize recount record: {error}")))
}

impl AtomStore {
    /// The blob references of the body of an atom, read from the body.
    async fn blobs_of_body(
        &self,
        scope: Option<&str>,
        atom: &str,
    ) -> Result<Vec<String>, SchemaError> {
        let key = build_storage_key(scope, &atom_key_codec::body_key(atom));
        let body = self
            .get_atoms_by_storage_keys(&[key])
            .await?
            .pop()
            .flatten();
        let body = body.ok_or_else(|| {
            SchemaError::InvalidData(format!(
                "atom {atom} has a body record that does not decode"
            ))
        })?;
        Ok(crate::atom::file_pointer::blob_refs_of_atom(
            body.content(),
            body.metadata(),
        ))
    }

    /// Turn one decision into the mutations of the batch, and count it.
    pub(super) async fn plan_decision(
        &self,
        pending: &mut Pending<'_>,
        pair: &PairRead,
        decision: Decision,
        report: &mut RecountReport,
    ) -> Result<(), SchemaError> {
        let scope = pending.scope;
        let counter_key = build_storage_key(scope, &atom_key_codec::counter_key(&pair.atom));
        let change = |action: &str, before: Option<u64>, after: Option<u64>| Change {
            scope: scope.map(str::to_string),
            atom: pair.atom.clone(),
            action: action.to_string(),
            before,
            after,
        };
        match decision {
            Decision::Keep => report.unchanged += 1,
            Decision::Nothing => {}
            Decision::LostBody => report.lost_bodies += 1,
            Decision::KeepOrphan => report.notes.push(format!(
                "kept the body of {} (a row did not decode)",
                pair.atom
            )),
            Decision::Write {
                refs,
                blobs,
                why,
                before,
            } => {
                let from_body = blobs.is_none();
                let blobs = match blobs {
                    Some(blobs) => blobs,
                    None => self.blobs_of_body(scope, &pair.atom).await?,
                };
                if from_body {
                    for blob in &blobs {
                        for (key, value) in BlobRefEdge::atom(&pair.atom, blob, scope).put_rows()? {
                            pending.mutations.push(KvMutation::put(key, value));
                        }
                    }
                }
                let counter = AtomCounter::new(refs, blobs);
                pending.mutations.push(KvMutation::put(
                    counter_key.into_bytes(),
                    to_bytes(&counter)?,
                ));
                note_change(report, change(why.action(), before, Some(refs)));
                count_why(report, why);
            }
            Decision::DeleteCounter { before } => {
                pending
                    .mutations
                    .push(KvMutation::delete(counter_key.into_bytes()));
                report.counters_deleted += 1;
                note_change(report, change("delete_counter", before, None));
            }
            Decision::DeleteAtom { blobs, before } => {
                let blobs = match blobs {
                    Some(blobs) => blobs,
                    None => self.blobs_of_body(scope, &pair.atom).await?,
                };
                let body_key = build_storage_key(scope, &atom_key_codec::body_key(&pair.atom));
                pending
                    .mutations
                    .push(KvMutation::delete(counter_key.into_bytes()));
                pending
                    .mutations
                    .push(KvMutation::delete(body_key.into_bytes()));
                for blob in &blobs {
                    pending
                        .mutations
                        .extend(BlobRefEdge::atom(&pair.atom, blob, scope).delete_mutations());
                }
                report.bodies_deleted += 1;
                note_change(report, change("delete_atom", before, None));
            }
        }
        Ok(())
    }

    /// Write the batch (when the recount executes) and start a new one.
    pub(super) async fn flush_pending(
        &self,
        pending: &mut Pending<'_>,
        execute: bool,
        report: &mut RecountReport,
    ) -> Result<(), SchemaError> {
        let mutations = std::mem::take(&mut pending.mutations);
        if mutations.is_empty() {
            return Ok(());
        }
        report.operations += mutations.len() as u64;
        report.batches += 1;
        if execute {
            self.raw()
                .inner()
                .batch_mutate(mutations)
                .await
                .map_err(SchemaError::from)?;
        }
        Ok(())
    }
}

fn note_change(report: &mut RecountReport, change: Change) {
    if report.changes.len() < LISTED_CHANGES {
        report.changes.push(change);
    }
}

fn count_why(report: &mut RecountReport, why: super::decide::Why) {
    use super::decide::Why;
    match why {
        Why::Create => report.created += 1,
        Why::Raise => report.raised += 1,
        Why::Lower => report.lowered += 1,
        Why::ClearSuspect => report.suspect_cleared += 1,
        Why::Rewrite => report.rewritten += 1,
    }
}
