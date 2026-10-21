//! The batches of the drain: the decision inside the gate, and the three writes.
//!
//! Each batch is one native write scope. It reads the molecule again inside the
//! gate (`decide`) before it deletes anything. The deletes go through the planner
//! (`commit_tip_changes`): this file holds no write verb of its own.

use super::candidate::Candidate;
use super::family::closing_keys;
use super::judge::{judge, Refusal, Verdict};
use crate::atom::molecule_key_codec::decode_hash_range;
use crate::db_operations::atom_store::counter::{TipChange, TipChangeSet, TipCommit};
use crate::db_operations::atom_store::AtomStore;
use crate::schema::types::field::build_storage_key;
use crate::schema::SchemaError;

/// What the gate decided.
pub(super) enum Gate {
    Go,
    Rebound,
    Absent,
    Waiting,
    Refused(Refusal),
    /// A tip appeared after the key phase: erase keys again.
    BackToKeys,
}

/// What a batch needs from the molecule.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Need {
    /// The molecule is dead: nothing names it. It may hold tips.
    Keys,
    /// The molecule is dead and holds no tip and no kept record.
    Family,
}

/// A batch that ran, or the reason it did not.
pub(super) enum Batch<T> {
    Ran(T),
    Stopped(Gate),
}

impl AtomStore {
    /// Decide inside the write gate, from the reads of this gate hold. A molecule
    /// that is bound again, or that has nothing left, loses its candidate row in
    /// this same hold.
    pub(super) async fn decide(
        &self,
        candidate: &Candidate,
        need: Need,
    ) -> Result<Gate, SchemaError> {
        let verdict = judge(&self.read_molecule_reads(candidate).await?);
        match verdict {
            Verdict::Bound => {
                self.drop_candidate_row(candidate).await?;
                Ok(Gate::Rebound)
            }
            Verdict::Absent => {
                self.drop_candidate_row(candidate).await?;
                Ok(Gate::Absent)
            }
            Verdict::Refused(why) => Ok(Gate::Refused(why)),
            Verdict::Dead { .. } if need == Need::Keys => Ok(Gate::Go),
            Verdict::Dead { slots, records } => match (slots, records) {
                (0, 0) => Ok(Gate::Go),
                (0, _) => Ok(Gate::Waiting),
                _ => Ok(Gate::BackToKeys),
            },
        }
    }

    pub(super) async fn drop_candidate_row(
        &self,
        candidate: &Candidate,
    ) -> Result<(), SchemaError> {
        self.commit_tip_changes(TipChangeSet {
            deletes: vec![candidate.key().into_bytes()],
            ..TipChangeSet::default()
        })
        .await
        .map(|_| ())
    }

    /// Erase a page of tip records. The planner lowers the atom counters and
    /// deletes a body at zero, in this same batch.
    pub(super) async fn tips_batch(
        &self,
        candidate: &Candidate,
        keys: &[Vec<u8>],
    ) -> Result<Batch<TipCommit>, SchemaError> {
        let scope = candidate.scope.as_deref();
        let slots = decode_slots(candidate, keys);
        let tips = keys
            .iter()
            .map(|key| {
                String::from_utf8(key.clone())
                    .map(|key| TipChange { key, new: None })
                    .map_err(|_| SchemaError::InvalidData("tip key is not UTF-8".into()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.in_background_scope(async {
            match self.decide(candidate, Need::Keys).await? {
                Gate::Go => {}
                gate => return Ok(Batch::Stopped(gate)),
            }
            let releases = self
                .slot_release_requests(scope, &[(candidate.molecule.as_str(), slots.as_slice())])
                .await?;
            let commit = self
                .commit_tip_changes(TipChangeSet {
                    scope,
                    tips,
                    rows: releases.rows,
                    releases: releases.releases,
                    ..TipChangeSet::default()
                })
                .await?;
            Ok(Batch::Ran(commit))
        })
        .await
    }

    /// Delete a page of rows that carry no count.
    pub(super) async fn delete_rows_batch(
        &self,
        candidate: &Candidate,
        keys: Vec<Vec<u8>>,
    ) -> Result<Batch<()>, SchemaError> {
        self.in_background_scope(async {
            match self.decide(candidate, Need::Family).await? {
                Gate::Go => {}
                gate => return Ok(Batch::Stopped(gate)),
            }
            self.commit_tip_changes(TipChangeSet {
                scope: candidate.scope.as_deref(),
                deletes: keys,
                ..TipChangeSet::default()
            })
            .await?;
            Ok(Batch::Ran(()))
        })
        .await
    }

    /// The final batch: the pointer, the conflict id list, the header and the
    /// candidate row.
    pub(super) async fn finish_batch(
        &self,
        candidate: &Candidate,
    ) -> Result<Batch<()>, SchemaError> {
        self.in_background_scope(async {
            match self.decide(candidate, Need::Family).await? {
                Gate::Go => {}
                gate => return Ok(Batch::Stopped(gate)),
            }
            self.commit_tip_changes(TipChangeSet {
                scope: candidate.scope.as_deref(),
                deletes: closing_keys(candidate),
                ..TipChangeSet::default()
            })
            .await?;
            Ok(Batch::Ran(()))
        })
        .await
    }
}

/// The storage-form `(hash, range)` of each tip key, for the release of the kept
/// conflict records of the slots. A key of another shape has no slot to release.
fn decode_slots(candidate: &Candidate, keys: &[Vec<u8>]) -> Vec<(String, String)> {
    let scope_prefix = build_storage_key(candidate.scope.as_deref(), "");
    keys.iter()
        .filter_map(|key| {
            let text = std::str::from_utf8(key).ok()?;
            let base = text.strip_prefix(&scope_prefix)?;
            decode_hash_range(base, &candidate.molecule)
        })
        .collect()
}
