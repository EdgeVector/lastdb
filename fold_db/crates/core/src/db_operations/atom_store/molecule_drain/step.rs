//! One drain step: read a page outside the gate, then one batch inside it.
//!
//! Every batch is one native write scope. Inside the gate the batch reads the
//! header, the `bindings` counter and the binding records of its molecule again,
//! and decides from those reads (`judge.rs`), not from what the page read saw. A
//! writer that binds the molecule or writes a tip between the page and the batch
//! is seen by the batch. The batch then stops and deletes nothing, or deletes
//! only the candidate row of a molecule that is bound again.
//!
//! Order of a molecule, and why a crash at any point resumes:
//!
//! 1. **Tips.** A page of `mk:{M}:` keys goes through the planner: the tip
//!    deletes, the atom counters and the delete of a body at zero, `slots`, and the
//!    kept conflict records of the slots, all in one batch. A crash keeps whole
//!    batches, so the next walk finds the keys that are left.
//! 2. **Rows.** Pages of the rows that carry no count (generation markers,
//!    generation records, delete barriers) are deleted. The batch needs `slots`
//!    and `records` at zero in the gate.
//! 3. **Finish.** One batch deletes the generation pointer, the conflict id list,
//!    the header and the candidate row. The header goes in the last batch, so a
//!    molecule that a crash leaves half removed still has its header and its
//!    zero counters. It reads as dead and the candidate row is still there, so
//!    the next wake finishes it.

use super::candidate::Candidate;
use super::cursor::{row_walk, tip_walk, Active, DrainCursor, ParkWhy, Phase};
use super::family::{closing_keys, row_prefixes};
use super::judge::{judge, Refusal, Verdict};
use crate::atom::molecule_key_codec::decode_hash_range;
use crate::db_operations::atom_store::counter::{TipChange, TipChangeSet, TipCommit};
use crate::db_operations::atom_store::{AtomStore, RangeWalk};
use crate::schema::types::field::build_storage_key;
use crate::schema::SchemaError;

/// The most tip records one batch erases, whatever the knob says. A tip costs at
/// most three operations (the tip, the counter, the body), and an older home
/// takes 4,096 operations in a batch.
pub(crate) const MAX_TIPS_PER_BATCH: usize = 1_024;

/// The size of one batch.
#[derive(Debug, Clone, Copy)]
pub(crate) struct DrainLimits {
    pub tips: usize,
    pub rows: usize,
}

impl DrainLimits {
    pub(crate) fn new(tips: usize) -> Self {
        let tips = tips.clamp(1, MAX_TIPS_PER_BATCH);
        Self {
            tips,
            rows: (tips * 4).min(2_048),
        }
    }
}

/// What one step did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum OutcomeKind {
    /// A batch erased tip records.
    Erased { tips: u64, atoms_deleted: u64 },
    /// A batch deleted rows that carry no count.
    Removed { rows: u64 },
    /// The final batch removed the molecule.
    Finished,
    /// The molecule is bound again. The candidate row is gone.
    Rebound,
    /// The molecule has no header, no counter and no record. The candidate row is
    /// gone.
    Absent,
    /// `slots` is zero and `records` is above zero: kept conflict records still
    /// name the molecule.
    Waiting,
    /// A counter or a record keeps the molecule from a drain.
    Refused(Refusal),
}

/// One finished step of one molecule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DrainOutcome {
    pub candidate: Candidate,
    pub kind: OutcomeKind,
}

/// What the gate decided.
enum Gate {
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
enum Need {
    /// The molecule is dead: nothing names it. It may hold tips.
    Keys,
    /// The molecule is dead and holds no tip and no kept record.
    Family,
}

/// A batch that ran, or the reason it did not.
enum Batch<T> {
    Ran(T),
    Stopped(Gate),
}

/// The next step of the molecule in work.
enum Move {
    Batch(OutcomeKind),
    Continue,
    Done(OutcomeKind),
}

impl AtomStore {
    /// Run one step: one batch of the molecule in work, or of the next candidate.
    /// `None` when no candidate is left to work on in this pass of the list.
    pub(crate) async fn molecule_drain_step(
        &self,
        cursor: &mut DrainCursor,
        limits: DrainLimits,
    ) -> Result<Option<DrainOutcome>, SchemaError> {
        loop {
            if cursor.active.is_none() && !self.take_next_candidate(cursor, limits).await? {
                return Ok(None);
            }
            let Some(mut active) = cursor.active.take() else {
                continue;
            };
            let candidate = active.candidate.clone();
            match self.step_active(&mut active, limits).await {
                Ok(Move::Continue) => cursor.active = Some(active),
                Ok(Move::Batch(kind)) => {
                    cursor.active = Some(active);
                    return Ok(Some(DrainOutcome { candidate, kind }));
                }
                Ok(Move::Done(kind)) => {
                    match &kind {
                        OutcomeKind::Waiting => cursor.park(candidate.clone(), ParkWhy::Records),
                        OutcomeKind::Refused(why) => {
                            cursor.park(candidate.clone(), ParkWhy::Refused(*why));
                        }
                        _ => cursor.unpark(&candidate),
                    }
                    return Ok(Some(DrainOutcome { candidate, kind }));
                }
                Err(error) => {
                    cursor.rotate(candidate);
                    return Err(error);
                }
            }
        }
    }

    /// Make the next candidate of the list the molecule in work. Reads pages of
    /// the list until it finds one that is not parked.
    async fn take_next_candidate(
        &self,
        cursor: &mut DrainCursor,
        limits: DrainLimits,
    ) -> Result<bool, SchemaError> {
        loop {
            if let Some(candidate) = cursor.pop() {
                cursor.active = Some(Active::new(candidate, limits.tips));
                return Ok(true);
            }
            if cursor.list_done() {
                return Ok(false);
            }
            let step = self.walk_step(cursor.list_walk()).await?;
            for (key, _) in step.rows {
                if let Some(candidate) = Candidate::parse(&key) {
                    cursor.offer(candidate);
                }
            }
        }
    }

    async fn step_active(
        &self,
        active: &mut Active,
        limits: DrainLimits,
    ) -> Result<Move, SchemaError> {
        let candidate = active.candidate.clone();
        match &mut active.phase {
            Phase::Tips(walk) => {
                let keys = self.next_keys(walk, limits.tips).await?;
                if keys.is_empty() {
                    active.phase = rows_phase(&candidate, 0, limits);
                    return Ok(Move::Continue);
                }
                match self.tips_batch(&candidate, &keys).await? {
                    Batch::Ran(commit) => {
                        let tips = commit.previous.len() as u64;
                        let atoms_deleted = commit.deleted.len() as u64;
                        self.drain_state().note_batch(tips, 0, atoms_deleted);
                        Ok(Move::Batch(OutcomeKind::Erased {
                            tips,
                            atoms_deleted,
                        }))
                    }
                    Batch::Stopped(gate) => Ok(self.stopped(gate)),
                }
            }
            Phase::Rows { index, walk } => {
                let keys = self.next_keys(walk, limits.rows).await?;
                if keys.is_empty() {
                    let next = *index + 1;
                    active.phase = if next < row_prefixes(&candidate).len() {
                        rows_phase(&candidate, next, limits)
                    } else {
                        Phase::Finish
                    };
                    return Ok(Move::Continue);
                }
                let rows = keys.len() as u64;
                match self.delete_rows_batch(&candidate, keys).await? {
                    Batch::Ran(()) => {
                        self.drain_state().note_batch(0, rows, 0);
                        Ok(Move::Batch(OutcomeKind::Removed { rows }))
                    }
                    Batch::Stopped(Gate::BackToKeys) => {
                        active.phase = Phase::Tips(tip_walk(&candidate, limits.tips));
                        Ok(Move::Continue)
                    }
                    Batch::Stopped(gate) => Ok(self.stopped(gate)),
                }
            }
            Phase::Finish => match self.finish_batch(&candidate).await? {
                Batch::Ran(()) => {
                    self.drain_state()
                        .note_batch(0, closing_keys(&candidate).len() as u64, 0);
                    self.drain_state().note_drained();
                    Ok(Move::Done(OutcomeKind::Finished))
                }
                Batch::Stopped(Gate::BackToKeys) => {
                    active.phase = Phase::Tips(tip_walk(&candidate, limits.tips));
                    Ok(Move::Continue)
                }
                Batch::Stopped(gate) => Ok(self.stopped(gate)),
            },
        }
    }

    /// The step that ends the molecule in work without a batch of removal.
    fn stopped(&self, gate: Gate) -> Move {
        match gate {
            Gate::Rebound => {
                self.drain_state().note_rebound();
                Move::Done(OutcomeKind::Rebound)
            }
            Gate::Absent => Move::Done(OutcomeKind::Absent),
            Gate::Waiting => Move::Done(OutcomeKind::Waiting),
            Gate::Refused(why) => Move::Done(OutcomeKind::Refused(why)),
            Gate::Go | Gate::BackToKeys => Move::Continue,
        }
    }

    /// The next page of keys of a walk, up to `want`. Empty only when the walk is
    /// done. A span that holds only deleted keys is passed inside the walk.
    async fn next_keys(
        &self,
        walk: &mut RangeWalk,
        want: usize,
    ) -> Result<Vec<Vec<u8>>, SchemaError> {
        let mut keys = Vec::new();
        while keys.len() < want && !walk.done() {
            walk.set_limit(want - keys.len());
            let step = self.walk_step(walk).await?;
            keys.extend(step.rows.into_iter().map(|(key, _)| key));
        }
        Ok(keys)
    }

    /// Decide inside the write gate, from the reads of this gate hold. A molecule
    /// that is bound again, or that has nothing left, loses its candidate row in
    /// this same hold.
    async fn decide(&self, candidate: &Candidate, need: Need) -> Result<Gate, SchemaError> {
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

    async fn drop_candidate_row(&self, candidate: &Candidate) -> Result<(), SchemaError> {
        self.commit_tip_changes(TipChangeSet {
            deletes: vec![candidate.key().into_bytes()],
            ..TipChangeSet::default()
        })
        .await
        .map(|_| ())
    }

    /// Erase a page of tip records. The planner lowers the atom counters and
    /// deletes a body at zero, in this same batch.
    async fn tips_batch(
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
    async fn delete_rows_batch(
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
    async fn finish_batch(&self, candidate: &Candidate) -> Result<Batch<()>, SchemaError> {
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

fn rows_phase(candidate: &Candidate, index: usize, limits: DrainLimits) -> Phase {
    match row_walk(candidate, index, limits.rows) {
        Some(walk) => Phase::Rows { index, walk },
        None => Phase::Finish,
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
