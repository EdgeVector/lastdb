//! Re-arm of old atom GC candidates when a write revives their atom.

use super::{
    atom_gc_candidate_key, candidate_delete_mutations, candidate_put_mutations, new_candidate,
    AtomGcCandidate, AtomStore,
};
use crate::clock::unix_nanos;
use crate::schema::SchemaError;
use crate::storage::KvMutation;
use std::sync::atomic::Ordering;

impl AtomStore {
    pub(in crate::db_operations::atom_store) async fn atom_gc_candidate_mutations_for_count(
        &self,
        atom_uuid: &str,
        before: u64,
        after: u64,
        storage_prefix: Option<&str>,
    ) -> Result<Vec<KvMutation>, SchemaError> {
        let existing = self
            .load_atom_gc_candidate(atom_uuid, storage_prefix)
            .await?;
        Self::atom_gc_candidate_mutations_for_loaded_count(
            atom_uuid,
            before,
            after,
            existing,
            storage_prefix,
        )
    }

    /// Plan from the exact candidate already loaded under the caller's count lock.
    pub(in crate::db_operations::atom_store) fn atom_gc_candidate_mutations_for_loaded_count(
        atom_uuid: &str,
        before: u64,
        after: u64,
        existing: Option<AtomGcCandidate>,
        storage_prefix: Option<&str>,
    ) -> Result<Vec<KvMutation>, SchemaError> {
        if after == 0 {
            return match existing {
                None => candidate_put_mutations(
                    &new_candidate(atom_uuid, storage_prefix, unix_nanos()),
                    storage_prefix,
                ),
                // The count was positive, yet a candidate survived. A lost
                // cancel left it. Its old epoch and audit must not count, so
                // start a new epoch.
                Some(old) if before > 0 => rearm_candidate_mutations(
                    atom_uuid,
                    storage_prefix,
                    Some(old.zero_since_unix_nanos),
                    unix_nanos(),
                ),
                Some(_) => Ok(Vec::new()),
            };
        }
        Ok(existing.map_or_else(Vec::new, |candidate| {
            candidate_delete_mutations(&candidate, storage_prefix)
        }))
    }

    /// Mutations that start a new epoch for each named atom whose candidate
    /// could reap. The caller holds the automatic-GC and count locks of every
    /// named atom.
    ///
    /// A body write revives an atom. Between the atoms phase and the tip phase
    /// of one lane job no reference exists yet, so an old audited candidate
    /// could reap the body. A new epoch has no audit and a fresh grace window.
    pub(in crate::db_operations::atom_store) async fn rearm_atom_gc_candidate_mutations(
        &self,
        atom_uuids: &[String],
        storage_prefix: Option<&str>,
    ) -> Result<Vec<KvMutation>, SchemaError> {
        // The reaper lists only unprefixed candidates.
        if storage_prefix.is_some() || atom_uuids.is_empty() {
            return Ok(Vec::new());
        }
        let mut unique = atom_uuids.to_vec();
        unique.sort_unstable();
        unique.dedup();
        let counts = self
            .atom_live_ref_count_rows(&unique, storage_prefix)
            .await?;
        // A candidate can reap only while the count record is an exact zero.
        let zero: Vec<&String> = unique
            .iter()
            .zip(counts)
            .filter(|(_, count)| count.is_some_and(|c| c.exact && c.live_refs == 0))
            .map(|(uuid, _)| uuid)
            .collect();
        if zero.is_empty() {
            return Ok(Vec::new());
        }
        let keys: Vec<Vec<u8>> = zero
            .iter()
            .map(|uuid| atom_gc_candidate_key(uuid, storage_prefix).into_bytes())
            .collect();
        let stored = self.main_store.inner().get_many(keys).await.map_err(|e| {
            SchemaError::InvalidData(format!("load atom GC candidates for re-arm: {e}"))
        })?;
        let now = unix_nanos();
        let mut out = Vec::new();
        for (uuid, raw) in zero.into_iter().zip(stored) {
            let Some(raw) = raw else { continue };
            // A record that does not decode counts as present.
            let old = serde_json::from_slice::<AtomGcCandidate>(&raw)
                .ok()
                .map(|candidate| candidate.zero_since_unix_nanos);
            out.extend(rearm_candidate_mutations(uuid, storage_prefix, old, now)?);
        }
        Ok(out)
    }

    /// Re-arm before a tip write that stores no body in the same job (a raw
    /// legacy tip replayed from an old cloud log). Takes its own locks, in the
    /// lane order: automatic GC first, count second. Call it before any tip
    /// lock, because the lane tip path takes the automatic-GC lock first.
    pub(crate) async fn rearm_atom_gc_candidates_before_tip(
        &self,
        atom_uuids: &[String],
        storage_prefix: Option<&str>,
    ) -> Result<u64, SchemaError> {
        if storage_prefix.is_some() || atom_uuids.is_empty() {
            return Ok(0);
        }
        let _body_guards = self.lock_automatic_gc_atoms(atom_uuids).await;
        let _count_guards = self.lock_atom_ref_counts(atom_uuids).await;
        let mutations = self
            .rearm_atom_gc_candidate_mutations(atom_uuids, storage_prefix)
            .await?;
        if mutations.is_empty() {
            return Ok(0);
        }
        let rearmed = (mutations.len() / 2) as u64;
        self.main_store
            .inner()
            .batch_mutate(mutations)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!("re-arm atom GC candidates: {error}"))
            })?;
        self.note_atom_gc_candidates_rearmed(rearmed);
        Ok(rearmed)
    }

    pub(in crate::db_operations::atom_store) fn note_atom_gc_candidates_rearmed(&self, count: u64) {
        self.atom_gc_candidates_rearmed
            .fetch_add(count, Ordering::Relaxed);
        tracing::info!(target: "fold_db::gc_atoms", rearmed = count, "re-armed atom GC candidates");
    }

    /// Candidates re-armed by body writes since the store opened.
    #[must_use]
    pub fn atom_gc_candidates_rearmed_total(&self) -> u64 {
        self.atom_gc_candidates_rearmed.load(Ordering::Relaxed)
    }
}

/// Two puts: the candidate record and a new queue record. The old queue
/// record stays. The reaper deletes it as a stale record (epoch mismatch).
pub(super) fn rearm_candidate_mutations(
    atom_uuid: &str,
    storage_prefix: Option<&str>,
    old_epoch: Option<u64>,
    now: u64,
) -> Result<Vec<KvMutation>, SchemaError> {
    // Strictly after the old epoch, also when the wall clock steps back.
    let epoch = old_epoch.map_or(now, |old| now.max(old.saturating_add(1)));
    candidate_put_mutations(
        &new_candidate(atom_uuid, storage_prefix, epoch),
        storage_prefix,
    )
}
