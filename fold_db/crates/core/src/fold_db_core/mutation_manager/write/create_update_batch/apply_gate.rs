//! Apply gate: lock, revalidate, settle delete barriers, publish resident tips.

use super::super::super::molecules::PreparedSchemaDelta;
use super::*;

/// How many times the apply gate re-prepares after a concurrent writer moves a
/// slot revision before the batch gives up.
const APPLY_GATE_RETRIES: usize = 64;

type PersistCompletion = (
    String,
    tokio::sync::oneshot::Receiver<crate::request_phases::RequestCounts>,
);

/// A protein sibling tip that must not lose to a newer Delete:
/// (delta index, sibling index, changed key, final storage key, entry).
type SiblingCandidate = (
    usize,
    usize,
    crate::db_operations::ChangedKey,
    String,
    crate::atom::AtomEntry,
);

impl MutationManager {
    /// Apply gate: lock molecules, validate revisions, publish resident tips.
    /// Returns the per-schema persist completions to await.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn apply_prepared_deltas(
        &self,
        prepared_deltas: &mut [PreparedSchemaDelta],
        storage_prefix: Option<&str>,
        require_durable: bool,
        author_clock_barrier: Option<super::super::super::author_clock::AuthorClockPersistBarrier>,
        timing_breakdown: &mut HashMap<&str, std::time::Duration>,
        molecule_lock_wait: &mut std::time::Duration,
        stages: &mut ResidentCommitStages,
    ) -> Result<Vec<PersistCompletion>, SchemaError> {
        for _ in 0..APPLY_GATE_RETRIES {
            self.recheck_delta_preconditions(prepared_deltas, storage_prefix)
                .await?;
            let molecule_lock_start = std::time::Instant::now();
            let molecule_write_guards = self
                .acquire_molecule_write_lock_keys(Self::delta_molecule_lock_keys(prepared_deltas))
                .await;
            let molecule_lock_elapsed = molecule_lock_start.elapsed();
            *molecule_lock_wait += molecule_lock_elapsed;
            Self::add_timing(
                timing_breakdown,
                "  - molecule_write_locks",
                molecule_lock_elapsed,
            );
            if prepared_deltas.iter().any(|delta| {
                !self.prepared_revisions_match(&delta.prepared)
                    || !self.prepared_revisions_match(&delta.sibling_prepared)
            }) {
                drop(molecule_write_guards);
                // A concurrent same-key writer moved these slot revisions out
                // from under this batch's prepare snapshot. Reseed from fresh
                // resident state and retry — the next loop iteration's CAS
                // recheck above re-validates any surviving CAS mutation
                // against that fresh state before this attempts to apply again.
                self.reseed_stale_deltas(prepared_deltas).await?;
                continue;
            }
            let (candidates, sibling_candidates) =
                self.collect_delete_barrier_candidates(prepared_deltas, storage_prefix)?;
            let tip_guards = self
                .drop_deletes_that_win(
                    prepared_deltas,
                    &candidates,
                    &sibling_candidates,
                    require_durable,
                )
                .await?;
            let phase2_start = std::time::Instant::now();
            let outcomes = self.publish_prepared_resident_deltas(prepared_deltas);
            // Resident tips are visible from this point. Capture must stage
            // even if persist-lane wait, flush, or mixed erase later returns
            // Err — otherwise RAM shows a commit the mutation log never saw.
            crate::sync::capture::mark_logical_commit_published();
            let mut persist_completions = Vec::new();
            for (delta, outcome) in prepared_deltas.iter_mut().zip(outcomes) {
                if let Some(completion) = self.finalize_prepared_schema_delta(
                    delta,
                    &outcome,
                    timing_breakdown,
                    author_clock_barrier.clone(),
                ) {
                    persist_completions.push((delta.schema_name.clone(), completion));
                }
            }
            let publish_elapsed = phase2_start.elapsed();
            stages.publish_us = crate::request_phases::duration_us(publish_elapsed);
            Self::add_timing(
                timing_breakdown,
                "  - update_memory_serial",
                publish_elapsed,
            );
            drop(tip_guards);
            drop(molecule_write_guards);
            return Ok(persist_completions);
        }
        Err(SchemaError::InvalidData(
            "apply-gate revision retries exceeded for multi-schema batch".into(),
        ))
    }

    /// Re-validate every surviving CAS precondition against the live head on
    /// EVERY attempt, not only after a detected revision mismatch. The
    /// batch-level check ran once, before this schema group was even prepared;
    /// a concurrent same-key writer — CAS or plain — can move the head in the
    /// gap between that check and this attempt without ever tripping the
    /// revision compare (the very next prepare simply adopts the moved state
    /// as its new baseline). Re-checking before every lock acquisition is what
    /// actually closes that gap. This is a no-op for a batch with no CAS
    /// mutations (`check_cas_preconditions` skips any mutation without
    /// `expected`), so it costs nothing on the plain-write hot path. See card
    /// fold-cas-lock-scope-excludes-non-cas-same-key-writers-20260906.
    pub(super) async fn recheck_delta_preconditions(
        &self,
        prepared_deltas: &[PreparedSchemaDelta],
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        for delta in prepared_deltas {
            self.check_cas_preconditions(&delta.schema_mutations, storage_prefix)
                .await?;
            // Same gap, same close: a concurrent delete landing between the
            // batch-level gate and this attempt would otherwise let a
            // `must_exist` update mint the phantom row it was asked to
            // refuse. Skipped for every mutation without the flag, so the
            // plain-write path pays one predicate check.
            self.check_must_exist_preconditions(&delta.schema_mutations, storage_prefix)
                .await?;
        }
        Ok(())
    }

    pub(super) fn delta_molecule_lock_keys(prepared_deltas: &[PreparedSchemaDelta]) -> Vec<String> {
        let mut lock_keys = Vec::new();
        for delta in prepared_deltas {
            lock_keys.extend(Self::molecule_lock_keys_for(
                &delta.schema,
                &delta.changed_keys,
            ));
            lock_keys.extend(Self::sibling_molecule_lock_keys(&delta.sibling_updates));
        }
        lock_keys
    }

    pub(super) async fn reseed_stale_deltas(
        &self,
        prepared_deltas: &mut [PreparedSchemaDelta],
    ) -> Result<(), SchemaError> {
        for delta in prepared_deltas {
            self.reseed_changed_from_resident(&mut delta.schema, &delta.changed_keys)?;
            delta.prepared = self.prepared_slot_revisions(&delta.schema, &delta.changed_keys);
            let sibling_updates = self
                .build_protein_sibling_updates(
                    &delta.schema,
                    &delta.schema_mutations,
                    &delta.mutation_key_values,
                    &delta.atom_results,
                )
                .await?;
            // Capture before the resident rebase. A sibling change
            // during either step invalidates this preparation under
            // the gates on the next attempt.
            delta.sibling_prepared = self.prepared_sibling_slot_revisions(&sibling_updates);
            delta.sibling_updates =
                self.rebase_sibling_updates_on_resident(&delta.schema, sibling_updates);
        }
        Ok(())
    }

    /// A newer Delete can win while a Put waits to publish in memory. Request
    /// and replay Puts use the same exact key gate as Delete. Collects the
    /// entry barriers and the protein sibling tips that publication must check.
    pub(super) fn collect_delete_barrier_candidates(
        &self,
        prepared_deltas: &[PreparedSchemaDelta],
        storage_prefix: Option<&str>,
    ) -> Result<
        (
            Vec<crate::atom::delete_barrier::DeleteBarrier>,
            Vec<SiblingCandidate>,
        ),
        SchemaError,
    > {
        let mut candidates = Vec::new();
        let mut sibling_candidates = Vec::new();
        for (delta_index, delta) in prepared_deltas.iter().enumerate() {
            for (mutation, key_value) in delta
                .schema_mutations
                .iter()
                .zip(&delta.mutation_key_values)
            {
                let mut resolved = mutation.clone();
                resolved.key_value = key_value.clone();
                candidates.extend(
                    super::super::super::super::purge::plan_normal_delete_barriers(
                        &self.db_ops,
                        &delta.schema,
                        std::slice::from_ref(&resolved),
                    )?,
                );
            }
            // Protein updates publish tips under different molecule keys.
            // Build the same final keys as the sibling persist path.
            for (sibling_index, (molecule_uuid, data, changed)) in
                delta.sibling_updates.iter().enumerate()
            {
                for key in changed {
                    let hash = key.disk_hash();
                    let range = key.disk_range();
                    let Some(entry) = data.get_atom_entry(hash, range) else {
                        continue;
                    };
                    let storage_hash = self.db_ops.atoms().storage_hash(molecule_uuid, hash)?;
                    let storage_range = self.db_ops.atoms().storage_range(molecule_uuid, range)?;
                    let base_key = crate::atom::molecule_key_codec::hash_range_record_key(
                        molecule_uuid,
                        &storage_hash,
                        &storage_range,
                    );
                    let final_key =
                        crate::schema::types::field::build_storage_key(storage_prefix, &base_key);
                    sibling_candidates.push((
                        delta_index,
                        sibling_index,
                        key.clone(),
                        final_key,
                        entry.clone(),
                    ));
                }
            }
        }
        Ok((candidates, sibling_candidates))
    }

    /// Take the tip-publication locks for every candidate key and settle each
    /// against the winning Delete barriers. A request fails before ack; replay
    /// filters stale mutations and retries the remaining batch without
    /// advancing its cursor early. A protein sibling can lose even when the
    /// entry Put wins: only that sibling key is removed from memory and disk
    /// work. The returned guards must outlive publication.
    pub(super) async fn drop_deletes_that_win(
        &self,
        prepared_deltas: &mut [PreparedSchemaDelta],
        candidates: &[crate::atom::delete_barrier::DeleteBarrier],
        sibling_candidates: &[SiblingCandidate],
        require_durable: bool,
    ) -> Result<Vec<tokio::sync::OwnedMutexGuard<()>>, SchemaError> {
        let mut tip_keys: Vec<String> = candidates
            .iter()
            .map(|candidate| candidate.mk_key.clone())
            .collect();
        tip_keys.extend(
            sibling_candidates
                .iter()
                .map(|(_, _, _, key, _)| key.clone()),
        );
        let tip_guards = self.db_ops.atoms().lock_tip_publications(&tip_keys).await;
        let delete_winners = self
            .db_ops
            .atoms()
            .winning_delete_barriers(&tip_keys)
            .await?;
        for candidate in candidates {
            if delete_winners
                .get(&candidate.mk_key)
                .is_some_and(|barrier| barrier.order_key() >= candidate.order_key())
            {
                return Err(if require_durable {
                    SchemaError::ReplayDeleteBarrierChanged
                } else {
                    SchemaError::InvalidData(
                        "a newer Delete won this key before Put publication".into(),
                    )
                });
            }
        }
        for (delta_index, sibling_index, changed_key, key, entry) in sibling_candidates {
            if delete_winners
                .get(key)
                .is_some_and(|barrier| barrier.blocks_tip(entry))
            {
                prepared_deltas[*delta_index].sibling_updates[*sibling_index]
                    .2
                    .remove(changed_key);
            }
        }
        for delta in prepared_deltas.iter_mut() {
            delta
                .sibling_updates
                .retain(|(_, _, changed)| !changed.is_empty());
        }
        Ok(tip_guards)
    }

    /// Insert every schema envelope before a durable caller waits. This keeps
    /// a multi-schema request from blocking its later schema groups.
    pub(super) async fn await_persist_completions(
        persist_completions: Vec<PersistCompletion>,
    ) -> Result<(), SchemaError> {
        for (schema_name, completion) in persist_completions {
            let counts = completion.await.map_err(|_| {
                SchemaError::InvalidData(format!(
                    "persist lane stopped before durable completion for schema '{schema_name}'"
                ))
            })?;
            crate::request_phases::add_counter(
                crate::request_phases::RequestCounter::MoleculesPersisted,
                counts.molecules_persisted,
            );
            crate::request_phases::add_counter(
                crate::request_phases::RequestCounter::MoleculeStoreCommits,
                counts.molecule_store_commits,
            );
        }
        Ok(())
    }
}
