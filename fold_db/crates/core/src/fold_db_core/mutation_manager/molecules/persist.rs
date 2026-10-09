//! Persist modified molecules and mutation events.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::atom::MutationEvent;
use crate::db_operations::{ChangedKey, MoleculeData, MoleculeGateHoldStats};
use crate::schema::types::Schema;
use crate::schema::SchemaError;

use super::super::helpers::ModifiedFieldKeys;
use super::super::MutationManager;

/// Everything one deferred durable persist owns while it runs.
pub(crate) struct DeferredPersistJob {
    /// The request author clock must reach durable metadata before this data.
    pub(in crate::fold_db_core::mutation_manager) author_clock_barrier:
        Option<super::super::author_clock::AuthorClockPersistBarrier>,
    pub deferred_atoms: Option<Vec<(crate::atom::Atom, Option<crate::atom::AtomPartition>)>>,
    pub storage_prefix: Option<String>,
    pub search_batch: Option<crate::db_operations::search_index::IndexChangeBatch>,
    pub share_prefixes: Vec<String>,
    pub schema: Schema,
    pub modified_fields: ModifiedFieldKeys,
    pub mutation_events: Vec<MutationEvent>,
    /// Idempotency decisions for this schema group. They become visible only
    /// after the data stages ahead of them succeed on the same lane.
    pub idempotency_entries: Vec<(String, String)>,
    pub retention_write: Option<(Vec<crate::schema::types::KeyValue>, u64)>,
    pub retention_hash_partitions: Option<Vec<crate::schema::types::KeyValue>>,
    pub schema_name: String,
    /// Protein sibling tip updates prepared on the hot path (memory only).
    /// Durable-stored here so multi-key fold does not await flush before ack
    /// under `LASTDB_RESIDENT_MODE=write`.
    pub sibling_updates: Vec<(String, MoleculeData, HashSet<ChangedKey>)>,
    /// Apply gates no longer travel with the persist job (PR 3). The persist
    /// lane serializes durable order. The field stays so a mid-rollout job
    /// constructed with leftover guards still drops them after persist.
    pub molecule_write_guards: Vec<MoleculeGateGuard>,
    /// Resident dirty keys this batch installed pre-ack (tips + deferred atom
    /// bodies, see [`MutationManager::resident_batch_dirty_keys`]). Atom keys
    /// clear after the FULL durable put succeeds; molecule tips clear only if
    /// the resident tip still names the atom UUID this job persisted. On failure
    /// they stay dirty so the persist worker retries and eviction is refused.
    pub batch_dirty_keys: Vec<crate::resident::DirtyKey>,
    /// Charge on the deferred-write byte window. Moved into the task, so it is
    /// returned on EVERY exit — including the early error returns and a panic —
    /// and a failed persist cannot permanently shrink the window. `None` when
    /// the request waits for durable completion after gate release (no RSS
    /// write-behind charge).
    pub defer_reservation: Option<crate::memory_budget::DeferReservation>,
    /// Slot revisions recorded under the apply gate, copied onto the envelope.
    pub slot_revisions: Vec<crate::resident::PersistSlotRevision>,
    /// Same placement log as the request. The lane task cannot see the task-local.
    pub batch_log: Option<std::sync::Arc<crate::durable_flush::BatchPlacementLog>>,
}

/// One held molecule write gate, which books how long it was held when it
/// drops.
///
/// The gate's *wait* side is already reported as the `molecule_gate` request
/// phase. This is the other side, and the two are only useful together: deep
/// queue + short holds is a hot key (caller's fix), shallow queue + long holds
/// is a stall inside the guarded region (write path's fix). See
/// [`MoleculeGateHoldStats`].
///
/// Timing starts at construction — i.e. after `lock_owned()` returned — so the
/// acquisition wait is not double-counted into the hold.
pub(crate) struct MoleculeGateGuard {
    /// Released after this struct's own `Drop` runs — Rust calls `Drop::drop`
    /// before dropping fields — so the booked duration covers the guarded
    /// region and not the mutex release. The three relaxed atomic ops that
    /// booking costs happen while the gate is still held, which is the right
    /// way round: it cannot under-report a hold, only over-report by the
    /// nanoseconds it takes to record one.
    _guard: tokio::sync::OwnedMutexGuard<()>,
    acquired_at: std::time::Instant,
    stats: Arc<MoleculeGateHoldStats>,
}

impl MoleculeGateGuard {
    fn new(guard: tokio::sync::OwnedMutexGuard<()>, stats: Arc<MoleculeGateHoldStats>) -> Self {
        Self {
            _guard: guard,
            acquired_at: std::time::Instant::now(),
            stats,
        }
    }
}

impl Drop for MoleculeGateGuard {
    fn drop(&mut self) {
        self.stats.record(self.acquired_at.elapsed());
    }
}

/// Sweep `molecule_persist_locks` for unreferenced gates once it holds this
/// many entries. Well above the in-flight write ceiling (QoS admits 64), so a
/// sweep reclaims nearly the whole map and the next one is far away.
const MOLECULE_PERSIST_LOCK_REAP_AT: usize = 4096;

impl MutationManager {
    pub(in crate::fold_db_core::mutation_manager) async fn persist_modified_molecules(
        &self,
        schema: &mut Schema,
        modified_fields: &ModifiedFieldKeys,
        mutation_events: &[MutationEvent],
        share_prefixes: &[String],
        timing_breakdown: &mut HashMap<&str, std::time::Duration>,
    ) -> Result<(), SchemaError> {
        #[cfg(not(feature = "sharing"))]
        let _ = share_prefixes;
        let mut molecules_to_store: Vec<(String, &MoleculeData, HashSet<ChangedKey>)> = Vec::new();
        let field_names: Vec<String> = modified_fields.keys().cloned().collect();

        for (field_name, changed_keys) in modified_fields {
            let schema_field = schema.runtime_fields.get(field_name).expect(
                "field_name came from modified_fields which was populated from runtime_fields keys",
            );
            let molecule_uuid = schema_field.common().molecule_uuid().unwrap().clone(); // verified is_some above

            if let Some(mol_data) = schema_field.molecule_data() {
                molecules_to_store.push((molecule_uuid, mol_data, changed_keys.clone()));
            }
        }

        // Persist into the request's storage scope — personal (`None`) or the
        // org/db `storage_prefix` stamped on schema fields from AccessContext
        // (X-LastDB-Db). The molecule was loaded under that same prefix by
        // `restore_missing_molecules`, so writing only the changed keys +
        // header here is sufficient and leaves no untouched key stranded.
        // Also writes archived tip versions (`tv:`) only when point-in-time
        // history was explicitly enabled. Mini's default thin-tip writes emit none.
        let request_storage_prefix = field_names
            .iter()
            .find_map(|name| schema.runtime_fields.get(name))
            .and_then(|f| f.common().storage_prefix())
            .map(str::to_string);
        let had_molecules = !molecules_to_store.is_empty();
        if had_molecules {
            tracing::info!(
                storage_prefix = request_storage_prefix.as_deref().unwrap_or("(personal)"),
                "Storing {} molecules",
                molecules_to_store.len()
            );
            self.store_molecules_changed_unlocked(
                &molecules_to_store,
                request_storage_prefix.as_deref(),
            )
            .await?;
        }

        #[cfg(feature = "sharing")]
        {
            // Push to share prefixes for personal data only. Org multi-DB
            // writes already live under a db_hash prefix; sharing fan-out is
            // a personal-namespace product and must not dual-write org data.
            if request_storage_prefix.is_none() && had_molecules {
                for share_prefix in share_prefixes {
                    self.store_molecules_split(&molecules_to_store, Some(share_prefix))
                        .await?;
                }
            }
        }

        drop(molecules_to_store);

        // Drain in-memory pending tip versions after a successful store so the
        // next write does not re-buffer the same archives.
        for field_name in field_names {
            if let Some(mol) = schema
                .runtime_fields
                .get_mut(&field_name)
                .and_then(|f| f.molecule_data_mut())
            {
                let _ = mol.take_pending_tip_versions();
            }
        }

        // No `history:` MutationEvent log — tip-version chain is the history.
        let _ = mutation_events;
        let _ = timing_breakdown;

        Ok(())
    }

    /// Persist every field molecule this mutation touched.
    ///
    /// One mutation touches one molecule PER FIELD, so this is not a rare
    /// multi-molecule case: measured on the primary 2026-08-17, 230 of 1,256
    /// mutations stored 24 molecules apiece (BoardCards' 24 fields) and the mean
    /// was 8.9. Storing them one at a time meant `2F` sequential awaited durable
    /// operations per mutation — a header read and a durable put each — and
    /// `persist_molecules` was consequently the largest phase in the write path
    /// on every mutating client (47.7% of the node's top consumer).
    ///
    /// So: probe the headers concurrently (they are independent reads), then
    /// commit every field molecule through ONE durable put via
    /// [`AtomStore::store_molecules_changed_keys_batch_ref`]. A tail field
    /// plus a full-order snapshot used to split: the tail batch committed,
    /// then the snapshot put failed, and LastgitCiStatus `state` stayed at
    /// the pre-write tip while `event_id`/`log_excerpt` advanced.
    ///
    /// The only remaining sequential path is an org-prefix first write, and
    /// that path refuses to mix with other field molecules in the same
    /// mutation rather than persist a subset.
    async fn store_molecules_changed_unlocked(
        &self,
        molecules: &[(String, &MoleculeData, HashSet<ChangedKey>)],
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        if molecules.is_empty() {
            return Ok(());
        }

        // The denominator for `persist_molecules_us`. Counted here rather than
        // at the `Storing {n} molecules` log line above because this is the
        // function that actually issues durable work, and a share/org fan-out
        // calls it again for the same mutation — the counter must sum those,
        // as the phase clock already does.
        crate::request_phases::add_counter(
            crate::request_phases::RequestCounter::MoleculesPersisted,
            molecules.len() as u64,
        );

        // A request storage scope is the primary for this write, even when it
        // has an org prefix. A missing header needs only the changed slots.
        // Share fan-out uses its separate full-generation path below.
        let batch: Vec<(&str, &MoleculeData, &HashSet<ChangedKey>)> = molecules
            .iter()
            .map(|(uuid, data, changed)| (uuid.as_str(), *data, changed))
            .collect();
        crate::request_phases::add_counter(
            crate::request_phases::RequestCounter::MoleculeStoreCommits,
            1,
        );
        self.db_ops
            .atoms()
            .store_molecules_changed_keys_batch_ref(&batch, storage_prefix)
            .await?;
        Ok(())
    }

    /// Fill a persist-lane slot that the request reserved before resident apply.
    /// The accepted reservation makes this operation infallible.
    pub(in crate::fold_db_core::mutation_manager) fn fill_reserved_persist(
        &self,
        job: DeferredPersistJob,
        reservation: crate::resident::PersistReservation<LanePersistJob>,
        completion: Option<tokio::sync::oneshot::Sender<crate::request_phases::RequestCounts>>,
        pending_task: crate::fold_db_core::pending_task_tracker::PendingTask,
    ) {
        let bytes = job
            .defer_reservation
            .as_ref()
            .map_or(1, |r| r.bytes().max(1));
        let slot_revisions = job.slot_revisions.clone();
        let mut envelope = crate::resident::PersistEnvelope::new(
            LanePersistJob::Write {
                job: Box::new(job),
                counts: crate::request_phases::RequestCounts::default(),
                counts_completion: completion,
                _pending_task: pending_task,
            },
            bytes,
        );
        envelope.slot_revisions = slot_revisions;
        reservation.fill(envelope);
    }

    /// Persist a batch of molecules under `storage_prefix`, rewriting each molecule's
    /// **full current key set** (delete-prefix-then-write for per-key kinds).
    /// Used for share-prefix fan-out, where the destination prefix is written
    /// only incrementally on mutation and was never independently loaded/
    /// migrated — so a key-removed-since-last-store stale record must be reaped,
    /// and the unchanged keys must be (re)written so a newly-added share rule
    /// receives the molecule's complete current state on the next touch.
    #[cfg(feature = "sharing")]
    pub(in crate::fold_db_core::mutation_manager) async fn store_molecules_split(
        &self,
        molecules: &[(String, &MoleculeData, HashSet<ChangedKey>)],
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        // Per-key molecules use immutable generations. The selected generation
        // is the complete source snapshot; live `mk:` rows written after the cut
        // remain as a sparse overlay. No prefix delete or write barrier is used.
        //
        // The molecule is re-read from the PRIMARY namespace rather than taken
        // from `molecules`, because the in-memory one is **mixed domain** and no
        // single `MoleculeKeyDomain` describes it. Historically an active share
        // rule forced `needs_full_load` so the write path fully hydrated every
        // field via `refresh_from_db` (storage-form slots), then `apply` inserted
        // the touched slot from the caller's plaintext key in API form alongside
        // its storage-form twin. Measured on a blinded/OPE home in
        // `share_split_encrypted_tests`: a 3-row molecule reached this loop
        // carrying 4 slots, and rewriting them all as `Api` produced 3 keys at
        // `blind(blind(hash))` / `ope(ope(range))` plus 1 correct one — the
        // recipient's copy of every pre-existing row silently unaddressable.
        // That full-load force was removed once this re-read-from-primary path
        // landed; share fan-out no longer consumes the in-memory molecule.
        //
        // Flipping this to `Storage` (the fix the papercut proposed) is WORSE,
        // not better: it would write that API-form slot verbatim, putting the
        // plaintext hash and range segments on disk on an encrypted home.
        //
        // The primary's own `mk:` rows have neither problem. They are complete
        // (this runs after `store_molecules_changed_unlocked` has awaited its
        // durable batch) and canonically storage-form, so they can be copied
        // verbatim under `Storage`. That is also why the extra load is cheap
        // relative to what this loop already does: it delete-prefixes and
        // rewrites every key of the molecule regardless.
        let mut generations = Vec::with_capacity(molecules.len());
        for (uuid, _data, _changed) in molecules {
            let generation_cut = self
                .db_ops
                .atoms()
                .prepare_molecule_generation(uuid, storage_prefix)
                .await?;
            let Some(canonical) = self
                .db_ops
                .atoms()
                .load_molecule_per_key(uuid, None)
                .await?
            else {
                return Err(SchemaError::InvalidData(format!(
                    "share fan-out source molecule {uuid} has no durable primary header"
                )));
            };
            generations.push((uuid.clone(), canonical, generation_cut));
        }
        self.db_ops
            .atoms()
            .store_molecule_generations_batch(
                generations,
                storage_prefix,
                crate::db_operations::atom_store::MoleculeKeyDomain::Storage,
            )
            .await?;
        Ok(())
    }
}

mod jobs;
mod lane_writer;
mod locks;
mod purge_resident;
mod purge_types;
mod quarantine_records;
mod resident_publish;
pub(in crate::fold_db_core::mutation_manager) use lane_writer::*;
pub(crate) use purge_types::*;
pub(crate) use quarantine_records::*;
