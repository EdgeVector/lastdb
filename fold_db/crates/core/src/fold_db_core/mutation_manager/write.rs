//! Public write entry points, batch pipeline orchestration, timing, finalize.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::schema::types::Mutation;
use crate::schema::{SchemaError, SchemaState};
use tracing::info;

#[cfg(not(feature = "cloud-sync"))]
use super::receipt::CloudMutationReceipt;
use super::receipt::{
    CloudCapturePolicy, ResidentCommitOperations, ResidentCommitReceipt, ResidentCommitStages,
    ResidentDurability,
};
use super::MutationManager;

use super::helpers::{apply_storage_prefix_to_schema, validate_derived_provenance};

/// Max keys purged under one exclusive per-schema barrier hold.
const PURGE_BARRIER_CHUNK: usize = 64;

mod attribution;
mod batch_inner;
mod cloud_entry;
mod create_update_batch;
mod erasure;
mod hard_erasure;
use attribution::{attribution_scopes, attribution_source_events_enabled};
use erasure::estimate_retained_atom_set_bytes;
#[cfg(feature = "cloud-sync")]
mod replay;
mod schema_delta;
mod schema_delta_persist;
mod timing;

/// Whether a batch is a caller's request or the replay of one already applied.
///
/// The only thing this changes is the missing-target policy on the two loud
/// hard-erasure peels, and the reason is that `Refuse` answers a question only
/// a requester can ask.
///
/// `PurgeMissingPolicy::Refuse` exists so a compliance purge cannot silently
/// no-op: the operator said "erase this record" and is owed the answer "it was
/// not there". **Replay has no requester and asks no question.** It restates an
/// act that already happened, and the end state it wants is absence — which an
/// absent target already satisfies. That is true of every replayer: the node
/// that authored the entry (its own local apply is what removed the targets), a
/// peer that never held the record, and a peer that already purged it on its
/// own cycle. In all three "already gone" is convergence, not a fault.
///
/// Steady-state personal download now skips self-authored entries, so this
/// self-echo path is restore/bootstrap (which still replays them) and any
/// peer that never held the record. A refuse-on-absent replay still pins
/// the cursor; restore must treat already-gone as convergence.
///
/// Getting this wrong is not a stray warning. A replay error propagates to
/// `SyncEngine::replay_entry`, which pins the download cursor at that seq; the
/// target's uploads queue behind the pin, so **cloud backup stops and RPO grows
/// without bound** until an operator runs `lastdb cloud quarantine-replay`.
/// Observed on the primary 2026-08-17: the telemetry reaper purged 64
/// `lastdb_telemetry/RequestOpsRollup` rows, the same intent came back down the
/// log, replay found 64 of 64 keys absent, and the pipeline stopped — RPO
/// climbing 270 s → 449 s across four cycles, not recovering, until the pin was
/// cleared by hand.
///
/// Half of this was already fixed one layer up and only for one verb:
/// `encode_mutations` strips `must_exist` from the envelope so a replayed
/// must-exist delete decodes as a plain `Delete` and routes to `Skip`
/// (`replayed_must_exist_delete_of_absent_key_succeeds`). `MutationType::Purge`
/// has no such downgrade — the verb *is* the mutation type, and rewriting it to
/// `Delete` on the wire would change the ledger verb and CDC record — so the
/// policy is chosen here, at the apply site, where the origin is known.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum WriteOrigin {
    /// A live caller's batch. Loud erasure verbs keep their loud contract.
    Request,
    /// A captured intent or internal aggregate recovery write.
    Replay,
}

pub(in crate::fold_db_core::mutation_manager) struct HardErasureOptions<'a> {
    pub(in crate::fold_db_core::mutation_manager) storage_prefix: Option<&'a str>,
    pub(in crate::fold_db_core::mutation_manager) missing: super::super::purge::PurgeMissingPolicy,
    pub(in crate::fold_db_core::mutation_manager) verb: super::super::purge::HardEraseVerb,
    pub(in crate::fold_db_core::mutation_manager) evict_resident: bool,
    pub(in crate::fold_db_core::mutation_manager) precomputed_retained: Option<HashSet<String>>,
    pub(in crate::fold_db_core::mutation_manager) emit_derived_events: bool,
}

impl WriteOrigin {
    /// Missing-target policy for the peels that are `Refuse` on request:
    /// compliance `Purge` and `Delete` + `must_exist`.
    ///
    /// The idempotent `Delete` peel is `Skip` under both origins and does not
    /// consult this.
    fn missing_policy_for_loud_erasure(self) -> super::super::purge::PurgeMissingPolicy {
        match self {
            Self::Request => super::super::purge::PurgeMissingPolicy::Refuse,
            Self::Replay => super::super::purge::PurgeMissingPolicy::Skip,
        }
    }
}

impl MutationManager {
    /// Write mutations with local caller context.
    ///
    /// Local callers approved by the host consent layer get full local DB write
    /// access. Trust tiers, capability quotas, payment gates, and namespace walls
    /// no longer gate this path (consent-only local control).
    ///
    /// When `access_context.storage_prefix` is set (org multi-DB handle from
    /// `X-LastDB-Db`), molecule keys land under `{prefix}:…` so cohabiting DBs
    /// on one Mini node stay isolated.
    pub async fn write_mutations_with_access_receipt(
        &self,
        mutations: Vec<Mutation>,
        access_context: &crate::access::AccessContext,
    ) -> Result<ResidentCommitReceipt, SchemaError> {
        self.write_mutations_with_access_receipt_cloud(
            mutations,
            access_context,
            CloudCapturePolicy::Async,
        )
        .await
    }

    /// [`Self::write_mutations_with_access_receipt`] returning only the
    /// mutation ids.
    ///
    /// This keeps the name and the exact result every existing caller already
    /// has; the receipt form is the sibling, mirroring
    /// [`Self::write_mutations_batch_async`] and
    /// [`Self::write_mutations_batch_with_receipt`] one layer down. Widening
    /// the established name instead would have made every unrelated call site
    /// unwrap a field it has no reader for.
    pub async fn write_mutations_with_access(
        &self,
        mutations: Vec<Mutation>,
        access_context: &crate::access::AccessContext,
    ) -> Result<Vec<String>, SchemaError> {
        Ok(self
            .write_mutations_with_access_receipt(mutations, access_context)
            .await?
            .mutation_ids)
    }

    /// Write multiple mutations in a batch for improved performance (async version)
    /// Groups mutations by schema to minimize schema reloads and uses true batching
    ///
    /// This is the preferred async version that avoids deadlocks.
    /// All storage operations use direct async/await instead of run_async.
    ///
    /// `storage_prefix`: when `Some`, scopes molecule/idempotency keys to that
    /// org/db hash (see design-org-db-handle-platform-gap). `None` is personal.
    pub async fn write_mutations_batch_async(
        &self,
        mutations: Vec<Mutation>,
        storage_prefix: Option<&str>,
    ) -> Result<Vec<String>, SchemaError> {
        Ok(self
            .write_mutations_batch_with_receipt(mutations, storage_prefix)
            .await?
            .mutation_ids)
    }

    /// [`Self::write_mutations_batch_async`] answering with the full
    /// [`ResidentCommitReceipt`] instead of only the mutation ids.
    ///
    /// This is the real body; the id-returning entry point above is a thin
    /// projection of it, so the two cannot drift.
    pub async fn write_mutations_batch_with_receipt(
        &self,
        mutations: Vec<Mutation>,
        storage_prefix: Option<&str>,
    ) -> Result<ResidentCommitReceipt, SchemaError> {
        self.write_mutations_batch_with_receipt_cloud(
            mutations,
            storage_prefix,
            CloudCapturePolicy::Async,
        )
        .await
    }

    pub(super) async fn write_mutations_batch_inner(
        &self,
        mutations: Vec<Mutation>,
        storage_prefix: Option<&str>,
        origin: WriteOrigin,
    ) -> Result<ResidentCommitReceipt, SchemaError> {
        self.write_mutations_batch_inner_with_clock(mutations, storage_prefix, origin, None)
            .await
    }

    // --- Helpers for write_mutations_batch_async ---

    pub(super) fn reject_blocked_mutation_targets(
        &self,
        mutations: &[Mutation],
    ) -> Result<(), SchemaError> {
        let states = self.schema_manager.get_schema_states()?;
        for mutation in mutations {
            if states
                .get(&mutation.schema_name)
                .copied()
                .unwrap_or_default()
                == SchemaState::Blocked
            {
                return Err(SchemaError::Blocked(format!(
                    "Schema '{}' is blocked and cannot be mutated",
                    mutation.schema_name
                )));
            }
        }
        Ok(())
    }

    /// Finalizes the batch with an optional durability flush.
    ///
    /// **Default (sled-like):** skip the barrier here. Puts are already in
    /// LastStore memory buffers; the process background flusher
    /// (see [`crate::fold_db_core::mutation_flush`]) and `FoldDB::shutdown`
    /// own whole-store F_FULLFSYNC. `force_durable` or
    /// `LASTDB_MUTATION_SYNC_FLUSH=1` syncs only the groups this batch wrote.
    pub(super) async fn finalize_batch(
        &self,
        timing_breakdown: &mut HashMap<&str, std::time::Duration>,
        force_durable: bool,
    ) -> Result<(), SchemaError> {
        let flush_required = force_durable || crate::fold_db_core::mutation_sync_flush_enabled();
        if flush_required {
            let flush_start = std::time::Instant::now();
            tracing::debug!("Flushing written groups after batch mutations");
            let written = crate::durable_flush::current_written_keys();
            self.db_ops.flush_dirty_scope(&written).await.map_err(|e| {
                tracing::error!(
                    "Failed to flush written groups after batch mutations: {}",
                    e
                );
                e
            })?;
            tracing::debug!("Database flushed in {:?}", flush_start.elapsed());
            // Timed only when a flush actually ran: a skipped flush must not
            // book its branch check + debug log as ~1µs of "flush" on every
            // mutation — the phase telemetry hides absent phases, and this is
            // what keeps the default build's flush column honestly absent.
            Self::add_timing(timing_breakdown, "flush", flush_start.elapsed());
        } else {
            // Memory-first: mutation is visible in-process; durable shortly via
            // background flusher / shutdown / LastStore max_dirty group-commit.
            tracing::debug!(
                "Skipping sync flush after batch mutations (deferred durability; \
                 set LASTDB_MUTATION_SYNC_FLUSH=1 for per-mutation fsync)"
            );
        }

        Ok(())
    }

    /// Groups mutations by schema name for efficient batch processing.
    /// Returns groups in first-seen schema order.
    pub(super) fn group_mutations_by_schema(
        &self,
        mutations: Vec<Mutation>,
    ) -> Vec<(String, Vec<Mutation>)> {
        let mut order: Vec<String> = Vec::new();
        let mut grouped: HashMap<String, Vec<Mutation>> = HashMap::new();

        for mutation in mutations {
            if !grouped.contains_key(&mutation.schema_name) {
                order.push(mutation.schema_name.clone());
            }
            grouped
                .entry(mutation.schema_name.clone())
                .or_default()
                .push(mutation);
        }

        order
            .into_iter()
            .map(|name| {
                let mutations = grouped
                    .remove(&name)
                    .expect("schema order came from grouped keys");
                (name, mutations)
            })
            .collect()
    }
}
