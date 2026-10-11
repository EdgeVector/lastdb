//! Referenced-atom collection and orphan protein/atom GC.

use super::*;

/// The fixed inputs of one `gc-atoms` atom walk.
struct GcAtomWalk<'a> {
    dry_run: bool,
    storage_prefix: Option<&'a str>,
    source_schema_name: Option<&'a str>,
    scan_started_at: DateTime<Utc>,
}

mod atom_walk;
mod proteins;
mod references;

impl AtomStore {
    /// GC orphan atoms + tip-version chains.
    ///
    /// 1. Drop `tv:` chains and clear `prev_tip_id` on eligible tips:
    ///    - default: only tips with `meta.tombstoned`
    ///    - `prune_live_history`: every tip with a non-empty history chain
    ///      (drops `as_of` history; reclaim path for append-heavy live records)
    /// 2. Delete `atom:` rows not referenced by any remaining tip / `tv:` /
    ///    history / conflict / legacy ref.
    pub async fn gc_orphan_atoms(
        &self,
        dry_run: bool,
        storage_prefix: Option<&str>,
    ) -> Result<AtomGcReport, SchemaError> {
        self.gc_orphan_atoms_with(dry_run, storage_prefix, false)
            .await
    }

    /// Like [`Self::gc_orphan_atoms`], with optional live tip-history prune.
    pub async fn gc_orphan_atoms_with(
        &self,
        dry_run: bool,
        storage_prefix: Option<&str>,
        prune_live_history: bool,
    ) -> Result<AtomGcReport, SchemaError> {
        self.gc_orphan_atoms_with_roots(
            dry_run,
            storage_prefix,
            prune_live_history,
            &HashSet::new(),
        )
        .await
    }

    /// Like [`Self::gc_orphan_atoms_with`], plus reference roots this store
    /// cannot reach on its own.
    ///
    /// The five roots below (`mk:`, `tv:`, `history:`, `conflict:`, `ref:`) are
    /// every plane an `AtomStore` owns, but they are not every referrer. A
    /// durable pin-log record carries `field_atom_uuids` into the atom plane
    /// and has its inline bodies stripped on that basis, and that plane lives
    /// in the sync engine's own namespace. Reading an atom as an orphan
    /// because the only referrer is one this type cannot see is how an acked
    /// write loses its cloud copy — so the composition happens one layer up,
    /// in `FoldDB::gc_orphan_atoms_with`, which owns both.
    ///
    /// An empty `extra_roots` reproduces the historical behavior exactly.
    pub async fn gc_orphan_atoms_with_roots(
        &self,
        dry_run: bool,
        storage_prefix: Option<&str>,
        prune_live_history: bool,
        extra_roots: &HashSet<String>,
    ) -> Result<AtomGcReport, SchemaError> {
        self.gc_orphan_atoms_with_roots_at_cut(
            dry_run,
            storage_prefix,
            prune_live_history,
            extra_roots,
            Utc::now(),
        )
        .await
    }

    /// Run atom GC with a cutoff captured before a deferred-persist barrier.
    ///
    /// The background janitor captures this time before it samples pending
    /// work. A body created after the sample is therefore recent, while an
    /// older body belongs to work that the sampled watermark must drain.
    pub(crate) async fn gc_orphan_atoms_with_roots_at_cut(
        &self,
        dry_run: bool,
        storage_prefix: Option<&str>,
        prune_live_history: bool,
        extra_roots: &HashSet<String>,
        scan_started_at: DateTime<Utc>,
    ) -> Result<AtomGcReport, SchemaError> {
        self.gc_orphan_atoms_with_roots_for_schema_at_cut(
            dry_run,
            storage_prefix,
            prune_live_history,
            extra_roots,
            None,
            Some(scan_started_at),
        )
        .await
    }

    /// Like [`Self::gc_orphan_atoms_with_roots`], but only decides atom bodies
    /// whose plain header names `source_schema_name`.
    pub async fn gc_orphan_atoms_with_roots_for_schema(
        &self,
        dry_run: bool,
        storage_prefix: Option<&str>,
        prune_live_history: bool,
        extra_roots: &HashSet<String>,
        source_schema_name: Option<&str>,
    ) -> Result<AtomGcReport, SchemaError> {
        self.gc_orphan_atoms_with_roots_for_schema_at_cut(
            dry_run,
            storage_prefix,
            prune_live_history,
            extra_roots,
            source_schema_name,
            None,
        )
        .await
    }

    /// The run start instant is taken before the FIRST read of the run. Everything below — the tip
    /// chain prune, the reference-set scans, the atom scan — is a snapshot
    /// of a store that keeps accepting writes throughout: `gc-atoms` takes
    /// no lock and the node is not quiesced. So the reference set is only
    /// authoritative for atoms that already existed at this instant; an atom
    /// written afterwards has a live tip no scan below could have seen.
    /// Deleting on that basis is how a *live* record loses its body while
    /// its tip survives — no tombstone, no purge, no trace.
    ///
    /// The prologue is bounded, so resume where the last pass stopped rather
    /// than replanning the plane. `sweep_complete` restarts at the head of
    /// `mk:`: chains accrue behind the cursor as records are rewritten, so a
    /// finished sweep is a lap, not a terminal state.
    ///
    /// Do not start another global plane while the prologue still has a
    /// durable page ahead. The automatic caller gives the whole operation
    /// 120 seconds. Returning here makes each call either reclaim bytes or
    /// bank prologue progress, and prevents `gc scan mk` from consuming the
    /// remainder after a bounded prune page.
    pub(super) async fn gc_orphan_atoms_with_roots_for_schema_at_cut(
        &self,
        dry_run: bool,
        storage_prefix: Option<&str>,
        prune_live_history: bool,
        extra_roots: &HashSet<String>,
        source_schema_name: Option<&str>,
        scan_started_at: Option<DateTime<Utc>>,
    ) -> Result<AtomGcReport, SchemaError> {
        if source_schema_name.is_some() && prune_live_history {
            return Err(SchemaError::InvalidData(
                "schema-scoped gc-atoms does not support --prune-live-history".into(),
            ));
        }
        let scan_started_at = scan_started_at.unwrap_or_else(Utc::now);

        let mut checkpoint = if source_schema_name.is_some() {
            GcAtomsPruneCheckpoint::default()
        } else {
            self.gc_atoms_prune_checkpoint(storage_prefix).await?
        };

        // One write-ahead row covers the whole bounded prologue lap. Opening a
        // fresh row on every partial pass adds a new physical handle to the
        // same main collection the cursor must cross. With one handle visited
        // per pass, that shape can never converge. Persist the shared row key
        // before the first prune delete, then resume it until the lap finishes.
        let ledger = self
            .open_gc_atoms_ledger(
                dry_run,
                storage_prefix,
                source_schema_name,
                scan_started_at,
                &mut checkpoint,
            )
            .await?;

        let prune = self
            .run_gc_atoms_prune_pass(
                dry_run,
                storage_prefix,
                prune_live_history,
                source_schema_name,
                scan_started_at,
                &mut checkpoint,
            )
            .await?;

        let mut report =
            Self::gc_atoms_initial_report(dry_run, source_schema_name, scan_started_at, &prune);

        if prune.more_remaining {
            return Ok(report);
        }

        let ledger_tip_versions_pruned = checkpoint.ledger_tip_versions_pruned;
        let ledger_tips_skipped_changed = checkpoint.ledger_tips_skipped_changed;
        if !dry_run && source_schema_name.is_none() {
            self.clear_gc_atoms_ledger_resume(storage_prefix, &mut checkpoint)
                .await?;
        }

        let referenced = self
            .collect_gc_atoms_roots(storage_prefix, dry_run, &prune, extra_roots)
            .await?;
        if source_schema_name.is_none() {
            report.atoms_referenced = referenced.len() as u64;
        }

        let walk = GcAtomWalk {
            dry_run,
            storage_prefix,
            source_schema_name,
            scan_started_at,
        };
        let mut pending: Vec<String> = Vec::new();
        let storage_keys_deleted = self
            .walk_gc_atom_plane(&walk, &referenced, &mut report, &mut pending)
            .await?;

        if let Some(handle) = ledger {
            self.finish_gc_atoms_ledger(
                handle,
                &mut pending,
                storage_keys_deleted,
                &report,
                (ledger_tip_versions_pruned, ledger_tips_skipped_changed),
            )
            .await?;
        }

        Ok(report)
    }
}
