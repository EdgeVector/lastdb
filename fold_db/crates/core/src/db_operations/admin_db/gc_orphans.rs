//! Referenced-atom collection and orphan protein/atom GC.

use super::*;

/// Who asks and how far one `gc-atoms` run reaches.
#[derive(Clone, Copy)]
pub(crate) struct GcAtomsRunScope<'a> {
    /// Decide only atom bodies whose plain header names this schema.
    pub(crate) source_schema_name: Option<&'a str>,
    /// The run start instant; `None` takes it at the first read.
    pub(crate) scan_started_at: Option<DateTime<Utc>>,
    /// The operator accepts that the atoms of a lost multi-device write can be
    /// deleted (the peer-apply interlock override).
    pub(crate) allow_after_peer_apply: bool,
}

/// The fixed inputs of one `gc-atoms` atom walk.
struct GcAtomWalk<'a> {
    dry_run: bool,
    storage_prefix: Option<&'a str>,
    source_schema_name: Option<&'a str>,
    scan_started_at: DateTime<Utc>,
    /// The operator override; the walk asks the interlock again for each page.
    allow_after_peer_apply: bool,
}

mod atom_row;
mod atom_walk;
mod proteins;
mod references;

impl AtomStore {
    /// Refuse a delete on a node that the peer-apply interlock holds off. Every caller of the
    /// walker (the manual verb and the opt-in reclaim janitor) passes through
    /// here. A dry run deletes nothing and always runs.
    async fn admit_gc_atoms_run(
        &self,
        dry_run: bool,
        scope: GcAtomsRunScope<'_>,
    ) -> Result<(), SchemaError> {
        if !dry_run {
            self.require_no_peer_apply_for_atom_delete(scope.allow_after_peer_apply, "gc-atoms")
                .await?;
        }
        Ok(())
    }
}

impl AtomStore {
    /// Delete atom bodies with no current tip, history event or conflict reference.
    pub async fn gc_orphan_atoms(
        &self,
        dry_run: bool,
        storage_prefix: Option<&str>,
    ) -> Result<AtomGcReport, SchemaError> {
        self.gc_orphan_atoms_with(dry_run, storage_prefix).await
    }

    /// Run current orphan atom GC in one storage scope.
    pub async fn gc_orphan_atoms_with(
        &self,
        dry_run: bool,
        storage_prefix: Option<&str>,
    ) -> Result<AtomGcReport, SchemaError> {
        self.gc_orphan_atoms_with_roots(dry_run, storage_prefix, &HashSet::new(), false)
            .await
    }

    /// Like [`Self::gc_orphan_atoms_with`], plus reference roots this store
    /// cannot reach on its own.
    ///
    /// The caller supplies roots from the durable cloud mutation log.
    /// Current tips, history events and conflicts provide the local roots.
    ///
    /// `allow_after_peer_apply` is the operator's explicit acceptance that the
    /// atoms of a lost multi-device write can be deleted. Without it a
    /// non-dry-run refuses unless this node has never applied a peer mutation.
    pub async fn gc_orphan_atoms_with_roots(
        &self,
        dry_run: bool,
        storage_prefix: Option<&str>,
        extra_roots: &HashSet<String>,
        allow_after_peer_apply: bool,
    ) -> Result<AtomGcReport, SchemaError> {
        self.gc_orphan_atoms_with_roots_at_cut(
            dry_run,
            storage_prefix,
            extra_roots,
            Utc::now(),
            allow_after_peer_apply,
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
        extra_roots: &HashSet<String>,
        scan_started_at: DateTime<Utc>,
        allow_after_peer_apply: bool,
    ) -> Result<AtomGcReport, SchemaError> {
        self.gc_orphan_atoms_with_roots_for_schema_at_cut(
            dry_run,
            storage_prefix,
            extra_roots,
            GcAtomsRunScope {
                source_schema_name: None,
                scan_started_at: Some(scan_started_at),
                allow_after_peer_apply,
            },
        )
        .await
    }

    /// Like [`Self::gc_orphan_atoms_with_roots`], but only decides atom bodies
    /// whose plain header names `source_schema_name`.
    pub async fn gc_orphan_atoms_with_roots_for_schema(
        &self,
        dry_run: bool,
        storage_prefix: Option<&str>,
        extra_roots: &HashSet<String>,
        source_schema_name: Option<&str>,
        allow_after_peer_apply: bool,
    ) -> Result<AtomGcReport, SchemaError> {
        self.gc_orphan_atoms_with_roots_for_schema_at_cut(
            dry_run,
            storage_prefix,
            extra_roots,
            GcAtomsRunScope {
                source_schema_name,
                scan_started_at: None,
                allow_after_peer_apply,
            },
        )
        .await
    }

    /// Capture the cutoff before reference reads. Newer bodies stay protected.
    pub(crate) async fn gc_orphan_atoms_with_roots_for_schema_at_cut(
        &self,
        dry_run: bool,
        storage_prefix: Option<&str>,
        extra_roots: &HashSet<String>,
        scope: GcAtomsRunScope<'_>,
    ) -> Result<AtomGcReport, SchemaError> {
        self.admit_gc_atoms_run(dry_run, scope).await?;
        let GcAtomsRunScope {
            source_schema_name,
            scan_started_at,
            allow_after_peer_apply,
        } = scope;
        let scan_started_at = scan_started_at.unwrap_or_else(Utc::now);

        let ledger = self
            .open_gc_atoms_ledger(dry_run, storage_prefix, scan_started_at)
            .await?;
        let mut report =
            Self::gc_atoms_initial_report(dry_run, source_schema_name, scan_started_at);
        let referenced = self
            .collect_gc_atoms_roots(storage_prefix, dry_run, extra_roots)
            .await?;
        if source_schema_name.is_none() {
            report.atoms_referenced = referenced.len() as u64;
        }

        let walk = GcAtomWalk {
            dry_run,
            storage_prefix,
            source_schema_name,
            scan_started_at,
            allow_after_peer_apply,
        };
        let mut pending: Vec<String> = Vec::new();
        let storage_keys_deleted = self
            .walk_gc_atom_plane(&walk, &referenced, &mut report, &mut pending)
            .await?;

        if let Some(handle) = ledger {
            self.finish_gc_atoms_ledger(handle, &mut pending, storage_keys_deleted, &report)
                .await?;
        }

        Ok(report)
    }
}
