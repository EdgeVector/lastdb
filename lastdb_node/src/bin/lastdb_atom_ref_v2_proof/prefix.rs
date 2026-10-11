//! One storage prefix: backfill, history upgrade, exact audit and lookup sample.

use super::*;

/// Read-only inputs shared by every prefix pass.
pub(super) struct ProofCtx<'a> {
    pub(super) host: &'a lastdb_node::Host,
    pub(super) args: &'a Args,
    pub(super) max_v2_bytes: u64,
}

/// Count one durable page against the run totals.
fn record_page(life: &mut Lifetime, pages: &mut usize, max_pages: usize) {
    *pages = pages.saturating_add(1);
    life.pages = life.pages.saturating_add(1);
    life.estimated_remaining_pages = Some(max_pages.saturating_sub(life.pages));
    life.progress(false);
}

/// Backfill, upgrade history, and audit one storage prefix. Returns `false`
/// when the atom budget ran out before the prefix finished.
pub(super) async fn prove_prefix(
    ctx: &ProofCtx<'_>,
    life: &mut Lifetime,
    state: &mut ProofState,
    storage_prefix: Option<String>,
    molecules: BTreeSet<String>,
) -> Result<bool, RunError> {
    let prefix = storage_prefix.as_deref();
    let mut pages = 0usize;
    life.set_phase("backfill");
    backfill_pages(ctx, life, prefix, &mut pages).await?;

    life.set_phase("history");
    for molecule_uuid in molecules {
        if life.timed_out() {
            return Err(RunError::Timeout);
        }
        if atom_budget_exhausted(ctx.args.max_atoms, life.atoms_audited) {
            return Ok(false);
        }
        prove_molecule(ctx, life, prefix, &molecule_uuid, &mut pages).await?;
    }

    if ctx.args.max_atoms.is_some() {
        state.proofs.push(PrefixProof {
            storage_prefix,
            pages,
            v2_bytes: v2_bytes(ctx.host),
            audit: None,
            lookup: None,
        });
        return Ok(true);
    }

    life.set_phase("mark_complete");
    await_with_progress(
        life,
        ctx.host
            .db
            .db_ops()
            .atoms()
            .mark_atom_ref_v2_history_complete(prefix),
    )
    .await?;
    let (audit, lookup) = audit_and_benchmark(ctx, life, prefix).await?;
    state.proofs.push(PrefixProof {
        storage_prefix,
        pages,
        v2_bytes: v2_bytes(ctx.host),
        audit: Some(audit),
        lookup,
    });
    Ok(true)
}

/// Page the compact rebuild until the backfill reports `Complete`.
async fn backfill_pages(
    ctx: &ProofCtx<'_>,
    life: &mut Lifetime,
    prefix: Option<&str>,
    pages: &mut usize,
) -> Result<(), RunError> {
    loop {
        if life.timed_out() {
            return Err(RunError::Timeout);
        }
        let status = await_with_progress(
            life,
            ctx.host
                .db
                .db_ops()
                .atoms()
                .atom_ref_v2_backfill_status(prefix),
        )
        .await?;
        require_growth_budget(ctx.host, ctx.max_v2_bytes, status.edges_written)
            .map_err(RunError::Fail)?;
        match status.phase {
            AtomRefBackfillPhase::Complete => return Ok(()),
            AtomRefBackfillPhase::Blocked => {
                return Err(RunError::Fail(format!(
                    "compact rebuild blocked after {} skipped row(s)",
                    status.skipped_rows
                )));
            }
            AtomRefBackfillPhase::Backfill | AtomRefBackfillPhase::Replay => {}
        }
        if *pages >= ctx.args.max_pages {
            return Err(RunError::Fail(format!(
                "compact rebuild exceeded {} pages",
                ctx.args.max_pages
            )));
        }
        await_with_progress(
            life,
            ctx.host
                .db
                .db_ops()
                .atoms()
                .reindex_atom_ref_v2_edges(prefix, Some(ctx.args.page)),
        )
        .await?;
        record_page(life, pages, ctx.args.max_pages);
    }
}

/// Upgrade one molecule's history to the compact manifest, then audit it.
/// A molecule whose manifest is already current is skipped and not counted.
async fn prove_molecule(
    ctx: &ProofCtx<'_>,
    life: &mut Lifetime,
    prefix: Option<&str>,
    molecule_uuid: &str,
    pages: &mut usize,
) -> Result<(), RunError> {
    let Some(manifest) = await_with_progress(
        life,
        ctx.host
            .db
            .db_ops()
            .atoms()
            .ensure_atom_ref_v2_molecule_manifest_after_reindex(molecule_uuid, prefix),
    )
    .await?
    else {
        return Err(RunError::Fail(format!(
            "missing compact manifest for {molecule_uuid}"
        )));
    };
    if manifest.version == ATOM_REF_MANIFEST_VERSION_HISTORY && manifest.replay_complete {
        return Ok(());
    }
    upgrade_history_pages(ctx, life, prefix, molecule_uuid, pages).await?;
    let molecule_audit = await_with_progress(
        life,
        ctx.host
            .db
            .db_ops()
            .atoms()
            .audit_atom_ref_v2_molecule(molecule_uuid, prefix),
    )
    .await?;
    let added = molecule_audit.expected_active_edges.max(1);
    if let Some(remaining) = atom_budget_remaining(ctx.args.max_atoms, life.atoms_audited) {
        life.atoms_audited = life.atoms_audited.saturating_add(added.min(remaining));
    } else {
        life.atoms_audited = life.atoms_audited.saturating_add(added);
    }
    life.progress(false);
    Ok(())
}

async fn upgrade_history_pages(
    ctx: &ProofCtx<'_>,
    life: &mut Lifetime,
    prefix: Option<&str>,
    molecule_uuid: &str,
    pages: &mut usize,
) -> Result<(), RunError> {
    loop {
        if life.timed_out() {
            return Err(RunError::Timeout);
        }
        let active_edges = await_with_progress(
            life,
            ctx.host
                .db
                .db_ops()
                .atoms()
                .atom_ref_v2_backfill_status(prefix),
        )
        .await?
        .edges_written;
        require_growth_budget(ctx.host, ctx.max_v2_bytes, active_edges).map_err(RunError::Fail)?;
        if *pages >= ctx.args.max_pages {
            return Err(RunError::Fail(format!(
                "compact history upgrade exceeded {} pages",
                ctx.args.max_pages
            )));
        }
        let report = await_with_progress(
            life,
            ctx.host
                .db
                .db_ops()
                .atoms()
                .upgrade_atom_ref_v2_molecule_history_page(molecule_uuid, prefix, ctx.args.page),
        )
        .await?;
        record_page(life, pages, ctx.args.max_pages);
        if report.complete {
            return Ok(());
        }
    }
}

/// Run the exact edge audit for a prefix, then the paired v1/v2 lookup sample
/// when the prefix has active edges.
async fn audit_and_benchmark(
    ctx: &ProofCtx<'_>,
    life: &mut Lifetime,
    prefix: Option<&str>,
) -> Result<(AtomRefV2AuditReport, Option<AtomRefV2LookupBenchmark>), RunError> {
    life.set_phase("audit");
    let audit = await_with_progress(
        life,
        ctx.host
            .db
            .db_ops()
            .atoms()
            .audit_atom_ref_v2_edges_on_isolated_copy(prefix),
    )
    .await?;
    if !audit.complete
        || audit.missing_edges != 0
        || audit.unexpected_edges != 0
        || audit.invalid_live_keys != 0
        || audit.false_zero_reference_atoms != 0
        || audit.indexed_active_edges != audit.expected_active_edges
    {
        return Err(RunError::Fail(format!(
            "compact audit is not exact: {audit:?}"
        )));
    }
    life.atoms_audited = life
        .atoms_audited
        .saturating_add(audit.expected_active_edges.max(audit.molecules_audited));
    let lookup = if audit.expected_active_edges == 0 {
        None
    } else {
        life.set_phase("lookup");
        Some(
            await_with_progress(
                life,
                ctx.host
                    .db
                    .db_ops()
                    .atoms()
                    .benchmark_atom_ref_v2_lookups_on_isolated_copy(
                        prefix,
                        ctx.args.lookup_samples,
                    ),
            )
            .await?,
        )
    };
    Ok((audit, lookup))
}
