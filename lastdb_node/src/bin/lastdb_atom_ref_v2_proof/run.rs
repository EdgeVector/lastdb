//! The proof run: preflight, boot, crash resume, then one pass per storage prefix.
//!
//! A timeout anywhere below `run_phases` surfaces as `RunError::Timeout`.
//! `run` turns it into a timeout report from the partial results kept in
//! `ProofState`.

use super::*;

/// What a timeout report needs from a run in progress.
struct ProofState {
    max_v2_bytes: u64,
    proofs: Vec<PrefixProof>,
    crash: Option<AtomRefCrashResumeProof>,
}

/// Read-only inputs shared by every prefix pass.
struct ProofCtx<'a> {
    host: &'a lastdb_node::Host,
    args: &'a Args,
    max_v2_bytes: u64,
}

pub(super) async fn run(args: Args, life: &mut Lifetime) -> Result<ProofReport, RunError> {
    let home = preflight(&args, life)?;
    if life.timed_out() {
        return Ok(life.timeout_report(home, 0, Vec::new(), None));
    }
    let mut state = ProofState {
        max_v2_bytes: 0,
        proofs: Vec::new(),
        crash: None,
    };
    match run_phases(&args, life, &home, &mut state).await {
        Err(RunError::Timeout) => {
            Ok(life.timeout_report(home, state.max_v2_bytes, state.proofs, state.crash))
        }
        other => other,
    }
}

/// Check the isolated-copy guards and return the canonical proof home.
fn preflight(args: &Args, life: &mut Lifetime) -> Result<PathBuf, RunError> {
    if !args.ack_isolated_copy {
        return Err(RunError::Fail(
            "pass --ack-isolated-copy after the safe copy step".to_string(),
        ));
    }
    let home = canonical_dir(&args.home).map_err(RunError::Fail)?;
    life.home = home.clone();
    life.publish();
    let temp = canonical_dir(&std::env::temp_dir()).map_err(RunError::Fail)?;
    if !home.starts_with(&temp) {
        return Err(RunError::Fail(format!(
            "proof home must stay under the system temporary directory {} (got {})",
            temp.display(),
            home.display()
        )));
    }
    if let Ok(live_home) = lastdb_node::host::resolve_home(None) {
        if live_home.canonicalize().ok().as_ref() == Some(&home) {
            return Err(RunError::Fail(
                "refusing to run the proof on the live LastDB home".to_string(),
            ));
        }
    }
    let cloud_config = home.join(lastdb_node::host::CLOUD_SYNC_CONFIG_FILE);
    if cloud_config.exists() {
        return Err(RunError::Fail(format!(
            "remove {} from the copy before the proof",
            cloud_config.display()
        )));
    }
    if args.drain_v1_and_compact && !env_falsey("LASTDB_ATOM_REF_BACKFILL") {
        return Err(RunError::Fail(
            "--drain-v1-and-compact requires LASTDB_ATOM_REF_BACKFILL=0".to_string(),
        ));
    }
    Ok(home)
}

async fn run_phases(
    args: &Args,
    life: &mut Lifetime,
    home: &Path,
    state: &mut ProofState,
) -> Result<ProofReport, RunError> {
    life.set_phase("boot");
    life.estimated_remaining_pages = Some(args.max_pages);
    let host = await_with_progress(life, lastdb_node::Host::boot(home)).await?;
    let v1_allocated_before = collection_allocated_bytes(&host, V1_COLLECTION);
    let v2_allocated_before = collection_allocated_bytes(&host, V2_COLLECTION);
    let crash_prefix = collect_molecules_by_prefix(&host)
        .map_err(RunError::Fail)?
        .keys()
        .next()
        .cloned()
        .flatten();
    life.set_phase("crash_resume");
    let (host, crash) =
        interrupt_and_resume_first_page(host, home, crash_prefix.as_deref(), args.page, life)
            .await?;
    state.crash = Some(crash);
    let molecules_by_prefix = collect_molecules_by_prefix(&host).map_err(RunError::Fail)?;

    let max_v2_bytes = lastdb_node::atom_ref_backfill::atom_ref_v2_max_bytes();
    life.max_v2_bytes = max_v2_bytes;
    life.publish();
    state.max_v2_bytes = max_v2_bytes;
    let ctx = ProofCtx {
        host: &host,
        args,
        max_v2_bytes,
    };
    for (storage_prefix, molecules) in molecules_by_prefix {
        if life.timed_out() {
            return Err(RunError::Timeout);
        }
        if atom_budget_exhausted(args.max_atoms, life.atoms_audited) {
            break;
        }
        if !prove_prefix(&ctx, life, state, storage_prefix, molecules).await? {
            break;
        }
    }

    if life.timed_out() {
        return Err(RunError::Timeout);
    }
    if args.max_atoms.is_some() {
        life.set_phase("partial");
        return Ok(life.finish_report(
            decide_status(false, true),
            home.to_path_buf(),
            max_v2_bytes,
            std::mem::take(&mut state.proofs),
            state.crash.take(),
            None,
        ));
    }

    verify_crash_resume(state)?;
    let reclaim = if args.drain_v1_and_compact {
        life.set_phase("drain");
        Some(
            drain_and_compact(
                &host,
                &state.proofs,
                args.page,
                args.max_pages,
                v1_allocated_before,
                v2_allocated_before,
                life,
            )
            .await?,
        )
    } else {
        None
    };

    if life.timed_out() {
        return Err(RunError::Timeout);
    }
    life.set_phase("complete");
    Ok(life.finish_report(
        decide_status(false, false),
        home.to_path_buf(),
        max_v2_bytes,
        std::mem::take(&mut state.proofs),
        state.crash.take(),
        reclaim,
    ))
}

/// The crash-resume proof holds only when the cursor survived and every
/// prefix audit finished.
fn verify_crash_resume(state: &mut ProofState) -> Result<(), RunError> {
    let resumed_to_complete = state
        .proofs
        .iter()
        .all(|proof| proof.audit.as_ref().is_some_and(|audit| audit.complete));
    let Some(crash) = state.crash.as_mut() else {
        return Err(RunError::Fail(
            "compact crash resume proof is missing".to_string(),
        ));
    };
    crash.resumed_to_complete = resumed_to_complete;
    if !crash.cursor_survived || !crash.resumed_to_complete {
        return Err(RunError::Fail(format!(
            "compact crash resume did not complete with a surviving cursor: {crash:?}"
        )));
    }
    Ok(())
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
async fn prove_prefix(
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
