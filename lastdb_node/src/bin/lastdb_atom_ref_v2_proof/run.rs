//! The proof run: preflight, boot, crash resume, then one pass per storage prefix.
//!
//! A timeout anywhere below `run_phases` surfaces as `RunError::Timeout`.
//! `run` turns it into a timeout report from the partial results kept in
//! `ProofState`.

use super::*;

#[path = "prefix.rs"]
mod prefix;

use prefix::{prove_prefix, ProofCtx};

/// What a timeout report needs from a run in progress.
pub(super) struct ProofState {
    pub(super) max_v2_bytes: u64,
    pub(super) proofs: Vec<PrefixProof>,
    pub(super) crash: Option<AtomRefCrashResumeProof>,
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
