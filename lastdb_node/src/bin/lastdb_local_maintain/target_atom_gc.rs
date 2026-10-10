//! Exact stopped-home atom reclaim with independent complete source proof.
//!
//! Selected UUIDs declare scope, never liveness. Every retained physical source,
//! reverse edge, pending hold, and durable cloud source remains a hold. This
//! tool appends ordinary deletes; it never compacts atoms or changes retirement
//! or completeness markers. The operator owns the clean-stop and cloud fence.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use clap::Args;
use sha2::{Digest, Sha256};

use crate::home::{open_home_for_offline_read, resolve_laststore_root, HomeStore};
use crate::reap::guard::{self, Flags};

mod execute;
mod input;
mod ledger;
mod model;
mod plan;
mod private_io;

#[derive(Args, Debug)]
pub(crate) struct TargetAtomGcArgs {
    #[arg(skip)]
    pub home: PathBuf,
    #[arg(long)]
    pub plan_dir: PathBuf,
    /// Exact content UUIDs from separately validated source evidence.
    #[arg(long)]
    pub target_atom_ids_file: PathBuf,
    #[arg(long)]
    pub target_atom_ids_sha256: String,
    /// Private provenance artifact; this is not deletion authority.
    #[arg(long)]
    pub source_evidence_file: PathBuf,
    #[arg(long)]
    pub source_evidence_sha256: String,
    #[arg(long)]
    pub execute: bool,
    #[arg(long)]
    pub stopped_primary: bool,
    #[arg(long)]
    pub i_know_this_is_primary: bool,
    #[arg(long)]
    pub json: bool,
}

pub(crate) fn run(args: &TargetAtomGcArgs) -> Result<(), String> {
    prove_stopped(args)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(err)?;
    let report = if args.execute {
        let saved = private_io::load_plan(&args.plan_dir)?;
        private_io::validate_location(&args.plan_dir, &saved.home, &saved.store_root)?;
        if saved.home != std::fs::canonicalize(&args.home).map_err(err)?
            || saved.store_root
                != std::fs::canonicalize(resolve_laststore_root(&args.home)?).map_err(err)?
            || saved.input != input::read(args)?
        {
            return Err("the saved target atom plan names a different home or input".into());
        }
        runtime.block_on(execute::execute(args, &saved))?
    } else {
        let opened = open_home_for_offline_read(&args.home)?;
        if opened.seam != "at-rest-seam" {
            return Err("target atom proof requires the at-rest reader".into());
        }
        let home = std::fs::canonicalize(&args.home).map_err(err)?;
        let root = std::fs::canonicalize(&opened.store_root).map_err(err)?;
        private_io::create_dir(&args.plan_dir, &home, &root)?;
        let started_at = chrono::Utc::now().to_rfc3339();
        let saved = runtime.block_on(plan::build(args, &opened, &started_at))?;
        input::read(args)?;
        prove_stopped(args)?;
        private_io::write(&args.plan_dir, model::PLAN_FILE, &saved)?;
        model::Report::planned(&saved)
    };
    if args.json {
        println!("{}", serde_json::to_string(&report).map_err(err)?);
    } else {
        println!("target atom GC operation complete; use --json for numeric counts.");
    }
    Ok(())
}

fn prove_stopped(args: &TargetAtomGcArgs) -> Result<(), String> {
    let root = resolve_laststore_root(&args.home)?;
    let sockets = guard::socket_paths(&args.home, &root);
    let view = guard::live_process_view(&sockets);
    guard::prove(
        &args.home,
        &root,
        Flags {
            stopped_primary: args.stopped_primary,
            i_know_this_is_primary: args.i_know_this_is_primary,
        },
        &view,
    )
    .map(|_| ())
    .map_err(err)
}

fn err(error: impl std::fmt::Display) -> String {
    error.to_string()
}
