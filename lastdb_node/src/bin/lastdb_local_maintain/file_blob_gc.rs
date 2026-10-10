//! Stopped-home local file-blob reclaim from complete physical atom sources.
//!
//! This tool never installs or bypasses a global completeness marker. It
//! keeps every blob named by any existing atom scope or durable cloud source.
//! The plan is read-only. Execute rebuilds the exact plan before key deletes,
//! writes and flushes a count-only ledger, verifies absence, and compacts only
//! the affected allowlisted mutable collections. Remote CAS is untouched.
//! The operator retains the cloud/writer freeze until a fresh snapshot commits.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use clap::Args;
use sha2::{Digest, Sha256};

use crate::home::{open_home_for_offline_read, resolve_laststore_root, HomeStore};
use crate::reap::guard::{self, Flags};

mod cloud_roots;
mod execute;
mod inventory;
mod model;
mod pointers;
mod snapshot_fence;
mod target_inventory;

#[derive(Args, Debug)]
pub(crate) struct FileBlobGcArgs {
    #[arg(skip)]
    pub home: PathBuf,
    /// New private directory for a plan; existing exact plan for execute.
    #[arg(long)]
    pub plan_dir: PathBuf,
    #[arg(long)]
    pub execute: bool,
    #[arg(long)]
    pub stopped_primary: bool,
    #[arg(long)]
    pub i_know_this_is_primary: bool,
    #[arg(long)]
    pub json: bool,
    /// Preserve exact atom identities and blob references without deletion.
    #[arg(long, conflicts_with = "execute", requires = "target_schema_file")]
    pub inventory_only: bool,
    /// Exact source-schema identities; never a schema-name pattern.
    #[arg(long, requires = "inventory_only")]
    pub target_schema_file: Option<PathBuf>,
}

pub(crate) fn run(args: &FileBlobGcArgs) -> Result<(), String> {
    let store_root = resolve_laststore_root(&args.home)?;
    prove_stopped(args)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(err)?;
    let opened = open_home_for_offline_read(&args.home)?;
    if opened.seam != "at-rest-seam" {
        return Err("file blob proof needs the at-rest reader".into());
    }
    if args.inventory_only {
        return runtime.block_on(target_inventory::run(args, &opened));
    }
    let report = if args.execute {
        let plan = model::load_plan(&args.plan_dir)?;
        if plan.home != std::fs::canonicalize(&args.home).map_err(err)?
            || plan.store_root != std::fs::canonicalize(&store_root).map_err(err)?
        {
            return Err("the file blob plan names a different home".into());
        }
        model::validate_plan_location(&args.plan_dir, &plan.home, &plan.store_root)?;
        let now = runtime.block_on(inventory::build(&args.home, &opened, &plan.started_at))?;
        model::exact_plan_equal(&plan, &now)?;
        drop(opened);
        runtime.block_on(execute::execute(args, &plan))?
    } else {
        let home = std::fs::canonicalize(&args.home).map_err(err)?;
        let store_root = std::fs::canonicalize(store_root).map_err(err)?;
        model::create_plan_dir(&args.plan_dir, &home, &store_root)?;
        let started = chrono::Utc::now().to_rfc3339();
        let plan = runtime.block_on(inventory::build(&args.home, &opened, &started))?;
        prove_stopped(args)?;
        model::write_private(&args.plan_dir, model::PLAN_FILE, &plan)?;
        model::Report {
            event: "file_blob_gc_offline",
            execute: false,
            counts: plan.counts,
            ledger_committed: false,
            file_blobs_deleted: 0,
            compactions: Vec::new(),
            atom_retirement_state_unchanged: true,
            fresh_snapshot_required: false,
            prerequisites: plan.prerequisites,
            pre_blob_snapshot_writer_map: plan.pre_blob_snapshot_writer_map,
            csn_before: 0,
            csn_after: 0,
        }
    };
    if args.json {
        println!("{}", serde_json::to_string(&report).map_err(err)?);
    } else {
        println!(
            "file blobs: read={} referenced={} recent={} undated={} candidates={} deleted={}",
            report.counts.file_blobs_read,
            report.counts.file_blobs_referenced,
            report.counts.file_blobs_recent,
            report.counts.file_blobs_undated,
            report.counts.candidate_rows,
            report.file_blobs_deleted,
        );
    }
    Ok(())
}

fn prove_stopped(args: &FileBlobGcArgs) -> Result<(), String> {
    let store_root = resolve_laststore_root(&args.home)?;
    let sockets = guard::socket_paths(&args.home, &store_root);
    let view = guard::live_process_view(&sockets);
    guard::prove(
        &args.home,
        &store_root,
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
