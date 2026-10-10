//! Genuine normal publication from a clean stopped primary, without node workers.
//!
//! This is the normal v1 publisher. It does not restore, bootstrap, replay,
//! capture mutations, compact data, or alter the maintenance frontier gates.

use clap::Args;
use std::path::PathBuf;

mod admission;
mod engine;
mod historical_claim;
mod io;
mod model;
mod publish;

#[derive(Args, Debug, Clone)]
pub(crate) struct NormalSnapshotArgs {
    #[arg(skip)]
    pub home: PathBuf,
    /// New private artifact directory outside the primary home.
    #[arg(long)]
    pub report_dir: PathBuf,
    #[arg(long)]
    pub expected_pid: u32,
    #[arg(long)]
    pub expected_start_ts: u64,
    #[arg(long)]
    pub expected_build_version: String,
    #[arg(long)]
    pub cloud_config_sha256: String,
    /// Canonical SHA of the already committed normal predecessor manifest.
    #[arg(long)]
    pub previous_manifest_sha256: String,
    /// Private owner evidence for held controls and the preserved rollback.
    #[arg(long)]
    pub operator_evidence_file: PathBuf,
    #[arg(long)]
    pub operator_evidence_sha256: String,
    #[arg(long)]
    pub stopped_primary: bool,
    #[arg(long)]
    pub i_know_this_is_primary: bool,
    #[arg(long)]
    pub json: bool,
}

pub(crate) fn run(args: &NormalSnapshotArgs) -> Result<(), String> {
    admission::stopped(args)?;
    let inputs = admission::load(args)?;
    io::create_dir(&args.report_dir, &inputs.home, &inputs.store_root)?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(3)
        .enable_all()
        .build()
        .map_err(err)?;
    let report = runtime.block_on(publish::run(args, &inputs))?;
    if args.json {
        println!("{}", serde_json::to_string(&report).map_err(err)?);
    } else {
        println!("normal stopped-home snapshot complete; use --json for numeric counts.");
    }
    Ok(())
}

fn err(error: impl std::fmt::Display) -> String {
    error.to_string()
}
