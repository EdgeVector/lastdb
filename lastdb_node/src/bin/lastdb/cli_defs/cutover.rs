//! DEV-only per-database cutover commands. They do not open a data directory.
//!
//! These commands prepare an inactive request. They do not commit signed-only.

use super::*;

#[derive(Subcommand, Debug)]
pub(crate) enum CutoverCommand {
    /// Check one database's DEV evidence. This writes nothing and activates nothing.
    Verify(CutoverEvidenceArgs),
    /// Write an inactive request, or repeat the same evidence. Exemem has not committed.
    Prepare(CutoverPrepareArgs),
    /// Report that a local request leaves legacy resume unchanged.
    ResumeCheck(CutoverResumeArgs),
    /// Check a second-device ack and matching keyed reads. This writes nothing.
    Proof(CutoverProofArgs),
}

#[derive(Args, Debug)]
pub(crate) struct CutoverEvidenceArgs {
    /// Database to check. Every evidence file must name this id.
    #[arg(long)]
    pub database_id: String,
    /// Directory of the six evidence files.
    #[arg(long = "evidence")]
    pub directory: PathBuf,
    /// Print the decision as JSON.
    #[arg(long)]
    pub json: bool,
}

#[derive(Args, Debug)]
pub(crate) struct CutoverPrepareArgs {
    #[command(flatten)]
    pub evidence: CutoverEvidenceArgs,
    /// Path of the inactive request. The parent must already exist.
    #[arg(long)]
    pub request: PathBuf,
}

#[derive(Args, Debug)]
pub(crate) struct CutoverResumeArgs {
    /// Database whose access this file must not change.
    #[arg(long)]
    pub database_id: String,
    /// Path of the inactive request.
    #[arg(long)]
    pub request: PathBuf,
    /// Print the decision as JSON.
    #[arg(long)]
    pub json: bool,
}

#[derive(Args, Debug)]
pub(crate) struct CutoverProofArgs {
    /// Database the second device read.
    #[arg(long)]
    pub database_id: String,
    /// Path of the inactive request.
    #[arg(long)]
    pub request: PathBuf,
    /// Second-device proof JSON.
    #[arg(long)]
    pub proof: PathBuf,
    /// Print the check as JSON.
    #[arg(long)]
    pub json: bool,
}
