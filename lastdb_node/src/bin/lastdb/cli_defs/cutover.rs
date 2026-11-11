//! Per-database cutover. Verify reads evidence. Request sends one receipt.
//!
//! Both commands use the same checks in every realm. Neither opens the store
//! or writes a local receipt.

use super::*;

#[derive(Subcommand, Debug)]
pub(crate) enum CutoverCommand {
    /// Check one existing database's evidence. This writes nothing and activates nothing.
    Verify(CutoverEvidenceArgs),
    /// Send the publisher cutover for one existing database.
    ///
    /// The realm is the saved cloud connection. The command writes nothing locally.
    Request(CutoverRequestArgs),
}

#[derive(Args, Debug)]
pub(crate) struct CutoverRequestArgs {
    /// Database to cut over. Every evidence file must name this id.
    #[arg(long)]
    pub database_id: String,
    /// Directory of the six evidence files.
    #[arg(long = "evidence")]
    pub directory: PathBuf,
    /// Print the live receipt as JSON.
    #[arg(long)]
    pub json: bool,
}

#[derive(Args, Debug)]
pub(crate) struct CutoverEvidenceArgs {
    /// Database to check. Every evidence file must name this id.
    #[arg(long)]
    pub database_id: String,
    /// Realm of this check: `dev` or `prod`. The same checks apply to both.
    #[arg(long)]
    pub environment: String,
    /// Directory of the six evidence files.
    #[arg(long = "evidence")]
    pub directory: PathBuf,
    /// Print the decision as JSON.
    #[arg(long)]
    pub json: bool,
}
