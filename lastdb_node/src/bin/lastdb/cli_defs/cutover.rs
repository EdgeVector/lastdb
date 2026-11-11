//! Per-database cutover evidence check. It does not open a data directory.
//!
//! The same command is the preparation step in every realm. It does not call
//! Exemem and it does not commit signed-only.

use super::*;

#[derive(Subcommand, Debug)]
pub(crate) enum CutoverCommand {
    /// Check one existing database's evidence. This writes nothing and activates nothing.
    Verify(CutoverEvidenceArgs),
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
