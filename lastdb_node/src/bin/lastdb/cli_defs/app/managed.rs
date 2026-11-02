//! Registry-managed activation commands.
use super::*;

#[derive(Subcommand, Debug)]
pub(crate) enum AppManagedCommand {
    /// Show actual installed versions and command-link health.
    Status {
        app_id: Option<String>,
        /// Run the installed publisher probe (inactive apps are skipped).
        #[arg(long)]
        check: bool,
        #[arg(long)]
        json: bool,
    },
    /// Restore the previous retained app version and its service hooks.
    Rollback {
        app_id: String,
        #[arg(long)]
        json: bool,
    },
}
