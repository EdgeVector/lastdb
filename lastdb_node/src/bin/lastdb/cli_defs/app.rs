//! Clap subcommands for `lastdb app` and `lastdb app index`.

use super::*;

#[derive(Subcommand, Debug)]
pub(crate) enum AppIndexCommand {
    /// Verify an index file against its detached signature.
    Verify {
        #[arg(long)]
        index: PathBuf,
        /// Signature file (default: `<index>.sig`).
        #[arg(long)]
        sig: Option<PathBuf>,
        /// Trusted verifying key (base64 or file). Default: the pinned release key.
        #[arg(long)]
        trust_key: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Print the pinned release verifying key this binary trusts.
    TrustKey,
}

#[path = "app/catalog.rs"]
mod catalog;
#[path = "app/lifecycle.rs"]
mod lifecycle;

pub(crate) use catalog::AppCatalogCommand;
pub(crate) use lifecycle::AppLifecycleCommand;

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

/// `lastdb app` subcommands. Grouped enums are flattened so each command
/// keeps the name it had when every variant lived on one enum.
#[derive(Subcommand, Debug)]
pub(crate) enum AppCommand {
    #[command(flatten)]
    Managed(AppManagedCommand),
    #[command(flatten)]
    Lifecycle(AppLifecycleCommand),
    #[command(flatten)]
    Catalog(AppCatalogCommand),
}
