//! Clap subcommands for `lastdb app` and `lastdb app index`.

use super::*;

#[derive(Subcommand, Debug)]
pub(crate) enum AppIndexCommand {
    /// Verify a registry list file against its detached signature.
    Verify {
        #[arg(long)]
        index: PathBuf,
        /// Signature file (default: `<index>.sig`).
        #[arg(long)]
        sig: Option<PathBuf>,
        /// Trusted verifying key (base64 or file). Default: the pinned registry list key.
        #[arg(long)]
        trust_key: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Print the pinned registry list verifying key this binary trusts.
    TrustKey,
}

#[path = "app/catalog.rs"]
mod catalog;
#[path = "app/lifecycle.rs"]
mod lifecycle;
#[path = "app/managed.rs"]
mod managed;
pub(crate) use managed::AppManagedCommand;

pub(crate) use catalog::AppCatalogCommand;
pub(crate) use lifecycle::AppLifecycleCommand;

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
