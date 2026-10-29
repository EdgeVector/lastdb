//! Clap subcommands for `lastdb app` and `lastdb app index`.

use super::*;

#[derive(Subcommand, Debug)]
pub(crate) enum AppIndexCommand {
    /// Sign an index file: writes `<index>.sig` (or `--out`).
    Sign {
        #[arg(long)]
        index: PathBuf,
        /// Ed25519 signing key file (base64 32-byte secret, `dev-init` format).
        /// Default: $LASTDB_REGISTRY_SIGNING_KEY, then <home>/registry-index-signing.key.
        #[arg(long)]
        key_file: Option<PathBuf>,
        #[arg(long)]
        out: Option<PathBuf>,
        #[arg(long)]
        json: bool,
    },
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
#[path = "app/release.rs"]
mod release;

pub(crate) use catalog::AppCatalogCommand;
pub(crate) use lifecycle::AppLifecycleCommand;
pub(crate) use release::AppReleaseCommand;

/// `lastdb app` subcommands. Grouped enums are flattened so each command
/// keeps the name it had when every variant lived on one enum.
#[derive(Subcommand, Debug)]
pub(crate) enum AppCommand {
    #[command(flatten)]
    Lifecycle(AppLifecycleCommand),
    #[command(flatten)]
    Catalog(AppCatalogCommand),
    #[command(flatten)]
    Release(AppReleaseCommand),
}
