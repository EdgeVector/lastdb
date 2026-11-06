//! `lastdb app` subcommands that read or install the app catalog.

use super::*;

#[derive(Subcommand, Debug)]
pub(crate) enum AppCatalogCommand {
    /// List apps on the signed public index.
    List {
        /// Index channel (`stable` | `next`). Default: stable.
        #[arg(long)]
        channel: Option<String>,
        /// Index location: S3 HTTPS base or directory (default: production S3,
        /// or $LASTDB_REGISTRY_INDEX).
        #[arg(long)]
        index: Option<String>,
        /// Trusted verifying key (base64 or file). Default: the pinned release key.
        #[arg(long)]
        trust_key: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Show one app's row on the signed public index.
    Info {
        app_id: String,
        /// Index channel (`stable` | `next`). Default: stable.
        #[arg(long)]
        channel: Option<String>,
        /// Index location: URL base or directory.
        #[arg(long)]
        index: Option<String>,
        /// Trusted verifying key (base64 or file). Default: the pinned release key.
        #[arg(long)]
        trust_key: Option<String>,
    },
    /// Resolve a publisher-signed app for this node's exact API version. Anonymous.
    Resolve {
        app_id: String,
        /// Index channel (`stable` | `next`). Default: stable.
        #[arg(long)]
        channel: Option<String>,
        /// Index location: URL base or directory.
        #[arg(long)]
        index: Option<String>,
        /// Trusted verifying key (base64 or file). Default: the pinned release key.
        #[arg(long)]
        trust_key: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Sign, verify, or show a static index file.
    #[command(subcommand)]
    Index(AppIndexCommand),
    /// Install apps from the signed list for this node's exact API version.
    Install {
        /// App ids (`brain kanban situations`).
        #[arg(required = true)]
        app_ids: Vec<String>,
        /// Index channel (`stable` | `next`). Default: stable.
        #[arg(long)]
        channel: Option<String>,
        /// Index location: URL base or directory.
        #[arg(long)]
        index: Option<String>,
        /// Trusted verifying key (base64 or file). Default: the pinned release key.
        #[arg(long)]
        trust_key: Option<String>,
        /// Install directory (default: <home>/apps/<app_id>). One app only.
        #[arg(long)]
        dir: Option<PathBuf>,
        /// Approve the pinned source install steps and command links.
        #[arg(long)]
        accept_install: bool,
        /// Install code without running probes or service hooks.
        #[arg(long)]
        inactive: bool,
        /// Replace an existing install directory.
        #[arg(long)]
        force: bool,
        /// Emit raw JSON only.
        #[arg(long)]
        json: bool,
        /// Local source: path to a git repository (requires --dev).
        #[arg(long)]
        from: Option<String>,
        /// Git revision to install (required with --from).
        #[arg(long)]
        rev: Option<String>,
        /// Allow local/dev installs (required with --from).
        #[arg(long)]
        dev: bool,
    },
    /// Upgrade installed apps from the list for the running node API.
    Upgrade {
        /// App ids (`brain kanban situations`).
        #[arg(required_unless_present = "all")]
        app_ids: Vec<String>,
        /// Upgrade every registry-managed app on its saved channel.
        #[arg(long, conflicts_with = "app_ids")]
        all: bool,
        /// Approve changed install instructions.
        #[arg(long)]
        accept_install: bool,
        /// Index channel (`stable` | `next`). Default: stable.
        #[arg(long)]
        channel: Option<String>,
        /// Index location: URL base or directory.
        #[arg(long)]
        index: Option<String>,
        /// Trusted verifying key (base64 or file). Default: the pinned release key.
        #[arg(long)]
        trust_key: Option<String>,
        /// Install directory (default: <home>/apps/<app_id>). One app only.
        #[arg(long)]
        dir: Option<PathBuf>,
        /// Emit raw JSON only.
        #[arg(long)]
        json: bool,
    },
}
