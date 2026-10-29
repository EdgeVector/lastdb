//! `lastdb app` subcommands that read or install the app catalog.

use super::*;

#[derive(Subcommand, Debug)]
pub(crate) enum AppCatalogCommand {
    /// List apps on the signed public index (default), or in the live
    /// registry service when `--env` / `--schema-url` is given.
    List {
        /// Legacy: read the live registry service for this environment.
        #[arg(long)]
        env: Option<String>,
        /// Legacy: explicit schema service base URL (implies the service path).
        #[arg(long)]
        schema_url: Option<String>,
        /// Index channel (`stable` | `next`). Default: stable.
        #[arg(long)]
        channel: Option<String>,
        /// Index location: URL base or directory (default: the public tap,
        /// or $LASTDB_REGISTRY_INDEX).
        #[arg(long)]
        index: Option<String>,
        /// Trusted verifying key (base64 or file). Default: the pinned release key.
        #[arg(long)]
        trust_key: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Live storage grouped by owning app (`owner_app_id`), read from the
    /// keep-small projection on the local node.
    ///
    /// Cheap: point reads of the write-path meters, not a store walk. Use
    /// this instead of `lastdb db inventory` for the daily "who is using the
    /// space" question. Reserved rows `system` (node-owned schemas) and
    /// `unattributed` (bytes the projection cannot attribute yet) are listed
    /// separately, and `complete` is false whenever `unattributed` is nonzero.
    Storage {
        /// Emit raw JSON only.
        #[arg(long)]
        json: bool,
        /// Run one bounded reconciliation page, then print the storage report.
        #[arg(long)]
        reconcile: bool,
        /// Layouts visited by `--reconcile` (default 32, cap 256).
        #[arg(long)]
        page_size: Option<u64>,
    },
    /// Show one app's row on the signed public index (default), or its live
    /// registry record when `--env` / `--schema-url` is given.
    Info {
        app_id: String,
        /// Legacy: read the live registry service for this environment.
        #[arg(long)]
        env: Option<String>,
        /// Legacy: explicit schema service base URL (implies the service path).
        #[arg(long)]
        schema_url: Option<String>,
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
    /// Resolve the one proved row for an app on this node: the newest compat
    /// row whose `lastdb_version` names the running node build. Anonymous.
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
        /// Node build string to resolve for (default: the running node, then
        /// this binary).
        #[arg(long)]
        lastdb_version: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Sign, verify, or show a static index file.
    #[command(subcommand)]
    Index(AppIndexCommand),
    /// Install one or more apps by proof: the newest compat row on the signed
    /// index proved with the running node. Legacy service path with
    /// `--env` / `--schema-url` (one app).
    Install {
        /// App ids (`brain kanban situations`).
        #[arg(required = true)]
        app_ids: Vec<String>,
        /// Legacy: install from the live registry service for this environment.
        #[arg(long)]
        env: Option<String>,
        /// Legacy: explicit schema service base URL (implies the service path).
        #[arg(long)]
        schema_url: Option<String>,
        /// Index channel (`stable` | `next`). Default: stable.
        #[arg(long)]
        channel: Option<String>,
        /// Index location: URL base or directory.
        #[arg(long)]
        index: Option<String>,
        /// Trusted verifying key (base64 or file). Default: the pinned release key.
        #[arg(long)]
        trust_key: Option<String>,
        /// Node build string to resolve for (default: the running node).
        #[arg(long)]
        lastdb_version: Option<String>,
        /// Install directory (default: <home>/apps/<app_id>). One app only.
        #[arg(long)]
        dir: Option<PathBuf>,
        /// Legacy: allow installing sandbox records.
        #[arg(long)]
        allow_sandbox: bool,
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
    /// Upgrade installed apps to the newest proved row for the running node.
    /// Legacy service path with `--env` / `--schema-url` (one app).
    Upgrade {
        /// App ids (`brain kanban situations`).
        #[arg(required = true)]
        app_ids: Vec<String>,
        /// Legacy: upgrade from the live registry service for this environment.
        #[arg(long)]
        env: Option<String>,
        /// Legacy: explicit schema service base URL (implies the service path).
        #[arg(long)]
        schema_url: Option<String>,
        /// Index channel (`stable` | `next`). Default: stable.
        #[arg(long)]
        channel: Option<String>,
        /// Index location: URL base or directory.
        #[arg(long)]
        index: Option<String>,
        /// Trusted verifying key (base64 or file). Default: the pinned release key.
        #[arg(long)]
        trust_key: Option<String>,
        /// Node build string to resolve for (default: the running node).
        #[arg(long)]
        lastdb_version: Option<String>,
        /// Install directory (default: <home>/apps/<app_id>). One app only.
        #[arg(long)]
        dir: Option<PathBuf>,
        /// Legacy: allow installing/upgrading sandbox records.
        #[arg(long)]
        allow_sandbox: bool,
        /// Emit raw JSON only.
        #[arg(long)]
        json: bool,
    },
}
