//! Clap definitions for the `lastdb` CLI.

use super::*;

#[path = "cli_defs/cloud.rs"]
mod cloud;
#[path = "cli_defs/share.rs"]
mod share;

pub(super) use cloud::*;
pub(super) use share::*;

#[derive(Subcommand, Debug)]
pub(crate) enum AdminCommand {
    /// Reclaim deleted space in active stores.
    #[command(subcommand)]
    Storage(AdminStorageCommand),
    /// Prepare recovery credentials and operate on exact backup jobs.
    #[command(subcommand)]
    Backup(AdminBackupCommand),
}

#[derive(Subcommand, Debug)]
pub(crate) enum AdminStorageCommand {
    /// Reclaim already-deleted v2 data in both active stores.
    Cleanup {
        #[arg(long)]
        status: bool,
        #[arg(long)]
        wait: bool,
        #[arg(long, default_value_t = 600, value_parser = clap::value_parser!(u64).range(1..=86400))]
        timeout: u64,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand, Debug)]
pub(crate) enum AdminBackupCommand {
    /// Create one fresh DEV identity without starting a daemon.
    Init(BackupInitArgs),
    /// Register a credentials-only recovery device without opening a store.
    RecoverCredentialsHome {
        #[command(flatten)]
        args: RecoverCredentialsArgs,
        #[arg(long, conflicts_with = "invite_code_file")]
        existing_dev: bool,
    },
    /// Prepare recovery credentials from an existing account without registering.
    PrepareRecoveryHome(PrepareRecoveryArgs),
}

#[derive(Subcommand, Debug)]
pub(crate) enum AppLifecycleCommand {
    /// Publish this clean git commit for the running node's exact API version.
    Publish {
        #[arg(long)]
        manifest: PathBuf,
        /// Target registry environment (dev | prod). Default: prod.
        #[arg(long)]
        env: Option<String>,
        #[arg(long)]
        key_file: Option<PathBuf>,
        /// Developer API key (default: $EXEMEM_DEV_API_KEY).
        #[arg(long)]
        api_key: Option<String>,
        /// Registry HTTP API base URL (default: $LASTDB_APP_REGISTRY_URL).
        #[arg(long)]
        registry_url: Option<String>,
        /// Signed-list channel. Default: stable.
        #[arg(long)]
        channel: Option<String>,
        /// Validate this checkout against the running node and write an unsigned declaration.
        #[arg(long, conflicts_with_all = ["key_file", "api_key", "registry_url"])]
        prepare: Option<PathBuf>,
    },
}

#[derive(clap::Args, Debug)]
pub(crate) struct RecordListArgs {
    pub schema: String,
    #[arg(long)]
    pub key_hash: Option<String>,
    #[arg(long, requires = "key_hash")]
    pub key_name: Option<String>,
    #[arg(long, default_value_t = 100)]
    pub limit: usize,
    #[arg(long)]
    pub cursor: Option<String>,
    #[arg(long)]
    pub json: bool,
}

#[derive(Parser, Debug)]
#[command(
    name = "lastdb",
    about = "Tiny LastDB control CLI for LastDB Mini",
    version = env!("FOLDDB_BUILD_VERSION")
)]
pub(super) struct Cli {
    #[arg(long)]
    pub(super) data_dir: Option<PathBuf>,
    #[arg(long, value_name = "ID_OR_NAME")]
    pub(super) db: Option<String>,
    #[command(subcommand)]
    pub(super) command: Option<Command>,
}

#[derive(Subcommand, Debug)]
pub(super) enum Command {
    /// Manage personal backups and restore an accepted recovery point.
    Backup {
        #[command(subcommand)]
        action: BackupCommand,
    },
    /// Invite readers and manage named database sharing.
    #[command(subcommand)]
    Share(ShareCommand),
    /// Create, list, and select databases on this node.
    Db {
        #[command(subcommand)]
        action: DatabaseCommand,
    },
    /// Diagnose, maintain, or recover a LastDB home.
    Admin {
        #[command(subcommand)]
        action: AdminCommand,
    },
    /// Show whether the local daemon socket is reachable.
    Status {
        #[arg(long)]
        json: bool,
        #[arg(long)]
        timeout: Option<u64>,
    },
    /// Submit a complete conditional mutation from JSON, or '-' for stdin.
    Mutate {
        #[arg(long)]
        request_file: PathBuf,
        #[arg(long)]
        durable: bool,
        #[arg(long)]
        mutation_id: Option<uuid::Uuid>,
    },
    /// List one page of primary keys, or named alternate keys under --key-hash.
    GetKeys(RecordListArgs),
    /// Point-get one record by exact hash (and range when the schema is HashRange).
    Get {
        schema: String,
        #[arg(long)]
        key_hash: Option<String>,
        #[arg(long)]
        key_range: Option<String>,
        #[arg(long)]
        json: bool,
        #[arg(long, value_name = "FILE")]
        hydrate: Option<PathBuf>,
        #[arg(long, value_name = "FILE")]
        hydrate_inline: Option<PathBuf>,
    },
    /// Discover, install, and publish apps through the signed registry.
    App {
        #[command(subcommand)]
        action: AppCommand,
    },
}

#[derive(Subcommand, Debug)]
pub(super) enum DatabaseCommand {
    /// Create an owned database. Personal stays the unscoped default.
    Create {
        name: String,
        /// Allow only the owner to sync or read this database.
        #[arg(long, conflicts_with_all = ["spec_name", "spec_version"])]
        private: bool,
        #[arg(long, requires = "spec_version")]
        spec_name: Option<String>,
        #[arg(long, requires = "spec_name")]
        spec_version: Option<u32>,
        #[arg(long)]
        json: bool,
    },
    /// List the personal database, owned databases, and received shares.
    List {
        #[arg(long)]
        json: bool,
    },
    /// Show the database a name or id selects. This does not change the default.
    Select {
        selector: String,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand, Debug)]
pub(crate) enum AppManagedCommand {
    /// Show actual installed versions and command-link health.
    Status {
        app_id: Option<String>,
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

#[derive(Subcommand, Debug)]
pub(crate) enum AppCatalogCommand {
    /// List apps on the signed public index or resolve one for this node.
    List {
        #[arg(value_name = "APP")]
        app_id: Option<String>,
        #[arg(long, requires = "app_id")]
        catalog_entry: bool,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long)]
        index: Option<String>,
        #[arg(long)]
        trust_key: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Install apps from the signed list for this node's exact API version.
    Install {
        #[arg(required = true)]
        app_ids: Vec<String>,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long)]
        index: Option<String>,
        #[arg(long)]
        trust_key: Option<String>,
        #[arg(long)]
        accept_install: bool,
        #[arg(long)]
        inactive: bool,
        #[arg(long)]
        json: bool,
    },
    /// Upgrade installed apps from the list for the running node API.
    Upgrade {
        #[arg(required_unless_present = "all")]
        app_ids: Vec<String>,
        #[arg(long, conflicts_with = "app_ids")]
        all: bool,
        #[arg(long)]
        accept_install: bool,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long)]
        index: Option<String>,
        #[arg(long)]
        trust_key: Option<String>,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand, Debug)]
pub(crate) enum AppCommand {
    #[command(flatten)]
    Managed(AppManagedCommand),
    #[command(flatten)]
    Lifecycle(AppLifecycleCommand),
    #[command(flatten)]
    Catalog(AppCatalogCommand),
}

#[derive(Subcommand, Debug)]
pub(crate) enum BackupCommand {
    /// Connect a prepared account without enabling automatic backup.
    Connect(BackupConnectArgs),
    /// Restore a signed owner database into a fresh home.
    Restore(BackupRestoreArgs),
}

impl BackupCommand {
    pub(crate) fn into_cloud(self) -> Result<CloudCommand, String> {
        let action = match self {
            Self::Connect(a) => ByteBackupCommand::Connect(a),
            Self::Restore(a) => ByteBackupCommand::Restore(a),
        };
        Ok(CloudCommand::BackupV2 { action })
    }
}
