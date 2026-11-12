//! Clap definitions for the `lastdb` CLI.

use super::*;

#[path = "cli_defs/admin.rs"]
mod admin;
#[path = "cli_defs/cloud.rs"]
mod cloud;
#[path = "cli_defs/app/lifecycle.rs"]
mod lifecycle;
#[path = "cli_defs/share.rs"]
mod share;

pub(super) use admin::*;
pub(super) use cloud::*;
pub(crate) use lifecycle::AppLifecycleCommand;
pub(super) use share::*;

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

pub(super) fn durable_mutation_client_timeout() -> Duration {
    lastdb_node::exec::handler_timeout().saturating_add(Duration::from_secs(20))
}

pub(super) fn admin_scan_client_timeout() -> Duration {
    lastdb_node::exec::admin_handler_timeout()
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
        /// Spec catalog name. Required together with --spec-version.
        #[arg(long, requires = "spec_version")]
        spec_name: Option<String>,
        /// Spec catalog version, an integer >= 1.
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
    /// Read backup policy, job state and last acceptance from the owner daemon.
    Status(BackupJsonArgs),
    /// Connect a prepared account without enabling automatic backup.
    Connect(BackupConnectArgs),
    /// Enable or disable automatic backup explicitly.
    Policy(BackupPolicyArgs),
    /// Start one manual backup on the owner daemon.
    Start(BackupStartArgs),
    /// List accepted stream heads. Prepare credentials with `admin backup` first.
    List(BackupListArgs),
    /// Restore an accepted cut into a fresh home, or resume a saved workspace.
    Restore(BackupRestoreSelection),
}

impl BackupCommand {
    pub(crate) fn into_cloud(self) -> Result<CloudCommand, String> {
        let action = match self {
            Self::Status(args) => ByteBackupCommand::Status(args),
            Self::Connect(args) => ByteBackupCommand::Connect(args),
            Self::Policy(args) => args.into_action()?,
            Self::Start(args) => ByteBackupCommand::Start(args),
            Self::List(args) => ByteBackupCommand::Discover(args),
            Self::Restore(selection) => ByteBackupCommand::Restore(selection.validate()?),
        };
        Ok(CloudCommand::BackupV2 { action })
    }
}

#[derive(Args, Debug)]
pub(crate) struct BackupRestoreSelection {
    #[command(flatten)]
    args: BackupRestoreArgs,
    #[arg(long, conflicts_with_all = ["resume", "commit", "commit_sha256"])]
    latest: bool,
}

impl BackupRestoreSelection {
    fn validate(self) -> Result<BackupRestoreArgs, String> {
        let args = self.args;
        if args.resume.is_none()
            && !self.latest
            && (args.generation.is_none() || args.commit.is_none() || args.commit_sha256.is_none())
        {
            return Err(
                "select --generation, --commit and --commit-sha256, or use --latest".into(),
            );
        }
        Ok(args)
    }
}

#[derive(Clone, Copy, Debug, clap::ValueEnum)]
enum PolicyMode {
    On,
    Off,
}

#[derive(Args, Debug)]
pub(crate) struct BackupPolicyArgs {
    #[arg(value_enum)]
    mode: PolicyMode,
    #[arg(long)]
    log_interval_seconds: Option<u64>,
    #[arg(long)]
    snapshot_interval_seconds: Option<u64>,
    #[arg(long)]
    json: bool,
}

impl BackupPolicyArgs {
    fn into_action(self) -> Result<ByteBackupCommand, String> {
        match self.mode {
            PolicyMode::On => Ok(ByteBackupCommand::EnableAutomatic(BackupEnableArgs {
                log_interval_seconds: self.log_interval_seconds.unwrap_or(60),
                snapshot_interval_seconds: self.snapshot_interval_seconds.unwrap_or(86400),
                json: self.json,
            })),
            PolicyMode::Off => {
                if self.log_interval_seconds.is_some() || self.snapshot_interval_seconds.is_some() {
                    return Err("interval options require backup policy on".into());
                }
                Ok(ByteBackupCommand::DisableAutomatic(BackupJsonArgs {
                    json: self.json,
                }))
            }
        }
    }
}
