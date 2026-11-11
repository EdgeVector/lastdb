//! Clap definitions for the `lastdb` CLI: the top-level `Cli`, every
//! subcommand enum, and the small value-enum argument types.

use super::*;

#[path = "cli_defs/admin.rs"]
mod admin;
#[path = "cli_defs/app.rs"]
mod app;
#[path = "cli_defs/cloud.rs"]
mod cloud;
#[path = "cli_defs/share.rs"]
mod share;

pub(super) use admin::*;
pub(super) use app::*;
pub(super) use cloud::*;
pub(super) use share::*;

/// Shared list/get-keys input; a named alternate always has a selected hash.
#[derive(clap::Args, Debug)]
pub(crate) struct RecordListArgs {
    pub schema: String,
    #[arg(long)]
    pub key_hash: Option<String>,
    /// Named alternate key. The default uses the schema's primary key.
    #[arg(long, requires = "key_hash")]
    pub key_name: Option<String>,
    #[arg(long, default_value_t = 100)]
    pub limit: usize,
    #[arg(long)]
    pub cursor: Option<String>,
    #[arg(long)]
    pub json: bool,
}

#[derive(Subcommand, Debug)]
pub(crate) enum ServiceHomeCommand {
    /// Persist the home directory service starts should use.
    Set {
        /// Absolute path, or a path beginning with ~/; must not resolve to ~/.lastdb.
        home: PathBuf,
    },
    /// Show the persisted service home, if one is configured.
    Show,
    /// Clear the persisted service home.
    Clear,
}

/// Client headroom above the server's mutation route budget.
pub(super) const MUTATION_CLIENT_HEADROOM_SECS: u64 = 20;

/// Client deadline for a durable mutation: the server's handler budget, which
/// the same env var sets, plus headroom for the response.
pub(super) fn durable_mutation_client_timeout() -> Duration {
    lastdb_node::exec::handler_timeout()
        .saturating_add(Duration::from_secs(MUTATION_CLIENT_HEADROOM_SECS))
}

/// Client-side deadline for admin scans, from the **same** env var the server
/// reads.
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
    /// Node home directory (default: LASTDB_HOME / service-home / ~/.lastdb).
    #[arg(long)]
    pub(super) data_dir: Option<PathBuf>,

    /// Database ID or local name. Defaults to the node's local database.
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
    /// Publish a separate database, invite readers, and manage contributor grants.
    #[command(subcommand)]
    Share(ShareCommand),
    /// Discover databases on this node.
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
    ///
    /// Exit 0 when the owner socket answers `/health`. Exit 1 when it does
    /// not; the first human line is then `lastdbd: not reachable — <reason>`.
    /// `--json` adds `"running": false` and the reason, and also exits 1.
    Status {
        /// Emit the machine-readable summary (`running`, builds).
        #[arg(long)]
        json: bool,
        /// Seconds to wait for the daemon's status read (default: 30, or
        /// `LASTDB_UDS_ADMIN_TIMEOUT_SECS` when that is exported).
        #[arg(long)]
        timeout: Option<u64>,
    },
    /// Submit a complete conditional mutation from JSON, or '-' for stdin.
    ///
    /// To supply the record's file, use an object with `mutation` and
    /// `file: { source, name?, media_type? }`. The source path is relative
    /// to the request file, or to the working directory when reading stdin.
    Mutate {
        #[arg(long)]
        request_file: PathBuf,
        #[arg(long)]
        durable: bool,
        /// Stable UUID for retrying a submitted contribution.
        #[arg(long)]
        mutation_id: Option<uuid::Uuid>,
    },
    /// List one page of primary keys, or named alternate keys under --key-hash.
    GetKeys(RecordListArgs),
    /// Point-get one record by exact hash (and range when the schema is HashRange).
    ///
    /// Wraps `POST /api/query` with a HashKey / HashRangeKey filter. Refuses a
    /// body that would scan. HashRange schemas need `--key-range`.
    Get {
        /// Schema identity hash, `app/Name`, or descriptive name.
        schema: String,
        /// Hash half of the key.
        #[arg(long)]
        key_hash: Option<String>,
        /// Range half of the key (required when the schema is HashRange).
        #[arg(long)]
        key_range: Option<String>,
        /// Emit raw JSON only.
        #[arg(long)]
        json: bool,
        /// Hydrate the same record version and write its verified file to a new path.
        #[arg(long, value_name = "FILE")]
        hydrate: Option<PathBuf>,
        /// Fetch the inline file of the same record version and write it,
        /// verified, to a new path.
        #[arg(long, value_name = "FILE")]
        hydrate_inline: Option<PathBuf>,
    },
    /// Inspect one installed schema's key layout and legal query shapes.
    Schema {
        #[command(subcommand)]
        action: SchemaInspectCommand,
    },
    /// Discover, install, and publish apps through the signed registry.
    App {
        #[command(subcommand)]
        action: AppCommand,
    },
}

#[derive(Subcommand, Debug)]
pub(super) enum DatabaseCommand {
    /// List the local database and registered shares.
    List {
        /// Emit JSON instead of a short list.
        #[arg(long)]
        json: bool,
    },
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
    ///
    /// Prepare credentials with `admin backup recover-credentials-home` or
    /// `prepare-recovery-home`, then select a cut from `backup list`.
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
    /// Select the current accepted head once, before staging the restore.
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
    /// Time between log checks while enabled (default: 60 seconds).
    #[arg(long)]
    log_interval_seconds: Option<u64>,
    /// Time between full snapshots while enabled (default: 86400 seconds).
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
