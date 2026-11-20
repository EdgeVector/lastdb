//! Declarative Clap schema definitions for the `lastdb` CLI.

use clap::{Args, Parser, Subcommand};
use std::path::PathBuf;

#[path = "cli_defs/cloud.rs"]
mod cloud;

pub(super) use cloud::*;

#[derive(Subcommand, Debug)]
pub(crate) enum AdminCommand {
    #[command(subcommand)]
    Storage(AdminStorageCommand),
    #[command(subcommand)]
    Backup(AdminBackupCommand),
}

#[derive(Subcommand, Debug)]
pub(crate) enum AdminStorageCommand {
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
    Init(BackupInitArgs),
    RecoverCredentialsHome(RecoverCredentialsArgs),
}

#[derive(Args, Debug)]
pub(crate) struct PublishArgs {
    #[arg(long)]
    pub manifest: PathBuf,
    #[arg(long)]
    pub env: Option<String>,
    /// Existing publisher signing key. Use - to read it from stdin.
    #[arg(long)]
    pub key_file: Option<PathBuf>,
    #[arg(long)]
    pub api_key: Option<String>,
    #[arg(long)]
    pub registry_url: Option<String>,
    #[arg(long)]
    pub channel: Option<String>,
    #[arg(long, conflicts_with_all = ["key_file", "api_key", "registry_url"])]
    pub prepare: Option<PathBuf>,
    /// Machine-readable output. The Schema Service outcome is a `schema_service` member.
    #[arg(long)]
    pub json: bool,
}

#[derive(Args, Debug)]
pub(crate) struct CatalogSource {
    #[arg(long, value_parser = ["dev", "prod"])]
    pub env: Option<String>,
    #[arg(long)]
    pub channel: Option<String>,
    #[arg(long)]
    pub index: Option<String>,
    #[arg(long)]
    pub trust_key: Option<String>,
}

#[derive(Args, Debug)]
pub(crate) struct RecordListArgs {
    pub schema: String,
    #[arg(long)]
    pub key_hash: Option<String>,
    #[arg(long, default_value_t = 100)]
    pub limit: usize,
    #[arg(long)]
    pub cursor: Option<String>,
    #[arg(long)]
    pub json: bool,
}

#[derive(Parser, Debug)]
#[command(name = "lastdb", about = "Tiny LastDB control CLI for LastDB Mini", version = env!("FOLDDB_BUILD_VERSION"))]
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
    #[command(subcommand)]
    Backup(BackupCommand),
    #[command(subcommand)]
    Cloud(CloudAccountCommand),
    #[command(subcommand)]
    Share(ShareCommand),
    #[command(subcommand)]
    Db(DatabaseCommand),
    #[command(subcommand)]
    Admin(AdminCommand),
    Status {
        #[arg(long)]
        json: bool,
        #[arg(long)]
        timeout: Option<u64>,
    },
    /// Show completed requests, by caller and by schema.
    ///
    /// The counts stay in memory. They start at zero when the node starts,
    /// and a backup does not include them. The call table holds at most 256
    /// rows. A further new caller and route counts on `{overflow}` and
    /// `other`. The schema table holds at most 256 rows and folds the same
    /// way. At most 32 unknown paths are kept. A further unknown path keeps
    /// its caller and uses `other`.
    Ops {
        /// Sort by request count. The default sort is total time.
        #[arg(long)]
        by_count: bool,
        /// Print the route JSON on stdout and nothing else.
        #[arg(long)]
        json: bool,
    },
    /// Write one conditional record.
    ///
    /// The request names a file by path. The command does not copy the file,
    /// and the file must stay where it is. On a replace, `"file": null` clears
    /// the record's file. A request with no `file` key sends no file.
    Mutate {
        #[arg(long)]
        request_file: PathBuf,
        #[arg(long)]
        durable: bool,
    },
    GetKeys(RecordListArgs),
    Get {
        schema: String,
        #[arg(long)]
        key_hash: Option<String>,
        #[arg(long)]
        key_range: Option<String>,
        #[arg(long)]
        json: bool,
        /// Folder the node writes the record's file into. A relative path is
        /// resolved to an absolute path. The folder must already exist. This
        /// command does not create it.
        #[arg(long, value_name = "DIR")]
        hydrate: Option<PathBuf>,
    },
    #[command(subcommand)]
    App(AppCommand),
}

#[derive(Subcommand, Debug)]
pub(super) enum DatabaseCommand {
    Create {
        name: String,
        #[arg(long)]
        private: bool,
        #[arg(long)]
        json: bool,
    },
    List {
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand, Debug)]
pub(crate) enum AppCommand {
    Status {
        app_id: Option<String>,
        #[arg(long)]
        check: bool,
        #[arg(long)]
        json: bool,
    },
    Rollback {
        app_id: String,
        #[arg(long)]
        json: bool,
    },
    /// Start a command of an installed app on the app socket.
    Run {
        app_id: String,
        command: String,
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<std::ffi::OsString>,
    },
    Publish(PublishArgs),
    List {
        #[arg(value_name = "APP")]
        app_id: Option<String>,
        #[arg(long, requires = "app_id")]
        catalog_entry: bool,
        #[command(flatten)]
        source: CatalogSource,
        #[arg(long)]
        json: bool,
    },
    Install {
        #[arg(required = true)]
        app_ids: Vec<String>,
        #[command(flatten)]
        source: CatalogSource,
        #[arg(long)]
        accept_install: bool,
        #[arg(long)]
        inactive: bool,
        #[arg(long)]
        json: bool,
    },
    Upgrade {
        #[arg(required_unless_present = "all")]
        app_ids: Vec<String>,
        #[arg(long, conflicts_with = "app_ids")]
        all: bool,
        #[arg(long)]
        accept_install: bool,
        #[command(flatten)]
        source: CatalogSource,
        #[arg(long)]
        json: bool,
    },
    /// Remove one installed app.
    ///
    /// Drops the app's grant and deletes the command links this app installed,
    /// every version directory, the current and previous links, and the install
    /// receipt. Schemas and records stay. The app data folder stays unless
    /// `--delete-data` is set. There is no flag that deletes schemas or records.
    /// An app that is not installed is an error, except a later run that only
    /// has a grant left to drop. If the node is not running, the files are still
    /// removed and the command says the grant was not. Run it again when the
    /// node is up. An app whose install ran `post_install` steps is refused:
    /// the manifest has no uninstall step. Prints JSON.
    Remove {
        app_id: String,
        /// Also remove the app data folder. Schemas and records stay.
        #[arg(long)]
        delete_data: bool,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand, Debug)]
pub(crate) enum BackupCommand {
    Connect(BackupConnectArgs),
    Restore(BackupRestoreArgs),
}

#[derive(Subcommand, Debug)]
pub(crate) enum ShareCommand {
    /// Register open material as a received signed-log database on this node.
    Accept {
        /// Opaque open material (`lastdb-read-v1.` token). People may wrap this as a consent code.
        #[arg(long)]
        link: String,
        /// Optional local database name.
        #[arg(long)]
        name: Option<String>,
    },
    /// Mint local open material for one shareable database (no ExeMem call).
    Link {
        /// Database id or name.
        database: String,
    },
}
