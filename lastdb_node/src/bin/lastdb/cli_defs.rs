//! Declarative Clap schema definitions for the `lastdb` CLI.

use clap::{Args, Parser, Subcommand};
use std::path::PathBuf;

#[path = "cli_defs/cloud.rs"]
mod cloud;
#[path = "cli_defs/share.rs"]
mod share;

pub(super) use cloud::*;
pub(super) use share::*;

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
    RecoverCredentialsHome {
        #[command(flatten)]
        args: RecoverCredentialsArgs,
        #[arg(long, conflicts_with = "invite_code_file")]
        existing_dev: bool,
    },
    PrepareRecoveryHome(PrepareRecoveryArgs),
}

#[derive(Args, Debug)]
pub(crate) struct PublishArgs {
    #[arg(long)]
    pub manifest: PathBuf,
    #[arg(long)]
    pub env: Option<String>,
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
}

#[derive(Args, Debug)]
pub(crate) struct CatalogSource {
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
    Mutate {
        #[arg(long)]
        request_file: PathBuf,
        #[arg(long)]
        durable: bool,
        #[arg(long)]
        mutation_id: Option<uuid::Uuid>,
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
        #[arg(long, value_name = "FILE")]
        hydrate: Option<PathBuf>,
        #[arg(long, value_name = "FILE")]
        hydrate_inline: Option<PathBuf>,
    },
    #[command(subcommand)]
    App(AppCommand),
}

#[derive(Subcommand, Debug)]
pub(super) enum DatabaseCommand {
    Create {
        name: String,
        #[arg(long, conflicts_with_all = ["spec_name", "spec_version"])]
        private: bool,
        #[arg(long, requires = "spec_version")]
        spec_name: Option<String>,
        #[arg(long, requires = "spec_name")]
        spec_version: Option<u32>,
        #[arg(long)]
        json: bool,
    },
    List {
        #[arg(long)]
        json: bool,
    },
    Select {
        selector: String,
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
}

#[derive(Subcommand, Debug)]
pub(crate) enum BackupCommand {
    Connect(BackupConnectArgs),
    Restore(BackupRestoreArgs),
    /// Queue a direct encrypted snapshot of the selected database.
    Snapshot {
        #[arg(long)]
        database: Option<String>,
        /// Return after the signed snapshot read shows through equal to the log head.
        #[arg(long)]
        wait: bool,
    },
}
