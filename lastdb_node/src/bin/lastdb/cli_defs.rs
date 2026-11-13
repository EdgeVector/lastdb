//! Clap definitions and admin commands for the `lastdb` CLI.

use super::*;
use fold_db::storage::disk_pack::{NativeCleanupPhase, NativeCleanupStatus};

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
}

pub(super) fn run_admin(data_dir: Option<PathBuf>, action: AdminCommand) -> Result<(), String> {
    match action {
        AdminCommand::Storage(AdminStorageCommand::Cleanup {
            status,
            wait,
            timeout,
            json,
        }) => {
            let (_, socket) = resolve_client_home_and_socket(data_dir)?;
            require_reachable(&socket)?;
            db_cleanup(&socket, status, wait, timeout, json)
        }
        AdminCommand::Backup(AdminBackupCommand::Init(a)) => cloud_command(
            data_dir,
            CloudCommand::BackupV2 {
                action: ByteBackupCommand::Init(a),
            },
        ),
        AdminCommand::Backup(AdminBackupCommand::PrepareRecoveryHome(a)) => {
            cloud_command(data_dir, CloudCommand::PrepareRecoveryHome(a))
        }
        AdminCommand::Backup(AdminBackupCommand::RecoverCredentialsHome { args, existing_dev }) => {
            if !existing_dev {
                return cloud_command(data_dir, CloudCommand::RecoverCredentialsHome(args));
            }
            if args.env != "dev" {
                return Err("--existing-dev requires --env dev".into());
            }
            let cmd = CloudCommand::RecoverDevCredentialsHome {
                into: args.into,
                expected_user_hash: args.expected_user_hash,
                identity_seed_stdin: args.identity_seed_stdin,
                json: args.json,
            };
            cloud_command(data_dir, cmd)
        }
    }
}

fn decode_cleanup(raw: &str, label: &str) -> Result<NativeCleanupStatus, String> {
    let v = parse_json_response(raw, label)?
        .get("cleanup")
        .cloned()
        .ok_or("response missing cleanup")?;
    let s: NativeCleanupStatus =
        serde_json::from_value(v).map_err(|e| format!("invalid cleanup response: {e}"))?;
    if s.complete != (s.phase == NativeCleanupPhase::Complete) {
        return Err("cleanup response has inconsistent completion fields".into());
    }
    Ok(s)
}

fn db_cleanup(
    socket: &Path,
    status_only: bool,
    wait: bool,
    timeout: u64,
    json: bool,
) -> Result<(), String> {
    let (raw, label) = if status_only {
        (
            get_path(socket, "/api/db/cleanup", Duration::from_secs(30))?,
            "cleanup status",
        )
    } else {
        (
            post_json_admin(socket, "/api/db/cleanup", &serde_json::json!({}))?,
            "/api/db/cleanup",
        )
    };
    let mut s = decode_cleanup(&raw, label)?;
    let id = s.request_id.clone();
    let (started, limit) = (Instant::now(), Duration::from_secs(timeout));
    while wait && !s.complete {
        if s.phase == NativeCleanupPhase::RecoveryRequired || s.request_id.is_none() {
            print_cleanup_status(&s, json);
            return Err(s
                .last_error
                .unwrap_or_else(|| "no cleanup request is active".into()));
        }
        let Some(left) = limit.checked_sub(started.elapsed()) else {
            print_cleanup_status(&s, json);
            return Err("cleanup remains incomplete; its durable request continues after this client timeout".into());
        };
        std::thread::sleep(Duration::from_millis(250).min(left));
        if let Some(left) = limit.checked_sub(started.elapsed()) {
            s = decode_cleanup(
                &get_path(socket, "/api/db/cleanup", Duration::from_secs(30).min(left))?,
                "cleanup status",
            )?;
            if s.request_id != id {
                return Err("the latest cleanup request changed while this client waited".into());
            }
        }
    }
    print_cleanup_status(&s, json);
    Ok(())
}

fn print_cleanup_status(s: &NativeCleanupStatus, json: bool) {
    if json {
        print_pretty_json(s);
        return;
    }
    println!("Cleanup: {:?}", s.phase);
    if let Some(id) = &s.request_id {
        println!("  request: {id}");
    }
    for (label, m) in [("main", &s.main), ("file blobs", &s.cas)] {
        println!(
            "  {label}: {} tiered, {} merge inputs, {} held bytes",
            m.tiered_runs, m.merging_runs, m.held_retired_bytes
        );
    }
    if let Some(b) = s.completed_through {
        println!(
            "  completed through: main {}, file blobs {}",
            b.main_frontier, b.cas_frontier
        );
    }
    if let Some(err) = &s.last_error {
        println!("  error: {err}");
    }
    println!(
        "  Scope: data already deleted from the active stores. Backup copies remain separate."
    );
}
