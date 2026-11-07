//! Clap definitions for the `lastdb` CLI: the top-level `Cli`, every
//! subcommand enum, and the small value-enum argument types.

use super::*;
#[path = "cli_defs/record_list.rs"]
mod record_list;
pub(crate) use record_list::RecordListArgs;

#[path = "cli_defs/admin.rs"]
mod admin;
#[path = "cli_defs/app.rs"]
mod app;
#[path = "cli_defs/backup.rs"]
mod backup;
#[path = "cli_defs/cloud.rs"]
mod cloud;
#[path = "cli_defs/db.rs"]
mod db;
#[path = "cli_defs/schema.rs"]
mod schema;

pub(super) use admin::*;
pub(super) use app::*;
pub(super) use backup::*;
pub(super) use cloud::*;
pub(super) use db::*;
pub(super) use schema::*;
#[path = "cli_defs/args.rs"]
mod args;
pub(super) use args::*;

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
///
/// These two deadlines have to move together. Reading the const directly here
/// meant the client gave up at the default while the server was still working
/// under a raised `LASTDB_UDS_ADMIN_TIMEOUT_SECS`, so no amount of raising it
/// could let a long scan finish — the client hung up first and the work looked
/// like a timeout even when it went on to commit.
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
    /// A reachable-but-degraded node (QoS, backup) still exits 0.
    ///
    /// Human lines by default. Pass `--json` for a machine-readable summary:
    /// home, socket, `running`, the daemon and CLI builds, and the at-rest
    /// compression counters. Key counts are intentionally omitted — use
    /// `lastdb admin storage inventory` for scanned key totals.
    ///
    /// Live `/api/status` is the **cheap health** path: process vitals,
    /// backup, QoS, and request-ops *scalars* only. The forensic request-ops
    /// ring (256 recent samples + rankings) is opt-in via `lastdb admin ops` or
    /// `GET /api/status?recent=1`.
    Status {
        /// Emit the machine-readable summary (`running`, builds, at-rest compression).
        #[arg(long)]
        json: bool,
        /// Print the additive gauge contract (unit + window + availability for
        /// every typed operator gauge). Does not change default status lines.
        /// Machine JSON: `GET /api/status` → `status.contract`.
        #[arg(long)]
        contract: bool,
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
        /// Read the record with `hydrate: inline` and write its verified
        /// inline file to a new path.
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
