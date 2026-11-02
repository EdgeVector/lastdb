//! Clap definitions for the `lastdb` CLI: the top-level `Cli`, every
//! subcommand enum, and the small value-enum argument types.

use super::*;
#[path = "cli_defs/record_list.rs"]
mod record_list;
pub(crate) use record_list::RecordListArgs;

#[path = "cli_defs/app.rs"]
mod app;
#[path = "cli_defs/cloud.rs"]
mod cloud;
#[path = "cli_defs/db.rs"]
mod db;
#[path = "cli_defs/schema.rs"]
mod schema;

pub(super) use app::*;
pub(super) use cloud::*;
pub(super) use db::*;
pub(super) use schema::*;
#[path = "cli_defs/args.rs"]
mod args;
pub(super) use args::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct MutateRequestOptions {
    pub(super) must_exist: bool,
    pub(super) durable: bool,
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
    /// Field history policy, retained values, and recovery.
    History(super::history_cmds::HistoryArgs),
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
    /// `lastdb db inventory` for scanned key totals.
    ///
    /// Live `/api/status` is the **cheap health** path: process vitals,
    /// backup, QoS, and request-ops *scalars* only. The forensic request-ops
    /// ring (256 recent samples + rankings) is opt-in via `lastdb ops` or
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
    /// Show worst request/mutation offenders (client, schema, latency).
    ///
    /// Clients self-identify with the `X-LastDB-Client` header (e.g. `kanban`).
    /// By default this shows the live in-process forensic ring (full
    /// `request_ops.recent` + rankings via `GET /api/status?recent=1`). Plain
    /// `lastdb status` stays on the cheap health path without that ring.
    /// Pass `--since` to include durable sampler rollups, or `--by-app` for a
    /// compact app/verb view.
    ///
    /// The default tables label process-lifetime and recent-ring populations.
    /// `--by-app` keeps the compact key-value contract for external parsers.
    Ops(OpsArgs),
    /// Run one proactive Mini health alert check.
    ///
    /// Two read-only probes, neither of which reads a DB record. `/health`
    /// drives the down path, its consecutive-failure threshold and its alert
    /// cooldown. `/api/status` — the node's own cheap health path — drives a
    /// DEGRADED verdict from signals the node already computes:
    /// `runtime_degraded`, `footprint_over_limit`, `durability.degraded`
    /// (with the node's own reasons), a slowest request at or above
    /// `--slowest-request-warn-ms`, and a `governor_state` of `purge-failed`
    /// held for at least `GOVERNOR_PURGE_FAILED_WARN_SECS` (not caller
    /// configurable; see `health_alert::probe_degradation`).
    ///
    /// V2 job failures remain explicit. Unmeasured V2 freshness reports unknown.
    ///
    /// A degradation probe that cannot be completed reports
    /// `reachable; degradation unknown — <why>`, never `ok`. Pass
    /// `--no-degradation-probe` to go back to liveness only.
    AlertCheck(AlertCheckArgs),
    /// Show or change the running daemon's tracing filter, without a restart.
    ///
    /// With no argument, prints the directive in force. With a directive, swaps
    /// it in place — the change takes effect on the next log event and is lost
    /// on restart, which is what makes it safe to raise verbosity on a live
    /// node and walk away.
    ///
    /// The directive is `EnvFilter` syntax, so it can target one module:
    ///
    ///   lastdb log-filter 'fold_db::fold_db_core::query::hash_range_query=debug,info'
    ///
    /// Pass `info` to restore the startup default. A malformed directive is
    /// refused and the active filter is left alone.
    LogFilter {
        /// The `EnvFilter` directive to install. Omit to read the current one.
        directive: Option<String>,
        /// Emit the raw `log_filter` object instead of a human line.
        #[arg(long)]
        json: bool,
    },
    /// Persist or inspect the home directory used by brew/launchd service starts.
    ServiceHome {
        #[command(subcommand)]
        action: ServiceHomeCommand,
    },
    /// Isolate a FRESH node home from macOS Time Machine and FSEvents.
    ///
    /// Spotlight exclusion already happens automatically on every boot and
    /// needs no attention here. Time Machine and FSEvents do not have that:
    /// `tmutil addexclusion` requires root, and FSEvents can only be turned
    /// off per APFS *volume*, not per directory — so isolating a home from
    /// FSEvents means giving it its own volume.
    ///
    /// Without `--execute`, prints the plan only; nothing on disk changes.
    /// With `--execute`, needs root (re-run with sudo) and:
    ///   1. excludes the home from Time Machine (`tmutil addexclusion`);
    ///   2. unless `--skip-volume`, creates a new APFS volume in the same
    ///      container as the reference path, mounts it at
    ///      `/Volumes/<volume-name>`, writes `.fseventsd/no_log` at its
    ///      root, and initializes the node home there instead of `--data-dir`.
    ///
    /// Refuses when the target home already holds a store: this is for a
    /// new install, not a migration of a live one.
    IsolateVolume {
        /// Name for the new APFS volume (ignored with --skip-volume).
        #[arg(long, default_value = "LastDB")]
        volume_name: String,
        /// Only apply the Time Machine exclusion; skip APFS volume creation.
        #[arg(long)]
        skip_volume: bool,
        /// Actually create the volume / set the exclusion. Without this,
        /// only the plan is printed and nothing changes.
        #[arg(long)]
        execute: bool,
        /// Emit machine-readable JSON instead of human lines.
        #[arg(long)]
        json: bool,
    },
    /// Explicit V2 backup, restore, and paused credentials.
    Cloud {
        #[command(subcommand)]
        action: CloudCommand,
    },
    /// Hard-erase one keyed record. Delete means gone (no trash, no undo).
    /// Re-delete of a missing key succeeds. Pass `--must-exist` to refuse a miss.
    ///
    /// Only `delete` is implemented. Create/update are refused until a fields
    /// file exists. Hidden `--type purge` is an alias of `--type delete --must-exist`.
    Mutate {
        /// Schema name (`App/Name` or descriptive).
        #[arg(long)]
        schema: String,
        /// Only `delete` is implemented. Hidden `purge` = `delete --must-exist`.
        #[arg(long = "type", default_value = "delete")]
        mutation_type: String,
        /// Hash half of the key.
        #[arg(long)]
        key_hash: Option<String>,
        /// Range half of the key.
        #[arg(long)]
        key_range: Option<String>,
        /// Delete rows whose range starts with this prefix under --key-hash.
        #[arg(long, conflicts_with = "key_range", requires = "key_hash")]
        key_range_prefix: Option<String>,
        /// Refuse if the target is already gone.
        #[arg(long)]
        must_exist: bool,
        /// Wait for the Delete to reach local durable storage before the response.
        #[arg(long)]
        durable: bool,
        /// Emit the raw node JSON response.
        #[arg(long)]
        json: bool,
    },
    /// List live record keys for one schema (hash + range). No field values.
    ///
    /// Walks the schema's records in primary key order and skips deleted
    /// records, so a page can hold fewer keys than the limit. With
    /// `--key-name`, reads the named alternate key entries under `--key-hash`.
    /// Default page is 100 keys; pass `--cursor` from the previous page to
    /// continue. Does not require `X-LastDB-Allow-Full-Scan`. Alias of `get-keys`.
    List(RecordListArgs),
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
    },
    /// Inspect one installed schema's key layout and legal query shapes.
    Schema {
        #[command(subcommand)]
        action: SchemaInspectCommand,
    },
    /// Inspect the counter of a file blob.
    Liveness {
        #[command(subcommand)]
        action: LivenessCommand,
    },
    /// Database admin.
    ///
    /// Most verbs talk to the running daemon over the owner socket.
    Db {
        #[command(subcommand)]
        action: DbCommand,
    },
    /// Read, set, or clear one installed schema's node-local retention policy.
    SchemaRetention {
        #[command(subcommand)]
        action: SchemaRetentionCommand,
    },
    /// Retire or restore one installed schema's claim on its `descriptive_name`.
    ///
    /// A rekey mints a new identity for the same product and leaves the
    /// predecessor Available under the SAME readable name, so that name
    /// resolves to two schemas and every lookup by it is a 409. Retiring the
    /// predecessor's claim drops it from descriptive-name resolution only. Its
    /// identity hash, its state and its data are untouched, so a reader that
    /// pins the predecessor by hash keeps reading it.
    ///
    /// Name the claimant by its canonical name or identity hash — naming it by
    /// the descriptive name it shares is exactly what is ambiguous.
    SchemaNameClaim {
        #[command(subcommand)]
        action: SchemaNameClaimCommand,
    },
    /// Rebuild Search app inbox by paging product records (off hot path).
    ///
    /// Exclusive offline open of `--data-dir` / LASTDB_HOME (stop `lastdbd` first
    /// for the primary home). Emits IndexChangeBatch JSON under
    /// `apps/search/inbox` for LastSeek or the Search app to read.
    /// After restore/cold home: run this, then `search rebuild` / drain.
    SearchRebuild {
        /// Records per page when walking schemas (default 32).
        #[arg(long, default_value_t = 32)]
        page_size: usize,
        /// Emit raw JSON only.
        #[arg(long)]
        json: bool,
    },
    /// App registry: check schema coverage, register novel schemas, and
    /// publish an app into the LastDB registry (design:
    /// brain `design-lastdb-app-registry`).
    App {
        #[command(subcommand)]
        action: AppCommand,
    },
}
